//go:build faults

package db

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// fk is a database reached through the fault proxy, plus a direct admin connection for checks.
type fk struct {
	t     *testing.T
	url   string
	proxy *faultkit.Proxy
	d     *DB
	admin *pgx.Conn
}

func newFK(t *testing.T) *fk {
	t.Helper()
	url := faultkit.NewDB(t)
	f := &fk{t: t, url: url, proxy: faultkit.NewProxy(t, url)}
	f.open()
	require.NoError(t, f.d.Migrate())
	f.admin = faultkit.Conn(t, url)
	return f
}

func (f *fk) open() {
	pool, err := pgxpool.New(context.Background(), f.proxy.URL())
	require.NoError(f.t, err)
	f.d = &DB{pool: pool}
	f.t.Cleanup(pool.Close)
}

// restart drops the process: a new pool, the startup migrate and reconcile, with the faults disarmed.
func (f *fk) restart() error {
	f.proxy.Disarm()
	f.d.pool.Close()
	f.open()
	return f.d.Migrate()
}

// recoverPasses runs retention passes, faults disarmed, until one succeeds and leaves nothing detaching, at most n.
func (f *fk) recoverPasses(cfg RetentionConfig, n int) []error {
	f.proxy.Disarm()
	var errs []error
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		err := f.d.retentionPass(ctx, cfg)
		cancel()
		if err == nil {
			var pending int
			_ = f.admin.QueryRow(context.Background(), `SELECT count(*) FROM history_partitions WHERE state <> 'attached'`).Scan(&pending)
			if pending == 0 {
				return errs
			}
		}
		errs = append(errs, err)
	}
	return errs
}

func (f *fk) q(sql string, args ...any) []string {
	f.t.Helper()
	rows, err := f.admin.Query(context.Background(), sql, args...)
	require.NoError(f.t, err)
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(f.t, err)
	return out
}

func (f *fk) n(sql string, args ...any) int64 {
	f.t.Helper()
	var v int64
	require.NoError(f.t, f.admin.QueryRow(context.Background(), sql, args...).Scan(&v))
	return v
}

// violations lists every broken retention invariant.
func (f *fk) violations() []string {
	var v []string
	cut := f.n(`SELECT CASE WHEN EXISTS (SELECT 1 FROM history_partitions WHERE suffix = 'legacy' AND hi IS NULL) THEN 0 ELSE 1 END`)
	if cut == 0 {
		for _, c := range f.q(`SELECT conrelid::regclass::text FROM pg_constraint WHERE conname = 'history_cut'`) {
			v = append(v, "uncut, leftover check on "+c)
		}
	}
	for _, s := range f.q(`SELECT p.suffix || '/' || t || ' (' || p.state || ')' FROM history_partitions p, unnest(history_tables()) t
		WHERE to_regclass(t || '_' || p.suffix) IS NULL`) {
		v = append(v, "row without table "+s)
	}
	for _, s := range f.q(`SELECT p.suffix || '/' || t FROM history_partitions p, unnest(history_tables()) t
		WHERE p.state = 'attached' AND NOT EXISTS (SELECT 1 FROM pg_inherits i
		      WHERE i.inhrelid = to_regclass(t || '_' || p.suffix) AND i.inhparent = t::regclass AND NOT i.inhdetachpending)`) {
		v = append(v, "attached row not attached "+s)
	}
	for _, s := range f.q(`SELECT p.suffix || '/' || t FROM history_partitions p, unnest(history_tables()) t
		WHERE p.state = 'detached' AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = to_regclass(t || '_' || p.suffix))`) {
		v = append(v, "detached row still attached "+s)
	}
	for _, s := range f.q(`SELECT c.relname::text FROM pg_class c WHERE c.relkind IN ('r', 'p')
		AND c.relname ~ ('^(' || array_to_string(history_tables(), '|') || ')_(legacy|p[0-9]+)$')
		AND NOT EXISTS (SELECT 1 FROM history_partitions p WHERE c.relname = ANY (SELECT t || '_' || p.suffix FROM unnest(history_tables()) t))`) {
		v = append(v, "orphan table "+s)
	}
	for _, s := range f.q(`SELECT suffix || '/' || tbl || ' ' || location || ' ' || row_count FROM history_archives`) {
		parts := strings.Fields(s)
		file, err := os.Open(parts[1])
		if err != nil {
			v = append(v, "archive record without file "+s)
			continue
		}
		gz, err := gzip.NewReader(file)
		lines := int64(0)
		if err == nil {
			sc := bufio.NewScanner(gz)
			sc.Buffer(make([]byte, 1<<20), 1<<24)
			for sc.Scan() {
				lines++
			}
			if sc.Err() != nil {
				err = sc.Err()
			}
		}
		file.Close()
		if err != nil || fmt.Sprint(lines) != parts[2] {
			v = append(v, fmt.Sprintf("archive %s: %d lines, err %v", s, lines, err))
		}
	}
	return v
}

// headInsertable reports whether ingest can store the block above the stored head.
func (f *fk) headInsertable() error {
	head := f.n(`SELECT COALESCE(max(number), 0) FROM blocks`)
	if _, err := f.d.EnsurePartitionsAhead(context.Background(), uint64(head), 0); err != nil {
		return err
	}
	return insertCutTestBlock(f.d, uint64(head+1))
}

func insertBlockAt(number uint64) *types.Block {
	return &types.Block{Number: number, Hash: fmt.Sprintf("fk%d", number), Timestamp: seedTimestamp(number)}
}
