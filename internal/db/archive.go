package db

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

// archiveRange streams every table of a detached range that has no archive record yet to
// dir/<suffix>/<table>.copy.gz (COPY text format, gzip), then records it in history_archives. Postgres only
// serves the COPY; compression and hashing run here, in the indexer.
func (d *DB) archiveRange(ctx context.Context, dir, suffix string) error {
	for _, t := range HistoryTables {
		var done bool
		if err := d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM history_archives WHERE suffix = $1 AND tbl = $2)`, suffix, t).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		start := time.Now()
		path := filepath.Join(dir, suffix, t+".copy.gz")
		rowCount, size, sum, err := d.archiveTable(ctx, t+"_"+suffix, path)
		if err != nil {
			return fmt.Errorf("%s: %w", t, err)
		}
		if _, err := d.pool.Exec(ctx, `
			INSERT INTO history_archives (suffix, tbl, location, row_count, bytes, sha256) VALUES ($1, $2, $3, $4, $5, $6)`,
			suffix, t, path, rowCount, size, sum); err != nil {
			return err
		}
		metrics.ObserveRetentionStep("archive", time.Since(start))
		log.Info("retention: table archived", "table", t, "range", suffix, "rows", rowCount, "bytes", size,
			"elapsed", time.Since(start))
	}
	return nil
}

func (d *DB) archiveTable(ctx context.Context, table, path string) (int64, int64, string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, 0, "", err
	}
	tmp := path + ".partial"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, 0, "", err
	}
	defer os.Remove(tmp)
	defer f.Close()

	hash := sha256.New()
	counted := &countingWriter{w: io.MultiWriter(f, hash)}
	gz := gzip.NewWriter(counted)

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return 0, 0, "", err
	}
	tag, err := conn.Conn().PgConn().CopyTo(ctx, gz, fmt.Sprintf(`COPY %s TO STDOUT`, pgx.Identifier{table}.Sanitize()))
	conn.Release()
	if err != nil {
		return 0, 0, "", err
	}
	if err := gz.Close(); err != nil {
		return 0, 0, "", err
	}
	if err := f.Sync(); err != nil {
		return 0, 0, "", err
	}
	if err := f.Close(); err != nil {
		return 0, 0, "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, 0, "", err
	}
	return tag.RowsAffected(), counted.n, hex.EncodeToString(hash.Sum(nil)), nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
