//go:build faults

package indexer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

type fkRig struct {
	t      *testing.T
	url    string
	proxy  *faultkit.Proxy
	node   *faultkit.Node
	client *rpc.Client
	admin  *pgx.Conn
	d      *db.DB
	cancel context.CancelFunc
	done   chan error
	idx    *Indexer
}

func newFKRig(t *testing.T, head uint64) *fkRig {
	t.Helper()
	r := &fkRig{t: t, url: faultkit.NewDB(t)}
	r.proxy = faultkit.NewProxy(t, r.url)
	r.node = faultkit.NewNode(t, head)
	c, err := rpc.New(r.node.URL())
	if err != nil {
		t.Fatal(err)
	}
	r.client = c
	r.admin = faultkit.Conn(t, r.url)
	d, err := db.New(r.url)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	d.Close()
	t.Cleanup(r.stop)
	return r
}

func fkConfig() *Config {
	return &Config{RPCWorkers: 4, RPCRateLimit: 1000, DBBatchSize: 50, CatchupEnabled: true, CatchupWorkers: 4,
		CatchupBatchSize: 20, CatchupQueueSize: 100, TokenMetadataWorkers: 2, BalanceWorkers: 2}
}

// start runs a new process's indexer (a new db.DB through the proxy, a new Indexer) in the background.
func (r *fkRig) start(cfg *Config) {
	r.t.Helper()
	d, err := db.New(r.proxy.URL())
	if err != nil {
		r.t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		r.t.Fatalf("restart: migrate/reconcile: %v", err)
	}
	r.d = d
	r.idx = NewWithConfig(d, r.client, 20*time.Millisecond, 0, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan error, 1)
	go func() { r.done <- r.idx.Start(ctx) }()
}

// stop ends the running process, bounded.
func (r *fkRig) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(20 * time.Second):
		r.t.Errorf("the indexer did not stop within 20 s")
	}
	r.cancel = nil
	if r.d != nil {
		r.d.Close()
		r.d = nil
	}
}

// exited reports the process's return, if it returned.
func (r *fkRig) exited() (error, bool) {
	select {
	case err := <-r.done:
		r.done <- err
		return err, true
	default:
		return nil, false
	}
}

func forkOfHash(h string) uint64 {
	v, _ := strconv.ParseUint(strings.TrimPrefix(h, "0x")[:16], 16, 64)
	return v - 1
}

// violations lists every way the stored data differs from the node's chain 0..target.
func (r *fkRig) violations(target uint64) []string {
	ctx := context.Background()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	rows, err := r.admin.Query(ctx, `SELECT number, hash FROM blocks ORDER BY number`)
	if err != nil {
		return []string{err.Error()}
	}
	seen := map[uint64]int{}
	var stale []uint64
	for rows.Next() {
		var n int64
		var h string
		_ = rows.Scan(&n, &h)
		seen[uint64(n)]++
		if uint64(n) <= target && h != r.node.BlockHash(uint64(n)) {
			stale = append(stale, uint64(n))
		}
	}
	rows.Close()
	var missing []uint64
	for n := uint64(1); n <= target; n++ {
		if seen[n] == 0 {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		add("blocks missing: %v", abbrev(missing))
	}
	if len(stale) > 0 {
		add("blocks on a stale fork: %v", abbrev(stale))
	}
	for _, q := range []struct{ name, sql string }{
		{"transactions", `SELECT block_number, hash FROM transactions`},
		{"logs", `SELECT block_number, tx_hash FROM logs`},
		{"token_transfers", `SELECT block_number, tx_hash FROM token_transfers`},
	} {
		rows, err := r.admin.Query(ctx, q.sql)
		if err != nil {
			add("%s: %v", q.name, err)
			continue
		}
		per := map[uint64]int{}
		var bad []uint64
		for rows.Next() {
			var n int64
			var h string
			_ = rows.Scan(&n, &h)
			per[uint64(n)]++
			if uint64(n) <= target && h != faultkit.TxHash(forkOfHash(r.node.BlockHash(uint64(n))), uint64(n)) {
				bad = append(bad, uint64(n))
			}
		}
		rows.Close()
		var dup, miss []uint64
		for n := uint64(0); n <= target; n++ {
			if n == 0 && seen[0] == 0 {
				continue
			}
			if per[n] > 1 {
				dup = append(dup, n)
			} else if per[n] == 0 {
				miss = append(miss, n)
			}
		}
		if len(bad) > 0 {
			add("%s on a stale fork: %v", q.name, abbrev(bad))
		}
		if len(dup) > 0 {
			add("%s duplicated: %v", q.name, abbrev(dup))
		}
		if len(miss) > 0 {
			add("%s missing: %v", q.name, abbrev(miss))
		}
	}
	var blocks, txs, transfers, cb, ct, ctr, tc int64
	_ = r.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM blocks), (SELECT count(*) FROM transactions), (SELECT count(*) FROM token_transfers),
		COALESCE((SELECT count FROM chain_counters WHERE name='blocks_total'),-1), COALESCE((SELECT count FROM chain_counters WHERE name='transactions_total'),-1),
		COALESCE((SELECT count FROM chain_counters WHERE name='transfers_total'),-1),
		COALESCE((SELECT transfer_count FROM tokens WHERE address=$1),-1)`, faultkit.TokenAddr).Scan(&blocks, &txs, &transfers, &cb, &ct, &ctr, &tc)
	if cb != blocks {
		add("chain_counters blocks_total %d, blocks %d", cb, blocks)
	}
	if ct != txs {
		add("chain_counters transactions_total %d, transactions %d", ct, txs)
	}
	if ctr != -1 && ctr != transfers {
		add("chain_counters transfers_total %d, token_transfers %d", ctr, transfers)
	}
	if tc != -1 && tc != transfers {
		add("tokens.transfer_count %d, token_transfers %d", tc, transfers)
	}
	var ranges int64
	_ = r.admin.QueryRow(ctx, `SELECT count(*) FROM missing_block_ranges`).Scan(&ranges)
	if ranges > 0 {
		add("missing_block_ranges rows: %d", ranges)
	}
	// address_stats, recounted from the stored rows the writer counts from.
	rows, err = r.admin.Query(ctx, `
		WITH s AS (SELECT a.address, a.tx_count, a.token_transfer_count FROM address_stats a),
		     txc AS (SELECT addr, count(DISTINCT h) n FROM (
		              SELECT from_address addr, hash h FROM transactions
		              UNION ALL SELECT to_address, hash FROM transactions WHERE to_address IS NOT NULL
		              UNION ALL SELECT from_address, tx_hash FROM token_transfers
		              UNION ALL SELECT to_address, tx_hash FROM token_transfers) x GROUP BY addr),
		     ttc AS (SELECT addr, count(*) n FROM (
		              SELECT from_address addr, tx_hash, log_index FROM token_transfers
		              UNION SELECT to_address, tx_hash, log_index FROM token_transfers) y GROUP BY addr)
		SELECT COALESCE(s.address, txc.addr), COALESCE(s.tx_count, 0), COALESCE(txc.n, 0), COALESCE(s.token_transfer_count, 0), COALESCE(ttc.n, 0)
		FROM s FULL JOIN txc ON txc.addr = s.address LEFT JOIN ttc ON ttc.addr = COALESCE(s.address, txc.addr)
		WHERE COALESCE(s.tx_count, 0) <> COALESCE(txc.n, 0) OR COALESCE(s.token_transfer_count, 0) <> COALESCE(ttc.n, 0)
		ORDER BY 1 LIMIT 5`)
	if err == nil {
		for rows.Next() {
			var a string
			var st, rt, sx, rx int64
			_ = rows.Scan(&a, &st, &rt, &sx, &rx)
			add("address_stats %s: tx_count %d (recount %d), token_transfer_count %d (recount %d)", a, st, rt, sx, rx)
		}
		rows.Close()
	} else {
		add("address_stats recount: %v", err)
	}
	return out
}

func abbrev(v []uint64) string {
	if len(v) > 12 {
		return fmt.Sprintf("%v ... (%d)", v[:12], len(v))
	}
	return fmt.Sprint(v)
}

// waitConverged waits until the stored data matches the node's chain up to target, failing loudly with what still
// differs after timeout, or when the process exits.
func (r *fkRig) waitConverged(target uint64, timeout time.Duration) {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	for time.Now().Before(deadline) {
		if err, ok := r.exited(); ok {
			r.t.Fatalf("the indexer exited: %v; differences: %v", err, r.violations(target))
		}
		last = r.violations(target)
		if len(last) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	r.t.Fatalf("not converged to %d after %s: %s", target, timeout, strings.Join(last, "; "))
}

// waitHead waits until block n is stored (any fork), bounded.
func (r *fkRig) waitStored(n uint64, timeout time.Duration) {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var ok bool
		_ = r.admin.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM blocks WHERE number = $1)`, n).Scan(&ok)
		if ok {
			return
		}
		if err, done := r.exited(); done {
			r.t.Fatalf("the indexer exited: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.t.Fatalf("block %d not stored after %s", n, timeout)
}

// baseline: a clean run converges and every invariant holds (calibrates the checks).
func TestFaultBaseline(t *testing.T) {
	for _, tc := range []struct {
		name string
		head uint64
		cfg  func(*Config)
	}{
		{"standard", 80, nil},
		{"standard-pipelined", 80, func(c *Config) { c.CommitPipelineDepth = 3 }},
		{"collector", 300, nil},
		{"collector-pipelined", 300, func(c *Config) { c.CommitPipelineDepth = 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newFKRig(t, tc.head)
			cfg := fkConfig()
			if tc.cfg != nil {
				tc.cfg(cfg)
			}
			r.start(cfg)
			r.waitConverged(tc.head-1, 60*time.Second)
			r.node.SetHead(tc.head + 20)
			r.waitConverged(tc.head+19, 60*time.Second)
		})
	}
}
