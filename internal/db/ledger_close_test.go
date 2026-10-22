package db

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedLedgerEdgeCases adds, every third block of [1, 999], the cases the address rows must count once: a self-send;
// a contract creation; a deposit (type 126); a call whose Transfer events involve its sender, its recipient, a
// self-transfer by a third address, that address twice, a mint and a burn; internal calls including a self-call and
// one with no recipient. selfTransfers more self-transfers per such block weigh the case.
func seedLedgerEdgeCases(t *testing.T, d *DB, selfTransfers int) {
	t.Helper()
	ctx := context.Background()
	seedHistory(t, d, 0, 14_000, 1)
	stmts := []string{
		`INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		 SELECT h || b, b, 1700000000 + b, i, f, tt, '0', 0, 0, '0x', 1, ty
		 FROM generate_series(1, 999, 3) b,
		      (VALUES ('s', 1, '0xs' , '0xs' , 2), ('c', 2, '0xd', NULL, 0), ('p', 3, '0xe', '0xf', 126), ('q', 4, '0xa', '0xtok', 2))
		      AS v(h, i, f, tt, ty)`,
		`INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		 SELECT '0xc' || b, '00', '0xd', 'c' || b, b FROM generate_series(1, 999, 3) b`,
		`INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, token_type)
		 SELECT 'q' || b, li, '0xtok', f, tt, 5, b, 1700000000 + b, 'ERC20'
		 FROM generate_series(1, 999, 3) b,
		      (VALUES (10, '0xa', '0xg'), (11, '0xh', '0xtok'), (12, '0xh', '0xh'), (13, '0xh', '0xi'),
		              (14, '0x0000000000000000000000000000000000000000', '0xa'), (15, '0xi', '0x0000000000000000000000000000000000000000'))
		      AS v(li, f, tt)`,
		`INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, token_type)
		 SELECT 'q' || b, 100 + n, '0xtok', '0xself', '0xself', 1, b, 1700000000 + b, 'ERC20'
		 FROM generate_series(1, 999, 3) b, generate_series(1, $1::int) n`,
		`INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
		 SELECT 'q' || b, b, ta, f, tt, '0', 'call'
		 FROM generate_series(1, 999, 3) b, (VALUES ('0', '0xa', '0xa'), ('1', '0xtok', NULL), ('2', '0xj', '0xk')) AS v(ta, f, tt)`,
	}
	for i, q := range stmts {
		var err error
		if i == 3 {
			_, err = d.pool.Exec(ctx, q, selfTransfers)
		} else {
			_, err = d.pool.Exec(ctx, q)
		}
		require.NoError(t, err, q)
	}
}

func ledgerRows(t *testing.T, d *DB) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT 'span ' || lo || ' ' || hi FROM ledger_spans
		UNION ALL SELECT 'chain ' || lo || ' ' || name || ' ' || value FROM ledger_chain
		UNION ALL SELECT 'token ' || lo || ' ' || token || ' ' || transfers || ' ' || supply FROM ledger_token
		UNION ALL SELECT 'address ' || lo || ' ' || address || ' ' || txs || ' ' || sent || ' ' || internal || ' '
		                 || transfers || ' ' || first_seen || ' ' || last_seen FROM ledger_address
		ORDER BY 1`)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

// closedBy019 closes the same data's spans with migration 019's ledger_close_span.
func closedBy019(t *testing.T, selfTransfers int) []string {
	t.Helper()
	d, cleanup := setupTestDB(t)
	defer cleanup()
	m, release := testMigrator(t, d)
	require.NoError(t, m.MigrateTo(context.Background(), 21))
	release()
	seedLedgerEdgeCases(t, d, selfTransfers)
	closeLedger(t, d)
	return ledgerRows(t, d)
}

func TestLedgerCloseRowsEqualMigration019s(t *testing.T) {
	for _, selfTransfers := range []int{1, 40} {
		want := closedBy019(t, selfTransfers)
		d, cleanup := setupTestDB(t)
		seedLedgerEdgeCases(t, d, selfTransfers)
		_, err := d.CloseLedgerSpans(context.Background(), LedgerConfig{SpanBlocks: 1000, WorkMem: "64MB"}, 1_000_000)
		require.NoError(t, err)
		got := ledgerRows(t, d)
		cleanup()
		require.Contains(t, want, "address 0 0xh 333 0 0 999 1 997", "0xh: a participant in each of the 333 calls, three transfers per call (its self-transfer once)")
		require.Equal(t, want, got, "the one-pass close writes 019's rows (%d self-transfers per call)", selfTransfers)
	}
}

func TestLedgerYieldsWhileIngestLags(t *testing.T) {
	d := &DB{}
	lag := uint64(0)
	cfg := LedgerConfig{MaxHeadLag: 5_000, HeadLag: func(context.Context) (uint64, error) { return lag, nil }}
	ctx := context.Background()
	lag = 5_000
	_, yield := d.ingestLagging(ctx, cfg)
	require.False(t, yield)
	lag = 5_001
	_, yield = d.ingestLagging(ctx, cfg)
	require.True(t, yield)
	cfg.HeadLag = func(context.Context) (uint64, error) { return 0, errors.New("node unreachable") }
	_, yield = d.ingestLagging(ctx, cfg)
	require.False(t, yield, "an unknown lag does not stop the closer")
	cfg.MaxHeadLag = 0
	_, yield = d.ingestLagging(ctx, LedgerConfig{HeadLag: func(context.Context) (uint64, error) { return 1 << 40, nil }})
	require.False(t, yield, "off by default")
}
