package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedVariedHistory adds, on top of seedHistory's blocks, a second transaction per block between many addresses,
// with contract creations, mints, self-transfers, self-calls and contracts rows mixed in.
func seedVariedHistory(t *testing.T, d *DB, from, to int64) {
	t.Helper()
	seedHistory(t, d, from, to, 1)
	_, err := d.pool.Exec(context.Background(), `
		WITH n AS (SELECT g AS b FROM generate_series($1::bigint, $2::bigint) g),
		addr AS (SELECT b, '0x' || lpad(to_hex(b % 37), 40, '0') AS f,
		                CASE WHEN b % 17 = 0 THEN NULL ELSE '0x' || lpad(to_hex(100 + b % 53), 40, '0') END AS t FROM n),
		txs AS (
			INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
			SELECT 'vx' || b, b, 1, f, t, '0', 0, 0, '0x', 1, 0 FROM addr RETURNING hash
		),
		tt AS (
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp)
			SELECT 'vx' || b, 1, $3,
			       CASE WHEN b % 33 = 0 THEN '0x0000000000000000000000000000000000000000' ELSE '0x' || lpad(to_hex(200 + b % 11), 40, '0') END,
			       CASE WHEN b % 39 = 0 THEN '0x' || lpad(to_hex(200 + b % 11), 40, '0') ELSE '0x' || lpad(to_hex(300 + b % 13), 40, '0') END,
			       1, b, 1700000000 + b
			FROM n WHERE b % 3 = 0 RETURNING id
		),
		itx AS (
			INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
			SELECT 'vx' || b, b, '0', '0x' || lpad(to_hex(400 + b % 7), 40, '0'),
			       CASE WHEN b % 28 = 0 THEN '0x' || lpad(to_hex(400 + b % 7), 40, '0') ELSE '0x' || lpad(to_hex(500 + b % 19), 40, '0') END,
			       '0', 'call'
			FROM n WHERE b % 4 = 0 RETURNING id
		)
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		SELECT '0x' || lpad(to_hex(600000 + b), 40, '0'), '6080', '0x' || lpad(to_hex(b % 37), 40, '0'), 'vx' || b, b
		FROM n WHERE b % 50 = 0`, from, to, retentionToken)
	require.NoError(t, err)
}

type addressCounts struct {
	txs, internal, transfers int64
	first, last              int64
}

func countsFromRows(t *testing.T, d *DB, lo, hi int64) map[string]addressCounts {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), ledgerAddressRowsSQL, lo, hi)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]addressCounts{}
	for rows.Next() {
		var a string
		var c addressCounts
		require.NoError(t, rows.Scan(&a, &c.txs, &c.internal, &c.transfers, &c.first, &c.last))
		out[a] = c
	}
	return out
}

func addressStatsNow(t *testing.T, d *DB) map[string]addressCounts {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen FROM address_stats`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]addressCounts{}
	for rows.Next() {
		var a string
		var c addressCounts
		require.NoError(t, rows.Scan(&a, &c.txs, &c.internal, &c.transfers, &c.first, &c.last))
		out[a] = c
	}
	return out
}

// addCounts applies counts as ingest's deltas do: added to an existing row, first/last seen widened, a new row
// counted in addresses_total.
func addCounts(t *testing.T, d *DB, counts map[string]addressCounts) {
	t.Helper()
	for a, c := range counts {
		_, err := d.pool.Exec(context.Background(), `
			WITH up AS (
				INSERT INTO address_stats AS s (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (address) DO UPDATE SET tx_count = s.tx_count + $2, internal_tx_count = s.internal_tx_count + $3,
					token_transfer_count = s.token_transfer_count + $4, first_seen = LEAST(s.first_seen, $5), last_seen = GREATEST(s.last_seen, $6)
				RETURNING (xmax = 0) AS created
			)
			UPDATE chain_counters SET count = count + 1 WHERE name = 'addresses_total' AND (SELECT created FROM up)`,
			a, c.txs, c.internal, c.transfers, c.first, c.last)
		require.NoError(t, err)
	}
}

func merged(a, b map[string]addressCounts) map[string]addressCounts {
	out := map[string]addressCounts{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if c, ok := out[k]; ok {
			v = addressCounts{c.txs + v.txs, c.internal + v.internal, c.transfers + v.transfers, min(c.first, v.first), max(c.last, v.last)}
		}
		out[k] = v
	}
	return out
}

// The repair recounts from ledgerAddressRowsSQL, which must count what the closer writes into ledger_address.
func TestLedgerAddressRowsMatchTheCloser(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedVariedHistory(t, d, 0, 40_001)
	closeLedger(t, d)
	var closed int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT max(hi) FROM ledger_spans`).Scan(&closed))
	require.Positive(t, closed)
	rows, err := d.pool.Query(ctx, `
		SELECT address, sum(txs), sum(internal), sum(transfers), min(first_seen), max(last_seen) FROM ledger_address GROUP BY address`)
	require.NoError(t, err)
	defer rows.Close()
	ledger := map[string]addressCounts{}
	for rows.Next() {
		var a string
		var c addressCounts
		require.NoError(t, rows.Scan(&a, &c.txs, &c.internal, &c.transfers, &c.first, &c.last))
		ledger[a] = c
	}
	require.Equal(t, countsFromRows(t, d, 0, closed), ledger)
	require.Greater(t, len(ledger), 100)
}

// PRST-5393, on a production deployment: address_stats undercounted (catch-up skipped it, the rebuild was cancelled) and double counted
// at the tip, with most of the history pruned. The repair restores it from the ledger plus the retained rows, exactly,
// while "ingest" keeps adding blocks; a dry run changes nothing; a stopped repair resumes; a second one finds nothing.
func TestAddressStatsRepairRestoresBothErrorsFromTheLedger(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedVariedHistory(t, d, 0, 40_001)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 100_001, cfg)
	seedVariedHistory(t, d, 40_002, 80_001)

	_, err := d.pool.Exec(ctx, `
		INSERT INTO chain_counters (name, count) VALUES ('addresses_total', 0) ON CONFLICT (name) DO UPDATE SET count = 0`)
	require.NoError(t, err)
	truth := countsFromRows(t, d, 0, 80_002)
	addCounts(t, d, truth)

	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Positive(t, wm, "history below the window is pruned")

	_, err = d.pool.Exec(ctx, `
		WITH ranked AS (SELECT address, row_number() OVER (ORDER BY address) AS r FROM address_stats)
		UPDATE address_stats a SET
			tx_count = CASE WHEN r <= 20 THEN a.tx_count - 1 - r % 3 ELSE a.tx_count + 3 END,
			token_transfer_count = CASE WHEN r <= 20 THEN a.token_transfer_count ELSE a.token_transfer_count + 2 END,
			internal_tx_count = CASE WHEN r <= 20 THEN a.internal_tx_count - 1 ELSE a.internal_tx_count + 1 END
		FROM ranked WHERE ranked.address = a.address AND (r <= 20 OR r BETWEEN 40 AND 49)`)
	require.NoError(t, err)
	var deleted []string
	rows, err := d.pool.Query(ctx, `
		WITH gone AS (DELETE FROM address_stats WHERE address IN (SELECT address FROM address_stats ORDER BY address DESC LIMIT 5) RETURNING address),
		counted AS (UPDATE chain_counters SET count = count - (SELECT count(*) FROM gone) WHERE name = 'addresses_total')
		SELECT address FROM gone`)
	require.NoError(t, err)
	for rows.Next() {
		var a string
		require.NoError(t, rows.Scan(&a))
		deleted = append(deleted, a)
	}
	rows.Close()
	require.Len(t, deleted, 5)
	broken := addressStatsNow(t, d)

	dry, err := d.StageAddressStatsRepair(ctx, true)
	require.NoError(t, err)
	require.EqualValues(t, 35, dry.Changes, "20 undercounted, 10 double counted, 5 missing")
	require.EqualValues(t, 5, dry.Missing)
	require.Zero(t, dry.Untouched)
	require.Len(t, dry.Largest, 10)
	require.Equal(t, broken, addressStatsNow(t, d), "a dry run changes nothing")

	stage, err := d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.EqualValues(t, 35, stage.Changes)
	_, err = d.StageAddressStatsRepair(ctx, false)
	require.ErrorIs(t, err, ErrAddressStatsRepairPending, "a staged repair is never restaged over")

	seedVariedHistory(t, d, 80_002, 80_002)
	ingested := countsFromRows(t, d, 80_002, 80_003)
	addCounts(t, d, ingested)

	for i := 0; i < 2; i++ {
		applied, _, done, err := d.ApplyAddressStatsRepairBatch(ctx, 7)
		require.NoError(t, err)
		require.Equal(t, 7, applied)
		require.False(t, done)
	}
	created := 0
	for {
		_, c, done, err := d.ApplyAddressStatsRepairBatch(ctx, 7)
		require.NoError(t, err)
		created += c
		if done {
			break
		}
	}
	require.Equal(t, merged(truth, ingested), addressStatsNow(t, d), "equal to a full recount, with the block ingested meanwhile")
	recreatedByIngest := 0
	for _, a := range deleted {
		if _, ok := ingested[a]; ok {
			recreatedByIngest++
		}
	}
	require.Equal(t, 5-recreatedByIngest, created, "the deleted rows the block ingested meanwhile did not recreate")
	var total, stored int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats)`).Scan(&total, &stored))
	require.Equal(t, stored, total, "the restored addresses are counted")

	again, err := d.StageAddressStatsRepair(ctx, true)
	require.NoError(t, err)
	require.Zero(t, again.Changes, "a second repair finds nothing")
}
