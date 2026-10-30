package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// addressStatsAgainstTheTruth lists every address whose address_stats row and truth disagree: a row with no truth, a
// truth with no row, or any counter or block that differs, the zero address included. The addresses are read from the
// rows, the closed ledger and every history table, not from the repair's own diff.
func addressStatsAgainstTheTruth(t *testing.T, d *DB) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		WITH c AS (
			SELECT array_agg(DISTINCT address) AS all FROM (
				SELECT address FROM address_stats UNION ALL SELECT address FROM ledger_address
				UNION ALL SELECT from_address FROM transactions UNION ALL SELECT to_address FROM transactions
				UNION ALL SELECT from_address FROM token_transfers UNION ALL SELECT to_address FROM token_transfers
				UNION ALL SELECT from_address FROM internal_transactions UNION ALL SELECT to_address FROM internal_transactions
				UNION ALL SELECT address FROM contracts
			) u WHERE address IS NOT NULL
		), t AS (SELECT x.* FROM c, ledger_address_truth(c.all) x)
		SELECT concat_ws(' ', COALESCE(s.address, t.address),
		                 'row', s.tx_count, s.sent_count, s.internal_tx_count, s.token_transfer_count, s.first_seen, s.last_seen,
		                 'truth', t.txs, t.sent, t.internal, t.transfers, t.first_seen, t.last_seen)
		FROM address_stats s
		FULL JOIN t ON t.address = s.address
		WHERE s.address IS NULL OR t.address IS NULL
		   OR (s.tx_count, s.sent_count, s.internal_tx_count, s.token_transfer_count, s.first_seen, s.last_seen)
		      IS DISTINCT FROM (t.txs, t.sent, t.internal, t.transfers, t.first_seen, t.last_seen)
		ORDER BY 1`)
	require.NoError(t, err)
	differ, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return differ
}

func requireAddressStatsIsExactlyTheTruth(t *testing.T, d *DB) {
	t.Helper()
	require.Empty(t, addressStatsAgainstTheTruth(t, d))
}

// repairFixture is blocks 1..250 with two closed spans below 201, a retained tail above, block 13501 at the head
// (the one a reorg can reach), and five kinds of drift:
// an overcount, an undercount (one of them at the head), sent_count alone, a missing row and a row no block backs.
func repairFixture(t *testing.T) (*DB, context.Context) {
	t.Helper()
	d, cleanup := setupTestDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	for n := 1; n <= 250; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(251, 13500) g`)
	require.NoError(t, err)
	head := rebaseBlock(13501)
	head.Block.ParentHash = "0xempty13500"
	require.NoError(t, d.InsertBlockDataBatch(ctx, head))
	closed, err := d.CloseLedgerSpans(ctx, repairLedger, 2)
	require.NoError(t, err)
	require.Equal(t, 2, closed)
	requireAddressStatsIsExactlyTheTruth(t, d)

	_, err = d.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = '%s';
		UPDATE address_stats SET tx_count = tx_count - 1, last_seen = 1 WHERE address = '%s';
		UPDATE address_stats SET sent_count = sent_count + 3 WHERE address = '%s';
		DELETE FROM address_stats WHERE address = '%s';
		UPDATE address_stats SET tx_count = 0 WHERE address = '%s';
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, first_seen, last_seen, is_contract, updated_at)
		VALUES ('0x00000000000000000000000000000000000dead0', 4, 0, 0, 2, 7, 9, false, NOW())`,
		rebaseHot, rebaseNewAddress(10), rebaseNewAddress(20), rebaseNewAddress(240), rebaseNewAddress(13501)))
	require.NoError(t, err)
	require.Len(t, addressStatsAgainstTheTruth(t, d), 6)
	return d, ctx
}

// replacementHash is the hash of the block reorgTail stores in place of the head.
const replacementHash = "0xrbfork13501"

var repairLedger = LedgerConfig{SpanBlocks: 100, FirstBlock: 1, WorkMem: "64MB"}

// reorgTail reverts the head, block 13501, and stores a replacement whose second transaction comes from another address.
func reorgTail(t *testing.T, d *DB) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, d.DeleteBlock(ctx, 13501))
	replacement := rebaseBlock(13501)
	other := fmt.Sprintf("0x%040x", 0x2000+13501)
	replacement.Block.Hash = replacementHash
	replacement.Transactions[0].Hash, replacement.Transactions[1].Hash = "0xrtfork13501a", "0xrtfork13501b"
	replacement.Transactions[0].To, replacement.Transactions[1].From = &other, other
	delete(replacement.AddressStats, rebaseNewAddress(13501))
	replacement.AddressStats[other] = &AddressStatsDelta{Address: other, TxCountDelta: 2, SentDelta: 1, BlockNumber: 13501}
	require.NoError(t, d.InsertBlockDataBatch(ctx, replacement))
}

// After any order of the repair and a span rebase, with a reorg at any point of the repair, address_stats is the truth
// for every address, and stays it through the next block and the next reorg.
func TestAddressStatsRepairAndRebaseEndAtTheTruth(t *testing.T) {
	repair := func(t *testing.T, d *DB) {
		for bucket := 0; bucket < AddressBuckets; bucket++ {
			_, err := d.RepairAddressBucket(context.Background(), bucket, 2)
			require.NoError(t, err)
		}
	}
	rebase := func(t *testing.T, d *DB) {
		_, err := d.CloseLedgerSpans(context.Background(), repairLedger, 1)
		require.NoError(t, err)
	}
	orders := map[string]func(t *testing.T, d *DB){
		"repair then rebase": func(t *testing.T, d *DB) { repair(t, d); rebase(t, d) },
		"rebase then repair": func(t *testing.T, d *DB) { rebase(t, d); repair(t, d) },
		"together": func(t *testing.T, d *DB) {
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); repair(t, d) }()
			go func() { defer wg.Done(); rebase(t, d) }()
			wg.Wait()
		},
	}
	reorgs := map[string]func(t *testing.T, d *DB){
		"no reorg": func(*testing.T, *DB) {},
		"before":   reorgTail,
		"between batches": func(t *testing.T, d *DB) {
			var mu sync.Mutex
			reads := 0
			rebaseReadHook = func([]string) {
				mu.Lock()
				defer mu.Unlock()
				if reads++; reads == 3 {
					reorgTail(t, d)
				}
			}
		},
		"between a batch's read and its writes": func(t *testing.T, d *DB) {
			onceBetweenReadAndWrite(t, func() { reorgTail(t, d) })
		},
	}
	for order, run := range orders {
		for when, reorg := range reorgs {
			t.Run(order+", reorg "+when, func(t *testing.T) {
				d, ctx := repairFixture(t)
				t.Cleanup(func() { rebaseReadHook = nil })
				reorg(t, d)
				run(t, d)
				rebaseReadHook = nil
				requireAddressStatsIsExactlyTheTruth(t, d)

				next := rebaseBlock(13502)
				next.Block.ParentHash = replacementHash
				require.NoError(t, d.InsertBlockDataBatch(ctx, next))
				requireAddressStatsIsExactlyTheTruth(t, d)
				require.NoError(t, d.DeleteBlock(ctx, 13502))
				requireAddressStatsIsExactlyTheTruth(t, d)
			})
		}
	}
}

// The repair corrects every kind of drift: the row no block backs is removed and the missing row created.
func TestAddressStatsRepairCorrectsEveryKindOfDrift(t *testing.T) {
	d, ctx := repairFixture(t)
	r := repairUntilDone(t, d)
	require.Equal(t, RebaseResult{Changed: 5, Created: 1, Removed: 1}, RebaseResult{Changed: r.Changed, Created: r.Created, Removed: r.Removed},
		"four rows corrected and one created are written; one removed")
	requireAddressStatsIsExactlyTheTruth(t, d)
	var drifted int
	for bucket := 0; bucket < AddressBuckets; bucket++ {
		drift, err := d.AddressRepairDriftIn(ctx, bucket)
		require.NoError(t, err)
		drifted += len(drift.Addresses)
	}
	require.Zero(t, drifted)
}

// tableChecksums is an md5 of every row of every table in the schema, by table.
func tableChecksums(t *testing.T, d *DB) map[string]string {
	t.Helper()
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = current_schema() ORDER BY 1`)
	require.NoError(t, err)
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	sums := map[string]string{}
	for _, table := range tables {
		var sum string
		require.NoError(t, d.pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT COALESCE(md5(string_agg(x::TEXT, '|' ORDER BY x::TEXT)), '') FROM %s x`, pgx.Identifier{table}.Sanitize())).Scan(&sum))
		sums[table] = sum
	}
	return sums
}

// --dry-run reads only: every table is byte-identical after it, a stage an earlier release left included.
func TestAddressStatsRepairDryRunWritesNothing(t *testing.T) {
	d, ctx := repairFixture(t)
	stageAsTheEarlierRelease(t, d)
	require.NoError(t, d.SetAddressRepairNext(ctx, 7))
	before := tableChecksums(t, d)
	var found AddressRepairDrift
	for bucket := 0; bucket < AddressBuckets; bucket++ {
		drift, err := d.AddressRepairDriftIn(ctx, bucket)
		require.NoError(t, err)
		found.Changed, found.Missing, found.NoTruth = found.Changed+drift.Changed, found.Missing+drift.Missing, found.NoTruth+drift.NoTruth
	}
	require.Equal(t, AddressRepairDrift{Changed: 4, Missing: 1, NoTruth: 1}, found)
	require.Equal(t, before, tableChecksums(t, d))
}

// A stage the earlier release left, half applied when its process died, is dropped; the repair then corrects what the
// applied half did along with everything else.
func TestAddressStatsRepairDropsAnEarlierReleasesStage(t *testing.T) {
	d, ctx := repairFixture(t)
	stageAsTheEarlierRelease(t, d)
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count - 4 WHERE address = $1`, rebaseNewAddress(30))
	require.NoError(t, err, "the half the earlier run applied")
	require.True(t, repairStageExists(t, d))

	require.NoError(t, d.DropAddressStatsRepairStage(ctx))
	require.False(t, repairStageExists(t, d))
	repairUntilDone(t, d)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// A repair stopped part way resumes at the bucket after the last one it finished, and its place is forgotten once it
// has walked every bucket.
func TestAddressStatsRepairResumesWhereItStopped(t *testing.T) {
	d, ctx := repairFixture(t)
	next, err := d.AddressRepairNext(ctx)
	require.NoError(t, err)
	require.Zero(t, next)
	for bucket := 0; bucket < 100; bucket++ {
		_, err := d.RepairAddressBucket(ctx, bucket, AddressRepairBatch)
		require.NoError(t, err)
		require.NoError(t, d.SetAddressRepairNext(ctx, bucket+1))
	}
	next, err = d.AddressRepairNext(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, next)
	for bucket := next; bucket < AddressBuckets; bucket++ {
		_, err := d.RepairAddressBucket(ctx, bucket, AddressRepairBatch)
		require.NoError(t, err)
		require.NoError(t, d.SetAddressRepairNext(ctx, bucket+1))
	}
	next, err = d.AddressRepairNext(ctx)
	require.NoError(t, err)
	require.Equal(t, AddressBuckets, next, "every bucket done")
	require.NoError(t, d.FinishAddressRepair(ctx))
	next, err = d.AddressRepairNext(ctx)
	require.NoError(t, err)
	require.Zero(t, next)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// A batch that runs past AddressRepairBatchTimeout is split and run again; it is never left half applied.
func TestAddressStatsRepairSplitsABatchThatRunsOutOfTime(t *testing.T) {
	d, ctx := repairFixture(t)
	repairBatchTimeout = 300 * time.Millisecond
	slow := 0
	rebaseReadHook = func(batch []string) {
		if len(batch) > 1 {
			slow++
			time.Sleep(2 * repairBatchTimeout)
		}
	}
	t.Cleanup(func() { rebaseReadHook, repairBatchTimeout = nil, AddressRepairBatchTimeout })
	for bucket := 0; bucket < AddressBuckets; bucket++ {
		_, err := d.RepairAddressBucket(ctx, bucket, 2)
		require.NoError(t, err)
	}
	require.NotZero(t, slow)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// PRST-5540: a revert takes the revert count, then address_stats rows, then the daily-stats lock (4670); the
// earlier release's final repair batch took the daily-stats lock after the revert count, and the two deadlocked. The
// repair takes neither in that order now. Order 1: a revert holds the count and its rows while a repair of other
// addresses runs to the end, day recount included. Order 2: the repair's day recount holds the daily-stats lock
// while a revert waits for it holding the count; the recount needs nothing the revert holds and commits, and the
// revert then finishes.
func TestARevertAndTheRepairDoNotDeadlockInEitherOrder(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}, {from: "0xd", to: "0xe"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(3, "h3", []revertTx{{from: "0xa", to: "0xf"}})))
	_, err := d.pool.Exec(ctx, `SELECT daily_stats_advance()`)
	require.NoError(t, err)
	drift := func() {
		_, err := d.pool.Exec(ctx, `
			UPDATE address_stats SET tx_count = tx_count + 2 WHERE address = '0xe';
			UPDATE address_stats SET first_seen = 2 WHERE address = '0xd';
			UPDATE ledger_verify_state SET value = value + 1 WHERE name = 'address_days_recount'`)
		require.NoError(t, err)
	}

	t.Run("revert first", func(t *testing.T) {
		drift()
		revert, err := d.pool.Begin(ctx)
		require.NoError(t, err)
		defer revert.Rollback(context.Background())
		_, err = revertAddressStats(ctx, revert, 3)
		require.NoError(t, err, "the revert holds the count and 0xa's and 0xf's rows")
		for bucket := 0; bucket < AddressBuckets; bucket++ {
			drifted, err := d.AddressRepairDriftIn(ctx, bucket)
			require.NoError(t, err)
			if len(drifted.Addresses) == 0 {
				continue
			}
			for _, a := range drifted.Addresses {
				require.Contains(t, []string{"0xd", "0xe"}, a)
			}
			_, err = d.RepairAddressBucket(ctx, bucket, AddressRepairBatch)
			require.NoError(t, err, "bucket %d beside an open revert", bucket)
		}
		_, err = revert.Exec(ctx, `SELECT daily_stats_invalidate(3)`)
		require.NoError(t, err)
		require.NoError(t, revert.Rollback(ctx))
	})

	t.Run("repair's day recount first", func(t *testing.T) {
		recount, err := d.pool.Begin(ctx)
		require.NoError(t, err)
		defer recount.Rollback(context.Background())
		_, err = recount.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`)
		require.NoError(t, err)
		reverted := make(chan error, 1)
		go func() { reverted <- d.DeleteBlock(ctx, 3) }()
		require.Eventually(t, func() bool {
			var waiting int
			_ = d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE wait_event_type = 'Lock' AND wait_event = 'advisory' AND datname = current_database()`).Scan(&waiting)
			return waiting == 1
		}, 10*time.Second, 20*time.Millisecond, "the revert waits for the daily-stats lock")
		require.NoError(t, recountDailyAddressColumns(ctx, recount), "the recount needs nothing the waiting revert holds")
		require.NoError(t, recount.Commit(ctx))
		require.NoError(t, <-reverted)
	})

	drift()
	repairUntilDone(t, d)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// The zero address has a row like any address: ingest and the ledger count the transactions sent to it and the
// internal calls to and from it, and no token transfer for it. Ingest and the span close keep it equal to its truth;
// drift on it is in bucket 0's diff, which the repair corrects. 036 down leaves it out of the diff, up puts it back.
func TestTheZeroAddressIsComparedAndRepairedLikeAnyAddress(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	zero := zeroAddress
	for n := uint64(1); n <= 4; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(n, fmt.Sprintf("hz%d", n), []revertTx{
			{from: "0xa", to: zero, transfers: [][2]string{{zero, "0xa"}, {"0xa", zero}}, internal: [][2]string{{"0xa", zero}}}})))
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, 'hz' || g, 'hz' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(5, 13200) g`)
	require.NoError(t, err)
	_, err = d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 100, FirstBlock: 1, WorkMem: "64MB"}, 1)
	require.NoError(t, err)
	requireAddressStatsIsExactlyTheTruth(t, d)
	var txs int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT tx_count FROM address_stats WHERE address = $1`, zero).Scan(&txs))
	require.EqualValues(t, 4, txs, "four transactions sent to the zero address")

	_, err = d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 9, internal_tx_count = 0 WHERE address = $1`, zero)
	require.NoError(t, err)
	inBucket0 := func() bool {
		drift, err := d.AddressRepairDriftIn(ctx, 0)
		require.NoError(t, err)
		for _, a := range drift.Addresses {
			if a == zero {
				return true
			}
		}
		return false
	}
	require.True(t, inBucket0(), "bucket 0 reports the zero address")

	pool := d.pool
	migrateTo(t, pool, 35)
	require.False(t, inBucket0(), "before 036 the diff left it out")
	migrateTo(t, pool, -1)
	require.True(t, inBucket0())

	r, err := d.RepairAddressBucket(ctx, 0, AddressRepairBatch)
	require.NoError(t, err)
	require.Equal(t, 1, r.Changed)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// The operator checks in docs/LEDGER.md ("Checking address_stats against the ledger by hand"), run as written: they
// find the drift the fixture holds, and nothing once the repair has run.
func TestTheLedgerDocsAddressStatsChecksRunAsWritten(t *testing.T) {
	doc, err := os.ReadFile("../../docs/LEDGER.md")
	require.NoError(t, err)
	section := string(doc)[strings.Index(string(doc), "## Checking address_stats against the ledger by hand"):]
	var queries []string
	for _, part := range strings.Split(section, "```sql\n")[1:] {
		queries = append(queries, part[:strings.Index(part, "```")])
	}
	require.Len(t, queries, 3, "per bucket, every address, the zero address")

	d, ctx := repairFixture(t)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, first_seen, last_seen, is_contract, updated_at)
		VALUES ($1, 3, 0, 0, 0, 5, 5, false, NOW())`, zeroAddress)
	require.NoError(t, err, "a zero-address row no block backs")
	rowsOf := func(q string) int {
		rows, err := d.pool.Query(ctx, q)
		require.NoError(t, err)
		n := 0
		for rows.Next() {
			n++
		}
		require.NoError(t, rows.Err())
		rows.Close()
		return n
	}
	zeroOK := func() bool {
		var ok *bool
		require.NoError(t, d.pool.QueryRow(ctx, queries[2]).Scan(&ok))
		return ok != nil && *ok
	}
	require.NotZero(t, rowsOf(queries[0]))
	require.Equal(t, 7, rowsOf(queries[1]), "the fixture's six drifts and the zero address")
	require.False(t, zeroOK())

	repairUntilDone(t, d)
	require.Zero(t, rowsOf(queries[0]))
	require.Zero(t, rowsOf(queries[1]))
	require.True(t, zeroOK())
}
