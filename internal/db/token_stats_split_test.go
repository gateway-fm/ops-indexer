package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// RefreshTokenStats used to recompute transfer_count, total_supply AND
// holder_count together, on both the per-block ingest path and the balance-flush
// path. It is now split in two, because only one of those paths can change
// holder_count: balances are fetched asynchronously and queued after the block
// commits, so the ingest path was recomputing a value the block had not touched.
//
// These tests pin the two halves of that contract:
//
//   - the transfer-side refresh is still exact on the next read, which is what
//     ruled out simply deferring the whole thing to a background worker;
//   - it does not touch holder_count, so re-merging the two by accident fails
//     here rather than in production;
//   - the holder-side refresh counts an address by its LATEST balance, so an
//     address that has gone to zero stops being a holder. That is the semantic a
//     current-balances table has to preserve, and the one the read path's
//     GetTokenHolders currently gets wrong.
//
// See PRST-4493.

const splitBurnAddr = "0x0000000000000000000000000000000000000000"

// splitFixture is one test's private set of rows. Every identity is derived from
// a per-test id so the tests can share a database -- they run against
// setupBenchDB, which honours BENCH_DATABASE_URL and then creates and drops
// nothing. Hardcoding block 1 and '0xtx1' would make the second test to run fail
// on a primary key rather than on its assertion.
type splitFixture struct {
	token  string
	block  int64
	txHash string
}

// newSplitFixture returns the fixture and a function that removes its rows.
//
// The caller must register that with `defer`, NOT t.Cleanup: t.Cleanup runs
// after the enclosing function's defers, by which point `defer cleanup()` from
// setupBenchDB has already closed the pool and every delete fails with "closed
// pool". Registering it as a defer AFTER setupBenchDB's puts it ahead in LIFO
// order, so the rows go before the pool does.
func newSplitFixture(t *testing.T, d *DB, id int64) (splitFixture, func()) {
	t.Helper()
	ctx := context.Background()

	// Address-shaped: 42 characters, as production. Not valid hex, matching the
	// existing '0xholder%035d' convention in the bench seeder.
	f := splitFixture{
		token:  fmt.Sprintf("0xsplit%035d", id),
		block:  4_493_000_000 + id,
		txHash: fmt.Sprintf("0xsplittx%057d", id),
	}

	// Drop first. A run that fails before its defer -- or is killed -- leaves
	// these rows behind, and the next run then fails on blocks_pkey instead of
	// on its own assertion. Safe to do unconditionally: every key here is
	// private to this fixture id, so this cannot delete anything else's rows.
	drop := func(when string) {
		for _, q := range []struct{ sql, what string }{
			{`DELETE FROM balances WHERE token_address = $1`, "balances"},
			{`DELETE FROM tokens WHERE address = $1`, "tokens"},
		} {
			if _, err := d.pool.Exec(ctx, q.sql, f.token); err != nil {
				t.Errorf("%s %s: %v", when, q.what, err)
			}
		}
		// token_transfers cascades from transactions, which cascades from blocks.
		if _, err := d.pool.Exec(ctx, `DELETE FROM blocks WHERE number = $1`, f.block); err != nil {
			t.Errorf("%s blocks: %v", when, err)
		}
	}
	drop("pre-clean")

	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES ($1, $2, $3, 1700000000, 21000, 30000000, 1)`,
		f.block, fmt.Sprintf("0xsplitblk%d", id), fmt.Sprintf("0xsplitblk%d", id-1))
	require.NoError(t, err)

	// token_transfers.tx_hash is a foreign key into transactions(hash), so the
	// parent transaction has to exist before any transfer can hang off it.
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, status)
		VALUES ($1, $2, 0, '0xsplitsender', '0xsplitrecipient', 0, 21000, 1, 1)`,
		f.txHash, f.block)
	require.NoError(t, err)

	_, err = d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
		VALUES ($1, 'SPLIT', 'Split Token', 18, 'ERC20', $2)`, f.token, f.block)
	require.NoError(t, err)

	// Errorf, not Fatalf: this runs while the test is unwinding, and a Fatalf
	// here would mask whatever the test actually found.
	return f, func() { drop("cleanup") }
}

// addTransfer appends one ERC20 transfer to the fixture's transaction. log_index
// is unique per row because token_transfers has UNIQUE(tx_hash, log_index).
func (f splitFixture) addTransfer(t *testing.T, d *DB, logIndex int, from, to, value string) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, transfer_type, token_type)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'transfer', 'ERC20')`,
		f.txHash, logIndex, f.token, from, to, value, f.block)
	require.NoError(t, err)
}

func (f splitFixture) addBalance(t *testing.T, d *DB, addr string, block int64, balance string) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		INSERT INTO balances (address, token_address, block_number, balance)
		VALUES ($1, $2, $3, $4)`, addr, f.token, block, balance)
	require.NoError(t, err)
}

func (f splitFixture) counters(t *testing.T, d *DB) (transferCount, holderCount int64, totalSupply string) {
	t.Helper()
	err := d.pool.QueryRow(context.Background(), `
		SELECT COALESCE(transfer_count, 0), COALESCE(holder_count, 0), COALESCE(total_supply, 0)::text
		FROM tokens WHERE address = $1`, f.token).Scan(&transferCount, &holderCount, &totalSupply)
	require.NoError(t, err)
	return
}

// TestRefreshTokenTransferStats_ExactOnNextRead is the read-after-write
// assertion. transfer_count and total_supply must be correct immediately after
// the transfers land, with no background worker in between.
func TestRefreshTokenTransferStats_ExactOnNextRead(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, dropFixture := newSplitFixture(t, d, 1)
	defer dropFixture()

	// Two mints of 1000 each, one ordinary transfer, one burn of 250.
	// total_supply is mints - burns = 2000 - 250 = 1750, over 4 transfers.
	f.addTransfer(t, d, 0, splitBurnAddr, "0xsplitholder1", "1000")
	f.addTransfer(t, d, 1, splitBurnAddr, "0xsplitholder2", "1000")
	f.addTransfer(t, d, 2, "0xsplitholder1", "0xsplitholder2", "500")
	f.addTransfer(t, d, 3, "0xsplitholder2", splitBurnAddr, "250")

	require.NoError(t, d.RefreshTokenTransferStats(ctx, f.token))

	transferCount, _, totalSupply := f.counters(t, d)
	require.Equal(t, int64(4), transferCount, "transfer_count must be exact on the next read")
	require.Equal(t, "1750", totalSupply, "total_supply must be mints minus burns, exact on the next read")

	// And it stays exact as more transfers arrive.
	f.addTransfer(t, d, 4, splitBurnAddr, "0xsplitholder3", "300")
	require.NoError(t, d.RefreshTokenTransferStats(ctx, f.token))

	transferCount, _, totalSupply = f.counters(t, d)
	require.Equal(t, int64(5), transferCount)
	require.Equal(t, "2050", totalSupply)
}

// TestRefreshTokenTransferStats_LeavesHolderCountAlone pins the split itself.
// holder_count belongs to the balance-write path; the transfer-side refresh must
// neither recompute it nor reset it. Without this, folding the two UPDATEs back
// together would go unnoticed -- the transfer-side test above would still pass.
func TestRefreshTokenTransferStats_LeavesHolderCountAlone(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, dropFixture := newSplitFixture(t, d, 2)
	defer dropFixture()
	f.addTransfer(t, d, 0, splitBurnAddr, "0xsplitholder1", "1000")

	// A sentinel no balance row could justify: this token has no balances at
	// all, so a refresh that touches holder_count computes 0 and this fails.
	_, err := d.pool.Exec(ctx, `UPDATE tokens SET holder_count = 4242 WHERE address = $1`, f.token)
	require.NoError(t, err)

	require.NoError(t, d.RefreshTokenTransferStats(ctx, f.token))

	_, holderCount, _ := f.counters(t, d)
	require.Equal(t, int64(4242), holderCount,
		"the transfer-side refresh must not write holder_count; balances are the only input to it")
}

// TestRefreshTokenHolderCount_CountsLatestBalanceOnly pins the holder semantic:
// membership follows the latest balance per address, so crossing to zero removes
// a holder and crossing back adds one. This is the property that makes
// holder_count hard to maintain as a delta, and it is exactly what the read
// path's GetTokenHolders gets wrong today -- it filters balance > 0 before
// picking the latest row, so an address that sold out still lists as a holder.
func TestRefreshTokenHolderCount_CountsLatestBalanceOnly(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, dropFixture := newSplitFixture(t, d, 3)
	defer dropFixture()

	// holder1 holds. holder2 held and went to zero. holder3 went to zero and
	// back. holder4 has only ever been zero. Expected holders: holder1, holder3.
	f.addBalance(t, d, "0xsplitholder1", f.block, "1000")
	f.addBalance(t, d, "0xsplitholder2", f.block, "1000")
	f.addBalance(t, d, "0xsplitholder2", f.block+1, "0")
	f.addBalance(t, d, "0xsplitholder3", f.block, "1000")
	f.addBalance(t, d, "0xsplitholder3", f.block+1, "0")
	f.addBalance(t, d, "0xsplitholder3", f.block+2, "500")
	f.addBalance(t, d, "0xsplitholder4", f.block, "0")

	require.NoError(t, d.RefreshTokenHolderCount(ctx, f.token))

	_, holderCount, _ := f.counters(t, d)
	require.Equal(t, int64(2), holderCount,
		"holder_count must follow each address's latest balance, not any balance it ever had")

	// The historical rows stay -- holder_count is derived from the latest of
	// them, so appending a newer zero for holder1 drops it again.
	f.addBalance(t, d, "0xsplitholder1", f.block+3, "0")
	require.NoError(t, d.RefreshTokenHolderCount(ctx, f.token))

	_, holderCount, _ = f.counters(t, d)
	require.Equal(t, int64(1), holderCount)
}

// TestRefreshSplit_UnknownTokenIsNoOp keeps the early return documented. Both
// refreshes are called with addresses discovered from a Transfer event, which
// can arrive before the token row is written, so neither may error there.
func TestRefreshSplit_UnknownTokenIsNoOp(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	unknown := fmt.Sprintf("0xsplit%035d", 999)
	require.NoError(t, d.RefreshTokenTransferStats(ctx, unknown),
		"a refresh for an unknown token must be a no-op, not an error")
	require.NoError(t, d.RefreshTokenHolderCount(ctx, unknown),
		"a refresh for an unknown token must be a no-op, not an error")
}
