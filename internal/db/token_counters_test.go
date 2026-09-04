package db

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

// tokens.transfer_count and tokens.total_supply used to be recomputed from the
// token's whole stored history on every block that touched it. They are now
// maintained by deltas inside InsertBlockDataBatch, in the same transaction as
// the transfer rows. That swap is only sound while the deltas and the
// recomputation they replaced agree, so the tests here pin exactly that:
//
//   - the maintained counters equal a from-scratch recompute over the stored
//     transfers, which is the definition they inherited;
//   - they are visible to a reader the instant InsertBlockDataBatch returns,
//     because the deltas commit with the rows (unlike holder_count, which
//     nothing on the per-block path can derive);
//   - a re-delivered block does not double-count, since the insert is
//     ON CONFLICT DO NOTHING and only rows that genuinely landed may count;
//   - mints add and burns subtract, and a 0x0 -> 0x0 row nets to zero rather
//     than being read as one or the other;
//   - total_supply moves for ERC20 only, while transfer_count moves for every
//     token type;
//   - a tokens row created after its transfers were already stored is seeded
//     from that history, which is the case absolute recompute used to cover for
//     free and the one a delta path can silently lose.
//
// Writes go through InsertBlockDataBatch rather than hand-written SQL, because
// the delta statements being tested live there.
//
// See PRST-4493.

const zeroAddr = "0x0000000000000000000000000000000000000000"

// ctrFixture keys every row off a per-test id so these can share one database
// with each other and with the other suites.
type ctrFixture struct {
	id    int64
	token string
	base  int64
	seq   int
}

func newCtrFixture(t *testing.T, d *DB, id int64, baseBlock int64) (*ctrFixture, func()) {
	t.Helper()
	ctx := context.Background()
	f := &ctrFixture{id: id, token: fmt.Sprintf("0xctr%038d", id), base: baseBlock}

	drop := func(when string) {
		for _, q := range []string{
			`DELETE FROM token_transfers WHERE token_address = $1`,
			`DELETE FROM tokens WHERE address = $1`,
		} {
			if _, err := d.pool.Exec(ctx, q, f.token); err != nil {
				t.Errorf("%s: %v", when, err)
			}
		}
		// transactions is the FK parent of token_transfers; its rows are keyed
		// off the same id so they can go too.
		if _, err := d.pool.Exec(ctx,
			`DELETE FROM transactions WHERE hash LIKE $1`, fmt.Sprintf("0xctr%d-%%", id)); err != nil {
			t.Errorf("%s: %v", when, err)
		}
		if _, err := d.pool.Exec(ctx,
			`DELETE FROM blocks WHERE number BETWEEN $1 AND $2`, baseBlock, baseBlock+999); err != nil {
			t.Errorf("%s: %v", when, err)
		}
	}
	// Pre-clean: a run killed before its defer leaves rows behind, and the next
	// run would then measure a fixture it did not build.
	drop("pre-clean")
	return f, func() { drop("cleanup") }
}

// transfer builds one transfer on the fixture's token, with a fresh tx hash and
// a parent transactions/blocks row so the foreign keys hold.
func (f *ctrFixture) transfer(t *testing.T, d *DB, from, to, value, tokenType string) *types.TokenTransfer {
	t.Helper()
	ctx := context.Background()
	f.seq++
	block := f.base + int64(f.seq)
	hash := fmt.Sprintf("0xctr%d-%d", f.id, f.seq)
	ts := uint64(time.Now().Unix())

	_, err := d.pool.Exec(ctx,
		`INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		 VALUES ($1, $2, '0xp', $3, 1, 1, 1) ON CONFLICT (number) DO NOTHING`,
		block, fmt.Sprintf("0xblk%d-%d", f.id, f.seq), ts)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx,
		`INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value,
			gas_used, gas_price, input_data, status, tx_type)
		 VALUES ($1, $2, 0, $3, $4, '0', 0, 0, '0x', 1, 0) ON CONFLICT (hash) DO NOTHING`,
		hash, block, zeroAddr, zeroAddr)
	require.NoError(t, err)

	return &types.TokenTransfer{
		TxHash:       hash,
		LogIndex:     0,
		TokenAddress: f.token,
		From:         from,
		To:           to,
		Value:        types.JSONString(value),
		BlockNumber:  uint64(block),
		Timestamp:    &ts,
		TransferType: "transfer",
		TokenType:    tokenType,
	}
}

// createToken writes the tokens row the counters hang off.
func (f *ctrFixture) createToken(t *testing.T, d *DB, tokenType string) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(),
		`INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
		 VALUES ($1, 'CTR', 'Counter Token', 18, $2, $3) ON CONFLICT (address) DO NOTHING`,
		f.token, tokenType, f.base)
	require.NoError(t, err)
}

// stored reads the maintained counters off the tokens row.
func (f *ctrFixture) stored(t *testing.T, d *DB) (int64, *big.Int) {
	t.Helper()
	var count int64
	var supply *string
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT COALESCE(transfer_count, 0), total_supply::TEXT FROM tokens WHERE address = $1`,
		f.token).Scan(&count, &supply))
	if supply == nil {
		return count, nil
	}
	n, ok := new(big.Int).SetString(*supply, 10)
	require.True(t, ok, "total_supply %q is not an integer", *supply)
	return count, n
}

// recomputed runs the from-scratch aggregate the deltas replaced. This is the
// oracle: it is the definition transfer_count and total_supply had before, so
// any disagreement is the delta path being wrong.
func (f *ctrFixture) recomputed(t *testing.T, d *DB) (int64, *big.Int) {
	t.Helper()
	var count int64
	var supply string
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT COUNT(*),
		        COALESCE(
		            SUM(CASE WHEN token_type = 'ERC20' AND from_address = '`+zeroAddr+`' THEN value ELSE 0 END)
		          - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '`+zeroAddr+`' THEN value ELSE 0 END),
		            0)::TEXT
		 FROM token_transfers WHERE token_address = $1`, f.token).Scan(&count, &supply))
	n, ok := new(big.Int).SetString(supply, 10)
	require.True(t, ok, "recomputed supply %q is not an integer", supply)
	return count, n
}

// requireAgrees asserts the maintained counters equal the recomputed truth.
func (f *ctrFixture) requireAgrees(t *testing.T, d *DB, wantCount int64, wantSupply string) {
	t.Helper()
	gotCount, gotSupply := f.stored(t, d)
	trueCount, trueSupply := f.recomputed(t, d)

	require.Equal(t, trueCount, gotCount,
		"transfer_count drifted from a recompute over the stored transfers")
	require.Equal(t, wantCount, gotCount, "transfer_count")

	if gotSupply == nil {
		gotSupply = new(big.Int)
	}
	require.Equal(t, trueSupply.String(), gotSupply.String(),
		"total_supply drifted from a recompute over the stored transfers")
	require.Equal(t, wantSupply, gotSupply.String(), "total_supply")
}

// TestTokenCounters_DeltasMatchRecompute walks a token through mints, burns,
// plain transfers and a re-delivered block, checking after every batch that the
// maintained counters still equal the recomputation they replaced.
func TestTokenCounters_DeltasMatchRecompute(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 1, 4_100_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)
	f.requireAgrees(t, d, 0, "0")

	// A mint adds to supply.
	mint := f.transfer(t, d, zeroAddr, "0xholder1", "1000", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{mint}}))
	f.requireAgrees(t, d, 1, "1000")

	// A plain transfer moves neither end of supply.
	plain := f.transfer(t, d, "0xholder1", "0xholder2", "400", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{plain}}))
	f.requireAgrees(t, d, 2, "1000")

	// A burn subtracts.
	burn := f.transfer(t, d, "0xholder2", zeroAddr, "250", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{burn}}))
	f.requireAgrees(t, d, 3, "750")

	// Several rows in one batch, including a second mint, must aggregate.
	multi := []*types.TokenTransfer{
		f.transfer(t, d, zeroAddr, "0xholder3", "5000", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder3", "0xholder1", "10", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder1", zeroAddr, "1000", types.TokenTypeERC20),
	}
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: multi}))
	f.requireAgrees(t, d, 6, "4750")

	// A 0x0 -> 0x0 row is a mint and a burn at once, so it must net to zero
	// rather than being counted as either alone.
	both := f.transfer(t, d, zeroAddr, zeroAddr, "999", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{both}}))
	f.requireAgrees(t, d, 7, "4750")

	// Re-delivering a block the indexer already stored is normal (a restart
	// replays from the last committed height). ON CONFLICT DO NOTHING drops the
	// rows, so the counters must not move either.
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: multi}))
	f.requireAgrees(t, d, 6+1, "4750")

	// A value wide enough to overflow int64, since the column is NUMERIC(78,0)
	// and the accumulator is a big.Int.
	huge := "123456789012345678901234567890"
	bigMint := f.transfer(t, d, zeroAddr, "0xholder4", huge, types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{bigMint}}))
	want := new(big.Int)
	want.SetString(huge, 10)
	want.Add(want, big.NewInt(4750))
	f.requireAgrees(t, d, 8, want.String())
}

// TestTokenCounters_ReadAfterWrite pins the property that separates these two
// counters from holder_count: they commit with the transfers, so the value a
// reader sees immediately after InsertBlockDataBatch returns is already
// correct. Nothing sits between the write and the read here on purpose.
func TestTokenCounters_ReadAfterWrite(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 2, 4_200_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)

	for i := 0; i < 5; i++ {
		tr := f.transfer(t, d, zeroAddr, "0xholder1", "100", types.TokenTypeERC20)
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}))

		// No refresh call, no settle, no retry: the very next read must already
		// carry this batch.
		count, supply := f.stored(t, d)
		require.Equal(t, int64(i+1), count,
			"transfer_count not visible immediately after the batch committed")
		require.Equal(t, fmt.Sprintf("%d", (i+1)*100), supply.String(),
			"total_supply not visible immediately after the batch committed")
	}
}

// TestTokenCounters_SeedsTokenRowCreatedAfterItsTransfers covers the hole a
// delta path opens and absolute recompute used to cover for free.
//
// Token discovery needs an RPC metadata fetch. When that fetch fails the tokens
// row is skipped while the block's transfers are still stored, and the delta
// aimed at that row updates nothing. Discovery retries on a later block, so the
// row does eventually appear -- and it has to appear carrying the transfers that
// landed while it did not exist, or the counter is permanently short.
func TestTokenCounters_SeedsTokenRowCreatedAfterItsTransfers(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 3, 4_300_000)
	defer drop()
	ctx := context.Background()

	// Transfers land with no tokens row: this is the failed-metadata-fetch block.
	orphaned := []*types.TokenTransfer{
		f.transfer(t, d, zeroAddr, "0xholder1", "700", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder1", "0xholder2", "50", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder2", zeroAddr, "200", types.TokenTypeERC20),
	}
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: orphaned}))

	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM tokens WHERE address = $1)`, f.token).Scan(&exists))
	require.False(t, exists, "fixture is meant to have no tokens row yet")

	// Discovery succeeds on a later block, arriving through the same batch path
	// that creates token rows -- and carrying a transfer of its own.
	late := f.transfer(t, d, zeroAddr, "0xholder3", "1000", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Tokens: []*types.Token{{
			Address:     f.token,
			Symbol:      "CTR",
			Decimals:    18,
			TokenType:   types.TokenTypeERC20,
			BlockNumber: uint64(f.base),
		}},
		Transfers: []*types.TokenTransfer{late},
	}))

	// All four transfers, and a supply covering the two mints and the burn that
	// predate the row: 700 - 200 + 1000.
	f.requireAgrees(t, d, 4, "1500")
}

// TestTokenCounters_SupplyIsERC20OnlyButCountIsNot pins the split the two
// counters make. transfer_count is every transfer of the token whatever its
// type; total_supply is the ERC20 mint-minus-burn figure and must not move for
// a collection whose supply comes from nft_tokens instead.
func TestTokenCounters_SupplyIsERC20OnlyButCountIsNot(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("ERC721 token counts transfers but keeps supply untouched", func(t *testing.T) {
		f, drop := newCtrFixture(t, d, 4, 4_400_000)
		defer drop()
		f.createToken(t, d, types.TokenTypeERC721)

		trs := []*types.TokenTransfer{
			f.transfer(t, d, zeroAddr, "0xholder1", "1", types.TokenTypeERC721),
			f.transfer(t, d, "0xholder1", "0xholder2", "1", types.TokenTypeERC721),
		}
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: trs}))

		count, supply := f.stored(t, d)
		require.Equal(t, int64(2), count, "transfer_count is maintained for every token type")
		require.Nil(t, supply,
			"total_supply must stay NULL for an ERC721: it derives from nft_tokens, "+
				"and a delta from the mint here would be a second, conflicting definition")
	})

	t.Run("an ERC721 row under an ERC20 token does not move supply", func(t *testing.T) {
		f, drop := newCtrFixture(t, d, 5, 4_500_000)
		defer drop()
		f.createToken(t, d, types.TokenTypeERC20)

		// The recomputation this replaced filtered the sums on the TRANSFER
		// row's token_type, not the token's, so a stray non-ERC20 row under an
		// ERC20 token contributes to the count only.
		trs := []*types.TokenTransfer{
			f.transfer(t, d, zeroAddr, "0xholder1", "600", types.TokenTypeERC20),
			f.transfer(t, d, zeroAddr, "0xholder2", "9", types.TokenTypeERC721),
		}
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: trs}))
		f.requireAgrees(t, d, 2, "600")
	})
}

// TestTokenCounters_SingleRowWriterMaintainsThem covers the other writer of
// token_transfers. InsertTokenTransfer is not on the per-block path, but it is
// on the indexer's Database interface, and a writer that skips the counters
// desyncs them with nothing left to recompute from history.
func TestTokenCounters_SingleRowWriterMaintainsThem(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 6, 4_600_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)

	mint := f.transfer(t, d, zeroAddr, "0xholder1", "300", types.TokenTypeERC20)
	require.NoError(t, d.InsertTokenTransfer(ctx, mint))
	f.requireAgrees(t, d, 1, "300")

	burn := f.transfer(t, d, "0xholder1", zeroAddr, "100", types.TokenTypeERC20)
	require.NoError(t, d.InsertTokenTransfer(ctx, burn))
	f.requireAgrees(t, d, 2, "200")

	// Re-inserting the same row is a no-op and must not double-count.
	require.NoError(t, d.InsertTokenTransfer(ctx, burn))
	f.requireAgrees(t, d, 2, "200")
}

// TestTokenCounters_ReorgRevertsThem covers the delete side of the delta
// discipline.
//
// A reorg reverts blocks with DeleteBlock, and the cascade
// (transactions.block_number -> blocks, token_transfers.tx_hash ->
// transactions, both ON DELETE CASCADE) physically removes the block's
// transfers. Before the counters were maintained by deltas, RefreshTokenStats
// recomputed them absolutely on every block, so a reorg self-healed; with the
// recompute gone, a delete that does not decrement leaves them permanently
// high.
func TestTokenCounters_ReorgRevertsThem(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 7, 4_700_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)

	// Three blocks, one transfer each: a mint, a plain move, a burn.
	mint := f.transfer(t, d, zeroAddr, "0xholder1", "900", types.TokenTypeERC20)
	plain := f.transfer(t, d, "0xholder1", "0xholder2", "100", types.TokenTypeERC20)
	burn := f.transfer(t, d, "0xholder2", zeroAddr, "300", types.TokenTypeERC20)
	for _, tr := range []*types.TokenTransfer{mint, plain, burn} {
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}))
	}
	f.requireAgrees(t, d, 3, "600")

	// Revert the burn's block. Its 300 must come back out of the subtraction.
	require.NoError(t, d.DeleteBlock(ctx, burn.BlockNumber))
	f.requireAgrees(t, d, 2, "900")

	// Revert the mint's block too.
	require.NoError(t, d.DeleteBlock(ctx, mint.BlockNumber))
	f.requireAgrees(t, d, 1, "0")

	// And the last one, back to empty.
	require.NoError(t, d.DeleteBlock(ctx, plain.BlockNumber))
	f.requireAgrees(t, d, 0, "0")
}

// TestTokenCounters_ReindexAfterReorgDoesNotDoubleCount is the failure the
// reorg gap actually produced.
//
// Reverting a block removes the (tx_hash, log_index) rows, so the
// ON CONFLICT DO NOTHING that suppresses a re-delivered block no longer fires
// for the replacement blocks -- they insert fresh rows and count them again. An
// uncompensated delete therefore does not merely lag, it doubles.
func TestTokenCounters_ReindexAfterReorgDoesNotDoubleCount(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 8, 4_800_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)

	orphaned := []*types.TokenTransfer{
		f.transfer(t, d, zeroAddr, "0xholder1", "5000", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder1", zeroAddr, "1000", types.TokenTypeERC20),
	}
	for _, tr := range orphaned {
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}))
	}
	f.requireAgrees(t, d, 2, "4000")

	// The reorg reverts both blocks.
	for _, tr := range orphaned {
		require.NoError(t, d.DeleteBlock(ctx, tr.BlockNumber))
	}
	f.requireAgrees(t, d, 0, "0")

	// The canonical chain is re-indexed. New blocks, new tx hashes, same token
	// and the same economic effect.
	replacements := []*types.TokenTransfer{
		f.transfer(t, d, zeroAddr, "0xholder1", "5000", types.TokenTypeERC20),
		f.transfer(t, d, "0xholder1", zeroAddr, "1000", types.TokenTypeERC20),
	}
	for _, tr := range replacements {
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}))
	}

	// Two transfers and 4000, not four and 8000.
	f.requireAgrees(t, d, 2, "4000")
}

// TestTokenCounters_ReorgLeavesOtherTokensAlone pins that the compensation is
// scoped to the reverted block: a token whose transfers live in blocks that
// survive must not be decremented by another token's revert.
func TestTokenCounters_ReorgLeavesOtherTokensAlone(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	keep, dropKeep := newCtrFixture(t, d, 9, 4_900_000)
	defer dropKeep()
	revert, dropRevert := newCtrFixture(t, d, 10, 5_000_000)
	defer dropRevert()
	ctx := context.Background()

	keep.createToken(t, d, types.TokenTypeERC20)
	revert.createToken(t, d, types.TokenTypeERC20)

	keepMint := keep.transfer(t, d, zeroAddr, "0xholder1", "777", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{keepMint}}))
	revertMint := revert.transfer(t, d, zeroAddr, "0xholder1", "222", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{revertMint}}))

	require.NoError(t, d.DeleteBlock(ctx, revertMint.BlockNumber))

	revert.requireAgrees(t, d, 0, "0")
	keep.requireAgrees(t, d, 1, "777")
}

// TestTokenCounters_SupplySaturatesInsteadOfFailingTheBlock pins the blast
// radius decision at batch.go's clampSupply.
//
// Individual values fit NUMERIC(78, 0); an accumulated mint total need not.
// Because the counters now move inside the ingest transaction, an overflow
// raised on assignment would roll back the block's transfers and every retry
// would fail on the same block, so one token with an unrepresentable supply
// would stop the chain. The figure saturates instead.
func TestTokenCounters_SupplySaturatesInsteadOfFailingTheBlock(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 11, 5_100_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)

	// 78 nines: the largest value the column can hold. Two of these overflow it.
	huge := strRepeat("9", 78)
	for i := 0; i < 2; i++ {
		tr := f.transfer(t, d, zeroAddr, "0xholder1", huge, types.TokenTypeERC20)
		require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}),
			"an over-range supply must not fail the block (mint %d)", i+1)
	}

	// Both transfers landed -- the rows are what matter, and they are unaffected
	// by the derived figure saturating.
	var stored int64
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM token_transfers WHERE token_address = $1`, f.token).Scan(&stored))
	require.Equal(t, int64(2), stored, "the transfers must be committed regardless of the supply clamp")

	count, supply := f.stored(t, d)
	require.Equal(t, int64(2), count)
	require.Equal(t, huge, supply.String(), "supply should saturate at the column maximum")
}

// TestTokenCounters_TokenLocksAreTakenInSortedOrder pins the lock-ordering
// discipline for the tokens row locks this change added.
//
// Every per-token UPDATE holds a tokens row lock to commit, and
// InsertBlockDataBatch runs concurrently -- catchup gives each worker its own
// block. The deltas are keyed by a Go map, whose iteration order is randomised
// per call, so two workers touching the same two tokens would have acquired
// them in opposite orders and deadlocked. This is the rule InsertBalancesBatch
// documents and sorts for; a deadlock cannot be provoked on demand, so the
// order the statements actually run in is observed directly instead.
func TestTokenCounters_TokenLocksAreTakenInSortedOrder(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	// Several tokens sharing one fixture's blocks, created in an order that is
	// not the sorted one so a missing sort is visible.
	f, drop := newCtrFixture(t, d, 12, 5_200_000)
	defer drop()

	tokens := make([]string, 0, 5)
	for _, n := range []int{4, 1, 3, 0, 2} {
		addr := fmt.Sprintf("0xctrlock%034d", n)
		tokens = append(tokens, addr)
		_, err := d.pool.Exec(ctx,
			`INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
			 VALUES ($1, 'LCK', 'Lock Order', 18, 'ERC20', $2) ON CONFLICT (address) DO NOTHING`,
			addr, f.base)
		require.NoError(t, err)
	}
	defer func() {
		for _, addr := range tokens {
			if _, err := d.pool.Exec(ctx, `DELETE FROM token_transfers WHERE token_address = $1`, addr); err != nil {
				t.Errorf("cleanup transfers %s: %v", addr, err)
			}
			if _, err := d.pool.Exec(ctx, `DELETE FROM tokens WHERE address = $1`, addr); err != nil {
				t.Errorf("cleanup token %s: %v", addr, err)
			}
		}
	}()

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS token_lock_audit (seq BIGSERIAL PRIMARY KEY, addr TEXT NOT NULL)`,
		`CREATE OR REPLACE FUNCTION audit_token_lock_order() RETURNS TRIGGER AS $$
		 BEGIN
			IF NEW.address LIKE '0xctrlock%' THEN
				INSERT INTO token_lock_audit (addr) VALUES (NEW.address);
			END IF;
			RETURN NULL;
		 END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER trg_audit_token_lock AFTER INSERT OR UPDATE ON tokens
			FOR EACH ROW EXECUTE FUNCTION audit_token_lock_order()`,
	} {
		_, err := d.pool.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	defer func() {
		for _, stmt := range []string{
			`DROP TRIGGER IF EXISTS trg_audit_token_lock ON tokens`,
			`DROP FUNCTION IF EXISTS audit_token_lock_order()`,
			`DROP TABLE IF EXISTS token_lock_audit`,
		} {
			if _, err := d.pool.Exec(ctx, stmt); err != nil {
				t.Errorf("teardown %q: %v", stmt, err)
			}
		}
	}()

	// One batch touching every token, built in the unsorted order above.
	var transfers []*types.TokenTransfer
	for _, addr := range tokens {
		tr := f.transfer(t, d, zeroAddr, "0xholder1", "10", types.TokenTypeERC20)
		tr.TokenAddress = addr
		transfers = append(transfers, tr)
	}
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: transfers}))

	rows, err := d.pool.Query(ctx, `SELECT addr FROM token_lock_audit ORDER BY seq`)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var a string
		require.NoError(t, rows.Scan(&a))
		got = append(got, a)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, len(tokens), "every token should have been updated once")

	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i], got[i-1],
			"tokens row locks were taken out of order at index %d (%s after %s): the sort in "+
				"InsertBlockDataBatch is gone, and two concurrent batches sharing these tokens can deadlock",
			i, got[i], got[i-1])
	}
}

// TestTokenCounters_CrossPhaseLockingDoesNotDeadlock is the case sorting each
// phase separately does not cover.
//
// A transaction takes tokens row locks in two runs: the upsert over the tokens
// the block discovered, and the delta UPDATEs over the tokens its transfers
// touched. sorted(A) then sorted(B) is a total order only when A and B are the
// same set, and they differ whenever a token already in the database is absent
// from this process's cache -- an ON CONFLICT DO UPDATE locks an existing row
// just as much as it creates one. One worker then takes a high address
// (discovered) before a low one (transfer) while a worker with a warm cache
// takes the low one first, and the two cycle.
//
// The two shapes below are exactly that pair, run against each other. The
// property is the absence of a 40P01, which is why this is a concurrency test
// rather than an ordering one: the fix is a SELECT ... FOR UPDATE pass over the
// union, and a lock taken without a write is invisible to the AFTER trigger the
// within-phase test uses.
func TestTokenCounters_CrossPhaseLockingDoesNotDeadlock(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newCtrFixture(t, d, 13, 5_300_000)
	defer drop()

	const keys = 8
	addrs := make([]string, keys)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("0xctrphase%031d", i)
	}
	// All of them exist: the asymmetry being tested is about cache state, not
	// about whether the row is there.
	for _, addr := range addrs {
		_, err := d.pool.Exec(ctx, `DELETE FROM token_transfers WHERE token_address = $1`, addr)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx,
			`INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
			 VALUES ($1, 'PH', 'Phase', 18, 'ERC20', $2)
			 ON CONFLICT (address) DO NOTHING`, addr, f.base)
		require.NoError(t, err)
	}
	defer func() {
		for _, addr := range addrs {
			if _, err := d.pool.Exec(ctx, `DELETE FROM token_transfers WHERE token_address = $1`, addr); err != nil {
				t.Errorf("cleanup transfers %s: %v", addr, err)
			}
			if _, err := d.pool.Exec(ctx, `DELETE FROM tokens WHERE address = $1`, addr); err != nil {
				t.Errorf("cleanup token %s: %v", addr, err)
			}
		}
	}()

	// Worker 0: discovers the DESCENDING half (upsert phase) and transfers the
	// ascending half (delta phase) -- so per-phase sorting gives it high before
	// low. Worker 1: cache warm, discovers nothing, transfers everything in
	// ascending order. Opposite acquisition orders over a shared set.
	build := func(round, writer int) *BlockData {
		bd := &BlockData{}
		for i, addr := range addrs {
			tr := f.transfer(t, d, zeroAddr, "0xholder1", "1", types.TokenTypeERC20)
			tr.TokenAddress = addr
			bd.Transfers = append(bd.Transfers, tr)
			if writer == 0 && i >= keys/2 {
				bd.Tokens = append(bd.Tokens, &types.Token{
					Address:     addrs[keys-1-i+keys/2-1],
					Symbol:      "PH",
					Decimals:    18,
					TokenType:   types.TokenTypeERC20,
					BlockNumber: uint64(f.base),
				})
			}
		}
		return bd
	}

	const rounds = 12
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		payloads := []*BlockData{build(round, 0), build(round, 1)}
		for writer := 0; writer < 2; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				errs[writer] = d.InsertBlockDataBatch(ctx, payloads[writer])
			}(writer)
		}
		wg.Wait()
		for writer, err := range errs {
			require.NoErrorf(t, err,
				"round %d writer %d: the upsert set and the transfer set differ between these two "+
					"batches, so without one sorted pass over the union of every tokens row the "+
					"transaction locks, they acquire them in opposite orders and deadlock",
				round, writer)
		}
	}

	// And the counters still agree with the stored rows for every token.
	for _, addr := range addrs {
		var cached, actual int64
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT COALESCE(transfer_count, 0) FROM tokens WHERE address = $1`, addr).Scan(&cached))
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM token_transfers WHERE token_address = $1`, addr).Scan(&actual))
		require.Equal(t, actual, cached, "counter drifted for %s under concurrent cross-phase batches", addr)
	}
}

// TestTokenCounters_ReorgRevertsChainCounters covers the other counters the
// reorg cascade invalidates.
//
// Compensating only transfers_total left blocks_total and transactions_total
// drifting, which is the same fix-one-site mistake one level up from the
// per-token counters. addresses_total is deliberately not reverted --
// address_stats has no foreign key into transactions, so the cascade does not
// reach it and an address seen in a reverted block has still been seen.
func TestTokenCounters_ReorgRevertsChainCounters(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	f, drop := newCtrFixture(t, d, 14, 5_400_000)
	defer drop()
	ctx := context.Background()

	f.createToken(t, d, types.TokenTypeERC20)
	tr := f.transfer(t, d, zeroAddr, "0xholder1", "10", types.TokenTypeERC20)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{Transfers: []*types.TokenTransfer{tr}}))

	// Seeded high so the GREATEST(..., 0) floor is not what the assertion
	// measures -- the floor is a safety net, not the behaviour under test.
	const seed = 1000
	for _, name := range []string{"blocks_total", "transactions_total", "transfers_total", "addresses_total"} {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO chain_counters (name, count, updated_at) VALUES ($1, $2, NOW())
			ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()`, name, seed)
		require.NoError(t, err)
	}

	read := func(name string) int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT count FROM chain_counters WHERE name = $1`, name).Scan(&n))
		return n
	}

	// The fixture wrote one block and one transaction for this transfer.
	require.NoError(t, d.DeleteBlock(ctx, tr.BlockNumber))

	require.Equal(t, int64(seed-1), read("blocks_total"), "blocks_total should lose the reverted block")
	require.Equal(t, int64(seed-1), read("transactions_total"), "transactions_total should lose the reverted transaction")
	require.Equal(t, int64(seed-1), read("transfers_total"), "transfers_total should lose the reverted transfer")
	require.Equal(t, int64(seed), read("addresses_total"),
		"addresses_total must not move: the cascade does not reach address_stats")
}
