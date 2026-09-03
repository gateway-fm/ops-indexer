package db

import (
	"context"
	"fmt"
	"math/big"
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
	token string
	base  int64
	seq   int
}

func newCtrFixture(t *testing.T, d *DB, id int64, baseBlock int64) (*ctrFixture, func()) {
	t.Helper()
	ctx := context.Background()
	f := &ctrFixture{token: fmt.Sprintf("0xctr%038d", id), base: baseBlock}

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
	hash := fmt.Sprintf("0xctr%s-%d", f.token[5:12], f.seq)
	ts := uint64(time.Now().Unix())

	_, err := d.pool.Exec(ctx,
		`INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		 VALUES ($1, $2, '0xp', $3, 1, 1, 1) ON CONFLICT (number) DO NOTHING`,
		block, fmt.Sprintf("0xblk%s-%d", f.token[5:12], f.seq), ts)
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
