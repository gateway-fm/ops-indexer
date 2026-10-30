package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func rereads(t *testing.T, d *DB) map[string]int64 {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT token_address || ' ' || address, epoch FROM balance_rereads WHERE open`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var k string
		var epoch int64
		require.NoError(t, rows.Scan(&k, &epoch))
		out[k] = epoch
	}
	require.NoError(t, rows.Err())
	return out
}

func currentBalance(t *testing.T, d *DB, token, holder string) (string, bool) {
	t.Helper()
	var b string
	err := d.pool.QueryRow(context.Background(), `
		SELECT balance::TEXT FROM token_balances_current WHERE token_address = $1 AND address = $2`, token, holder).Scan(&b)
	if err != nil {
		return "", false
	}
	return b, true
}

func holderCount(t *testing.T, d *DB, token string) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT COALESCE(holder_count, 0) FROM tokens WHERE address = $1`, token).Scan(&n))
	return n
}

// The revert asks for every key of its block to be read again: the keys with history rows at the block and the
// holders of its transfers whose reads had not been written yet.
func TestARevertRequestsARereadOfEveryKeyItsBlockTouched(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	id := "1"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil),
		reorgBalTransfer(reorgBalNFT, types.TokenTypeERC721, zeroAddress, reorgBalO, "1", &id))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "6"})))

	require.NoError(t, d.DeleteBlock(ctx, 2))
	got := rereads(t, d)
	require.Len(t, got, 3, "A's row at 2, and B's and O's unflushed reads: %v", got)
	for _, k := range []string{reorgBalToken + " " + reorgBalA, reorgBalToken + " " + reorgBalB, reorgBalNFT + " " + reorgBalO} {
		require.Contains(t, got, k)
	}
	b, ok := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.True(t, ok)
	require.Equal(t, "10", b, "the latest remaining row, until the reread")
}

// A read decided before the revert (on the fork) lands after it, stamped with the reverted block: it carries an older
// epoch than the key's and is not written, while the request is open and after it closed. A read at the key's epoch
// is written and closes the request; reads at the key's epoch or later then go through the highest-block guard.
func TestALateReadOfARevertedBlockIsNotWrittenOverTheReread(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	epoch := rereads(t, d)[reorgBalToken+" "+reorgBalB]
	require.NotZero(t, epoch)
	require.Equal(t, epoch, currentEpoch(t, d))

	fork := reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "6"}, [3]string{reorgBalB, reorgBalToken, "4"})
	for _, b := range fork {
		b.Epoch = epoch - 1
	}
	require.NoError(t, d.InsertBalancesBatch(ctx, fork), "the fork's reads land late")
	_, ok := currentBalance(t, d, reorgBalToken, reorgBalB)
	require.False(t, ok, "the fork's value for B is not written")
	b, _ := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.Equal(t, "10", b)
	require.Equal(t, 0, countWhereDB(t, d, `SELECT count(*) FROM balances WHERE block_number = 2`))
	require.Equal(t, 1, holderCount(t, d, reorgBalToken))

	answer := reorgBalances(1, [3]string{reorgBalB, reorgBalToken, "0"})
	answer[0].Epoch = epoch
	require.NoError(t, d.InsertBalancesBatch(ctx, answer))
	require.NotContains(t, rereads(t, d), reorgBalToken+" "+reorgBalB, "answered")
	require.Contains(t, rereads(t, d), reorgBalToken+" "+reorgBalA, "A's request is still open")

	require.NoError(t, d.InsertBalancesBatch(ctx, fork), "the fork's reads land after the request closed")
	b, _ = currentBalance(t, d, reorgBalToken, reorgBalB)
	require.Equal(t, "0", b, "still the reread's value, not the fork's")

	later := reorgBalances(3, [3]string{reorgBalB, reorgBalToken, "3"})
	later[0].Epoch = epoch
	require.NoError(t, d.InsertBalancesBatch(ctx, later))
	b, _ = currentBalance(t, d, reorgBalToken, reorgBalB)
	require.Equal(t, "3", b, "a read decided after the revert is written as usual")
}

// currentEpoch is the epoch the balance pool would stamp on a read decided now.
func currentEpoch(t *testing.T, d *DB) int64 {
	t.Helper()
	e, err := d.CurrentBalanceEpoch(context.Background())
	require.NoError(t, err)
	return e
}

// The reread, stamped with the stored head at its claim, and a later read stamped with the new chain's next block:
// whichever flushes first, the later block's value is the current one.
func TestTheRereadAndALaterReadKeepTheLaterBlockInEitherFlushOrder(t *testing.T) {
	for _, rereadFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "reread first", false: "later read first"}[rereadFirst], func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
				reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
			require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
			require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
				reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
			require.NoError(t, d.DeleteBlock(ctx, 2))
			epoch := currentEpoch(t, d)
			reread := reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})
			reread[0].Epoch = epoch
			later := reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "7"})
			later[0].Epoch = epoch
			first, second := reread, later
			if !rereadFirst {
				first, second = later, reread
			}
			require.NoError(t, d.InsertBalancesBatch(ctx, first))
			require.NoError(t, d.InsertBalancesBatch(ctx, second))
			b, _ := currentBalance(t, d, reorgBalToken, reorgBalA)
			require.Equal(t, "7", b)
			require.Empty(t, rereads(t, d)[reorgBalToken+" "+reorgBalA], "the request is closed")
		})
	}
}

// The stats backfill writes current balances with no epoch: for a key a revert touched it is not written, and the
// key's reread puts the node's value there.
func TestABackfillBalanceOfARevertedKeyIsLeftToTheReread(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.NoError(t, d.InsertCurrentBalances(ctx, reorgBalances(0, [3]string{reorgBalA, reorgBalToken, "9"})))
	_, ok := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.False(t, ok, "the backfill's read carries no epoch")
	answer := reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})
	answer[0].Epoch = rereads(t, d)[reorgBalToken+" "+reorgBalA]
	require.NoError(t, d.InsertBalancesBatch(ctx, answer))
	b, _ := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.Equal(t, "10", b, "the reread's value")
}

func countWhereDB(t *testing.T, d *DB, sql string) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), sql).Scan(&n))
	return n
}

// A holder whose current row the stats backfill wrote (no history row) and whose only history row is the reverted
// block's: the revert removes its current row, and the reread puts the node's value back.
func TestARevertRemovesACurrentRowItCannotRestoreAndTheRereadPutsItBack(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1)))
	require.NoError(t, d.InsertCurrentBalances(ctx, reorgBalances(0, [3]string{reorgBalA, reorgBalToken, "10"})))
	require.Equal(t, 1, holderCount(t, d, reorgBalToken))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "6"})))

	require.NoError(t, d.DeleteBlock(ctx, 2))
	_, ok := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.False(t, ok, "nothing remains to restore it from")
	require.Equal(t, 0, holderCount(t, d, reorgBalToken))

	answer := reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})
	answer[0].Epoch = rereads(t, d)[reorgBalToken+" "+reorgBalA]
	require.NoError(t, d.InsertBalancesBatch(ctx, answer))
	b, _ := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.Equal(t, "10", b)
	require.Equal(t, 1, holderCount(t, d, reorgBalToken))
}

// An ERC-1155 transfer has no balance rows (balanceOf(holder) does not answer for it): the revert neither requests a
// read for it nor moves a holder count.
func TestARevertLeavesERC1155TransfersAlone(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	id := "7"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalNFT, "ERC1155", zeroAddress, reorgBalX, "3", &id))))
	before := holderCount(t, d, reorgBalNFT)
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.Empty(t, rereads(t, d))
	require.Equal(t, before, holderCount(t, d, reorgBalNFT))
}

// The open requests are handed out with an attempt counted each time, and dropped after the last attempt.
func TestBalanceRereadsAreClaimedAndDroppedAfterTheirAttempts(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	for attempt := 1; attempt <= MaxBalanceRereadAttempts; attempt++ {
		claimed, dropped, err := d.ClaimBalanceRereads(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, dropped)
		require.Len(t, claimed, 2, "attempt %d: A's and B's", attempt)
	}
	claimed, dropped, err := d.ClaimBalanceRereads(ctx, 10)
	require.NoError(t, err)
	require.Len(t, dropped, 2)
	require.Empty(t, claimed)
	require.Empty(t, rereads(t, d))
}

// A wipe forgets the open requests with the chain they were made for.
func TestAWipeForgetsTheBalanceRereads(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.NotEmpty(t, rereads(t, d))
	require.NoError(t, d.WipeAllData(ctx))
	require.Empty(t, rereads(t, d))
}

// A balance flush and a revert of the same key run one after the other in either order, and a block write of a later
// block meanwhile waits on the revert's tokens rows without a cycle.
func TestABalanceFlushAndARevertDoNotDeadlockInEitherOrder(t *testing.T) {
	for _, flushFirst := range []bool{true, false} {
		name := "revert first"
		if flushFirst {
			name = "flush first"
		}
		t.Run(name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			d = withPoolSize(t, d, 16)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
				reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
			require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
			require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
				reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
			require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "6"})))

			holder, err := d.pool.Begin(ctx)
			require.NoError(t, err)
			defer holder.Rollback(context.Background())
			if flushFirst {
				_, err = holder.Exec(ctx, tbcLockKeysSQL, tbcLockKeys([]string{reorgBalToken}, []string{reorgBalA}))
			} else {
				_, err = holder.Exec(ctx, `SELECT 1 FROM tokens WHERE address = $1 FOR UPDATE`, reorgBalToken)
			}
			require.NoError(t, err)

			reverted := make(chan error, 1)
			flushed := make(chan error, 1)
			written := make(chan error, 1)
			go func() { reverted <- d.DeleteBlock(ctx, 2) }()
			go func() {
				flushed <- d.InsertBalancesBatch(ctx, reorgBalances(2, [3]string{reorgBalA, reorgBalToken, "6"}))
			}()
			go func() {
				written <- d.InsertBlockDataBatch(ctx, reorgBalBlock(3,
					reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalX, "1", nil)))
			}()
			time.Sleep(300 * time.Millisecond)
			require.NoError(t, holder.Commit(ctx))
			require.NoError(t, <-reverted)
			require.NoError(t, <-flushed)
			require.NoError(t, <-written)
		})
	}
}
