package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// perTxBlock is block 20 as a chain that numbers logIndex per transaction returns it: each of its two transactions
// has a Transfer log at index 0. withLogs false leaves only the transfers, to reach the token_transfers check alone.
func perTxBlock(withLogs bool) *BlockData {
	to := "0xb"
	data := &BlockData{
		Block: &types.Block{Number: 20, Hash: "blk20", ParentHash: "blk19", Timestamp: 1_700_000_020},
		Transactions: []*types.Transaction{
			{Hash: "0xtx1", BlockNumber: 20, From: "0xa", To: &to, Value: "0", Status: 1},
			{Hash: "0xtx2", BlockNumber: 20, TxIndex: 1, From: "0xa", To: &to, Value: "0", Status: 1},
		},
		SkipAddressStats: true,
	}
	for _, hash := range []string{"0xtx1", "0xtx2"} {
		if withLogs {
			data.Logs = append(data.Logs, &types.Log{TxHash: hash, LogIndex: 0, Address: "0x70c1", Data: "0x01", BlockNumber: 20})
		}
		data.Transfers = append(data.Transfers, &types.TokenTransfer{TxHash: hash, LogIndex: 0, TokenAddress: "0x70c1",
			From: "0xa", To: "0xb", Value: "1", BlockNumber: 20, TransferType: types.TransferTypeTransfer,
			TokenType: types.TokenTypeERC20})
	}
	return data
}

func requireNothingStoredForBlock20(t *testing.T, d *DB) {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM blocks WHERE number = 20) + (SELECT count(*) FROM transactions WHERE block_number = 20) +
		       (SELECT count(*) FROM logs WHERE block_number = 20) + (SELECT count(*) FROM token_transfers WHERE block_number = 20)`).
		Scan(&n))
	require.Zero(t, n, "nothing of the block is committed")
}

// Two different logs at one (block_number, log_index) stop the block's write with an error that names the block, the
// index and both transactions, instead of keeping the first and dropping the second.
func TestTwoLogsAtOneIndexStopIngest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		withLogs bool
		table    string
	}{{"logs", true, "logs"}, {"token_transfers", false, "token_transfers"}} {
		t.Run(tc.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			err := d.InsertBlockDataBatch(context.Background(), perTxBlock(tc.withLogs))
			require.ErrorIs(t, err, ErrLogIndexPerTransaction)
			require.ErrorContains(t, err, "block 20, log_index 0: "+tc.table+" holds the log of tx 0xtx1, and tx 0xtx2")
			requireNothingStoredForBlock20(t, d)
		})
	}
}

// The same block written twice, as realtime and catch-up both can, is not an error and stores each log once.
func TestTheSameLogsWrittenTwiceAreStoredOnce(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	block := func() *BlockData {
		data := perTxBlock(true)
		data.Logs[1].LogIndex, data.Transfers[1].LogIndex = 1, 1
		return data
	}
	require.NoError(t, d.InsertBlockDataBatch(ctx, block()))
	require.NoError(t, d.InsertBlockDataBatch(ctx, block()))
	var logs, transfers int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM logs WHERE block_number = 20), (SELECT count(*) FROM token_transfers WHERE block_number = 20)`).
		Scan(&logs, &transfers))
	require.Equal(t, 2, logs)
	require.Equal(t, 2, transfers)
}
