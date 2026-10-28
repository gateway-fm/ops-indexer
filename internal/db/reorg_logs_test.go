package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// Since 013 a block's logs are keyed by (block_number, log_index) and DeleteBlock removes them
// itself. After a reorg the replacement block's logs are stored, not the old fork's.
func TestReorgReplacesTheBlocksLogs(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	block := func(fork string) *BlockData {
		to := "0xb"
		return &BlockData{
			Block:            &types.Block{Number: 10, Hash: "blk" + fork, ParentHash: "blk9", Timestamp: 1_700_000_010},
			Transactions:     []*types.Transaction{{Hash: "tx" + fork, BlockNumber: 10, From: "0xa", To: &to, Value: "0", Status: 1}},
			Logs:             []*types.Log{{TxHash: "tx" + fork, LogIndex: 0, Address: "0xc", Data: "0x" + fork, BlockNumber: 10}},
			SkipAddressStats: true,
		}
	}
	require.NoError(t, d.InsertBlockDataBatch(ctx, block("01")))
	require.NoError(t, d.DeleteBlock(ctx, 10))
	require.NoError(t, d.InsertBlockDataBatch(ctx, block("02")))

	var txHash, data string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT tx_hash, data FROM logs WHERE block_number = 10`).Scan(&txHash, &data))
	require.Equal(t, "tx02", txHash, "the new fork's log, not the reverted one")
	require.Equal(t, "0x02", data)
}
