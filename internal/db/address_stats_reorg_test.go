package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// Block 10: A sends B a transaction carrying a transfer A->B, and an internal call A->B, as the writer counts it.
func reorgBlock(hash string) *BlockData {
	ts := uint64(1_700_000_010)
	to := "0xb"
	return &BlockData{
		Block:        &types.Block{Number: 10, Hash: hash, ParentHash: "blk9", Timestamp: ts},
		Transactions: []*types.Transaction{{Hash: "t" + hash, BlockNumber: 10, From: "0xa", To: &to, Value: "0", TxType: 2, Status: 1}},
		Transfers: []*types.TokenTransfer{{TxHash: "t" + hash, LogIndex: 0, TokenAddress: "0x70c1", From: "0xa", To: "0xb",
			Value: "1", BlockNumber: 10, Timestamp: &ts, TokenType: types.TokenTypeERC20}},
		InternalTransactions: []*types.InternalTransaction{{TxHash: "t" + hash, BlockNumber: 10, TraceAddress: "0", From: "0xa", To: &to, Value: "0", CallType: "call"}},
		AddressStats: map[string]*AddressStatsDelta{
			"0xa": {Address: "0xa", TxCountDelta: 1, SentDelta: 1, TokenTransferDelta: 1, InternalTxCountDelta: 1, BlockNumber: 10},
			"0xb": {Address: "0xb", TxCountDelta: 1, TokenTransferDelta: 1, InternalTxCountDelta: 1, BlockNumber: 10},
		},
	}
}

// Block 9: A sends C a transaction, so A's row outlives the revert of block 10 and its counters must come out exact.
func earlierBlock() *BlockData {
	ts := uint64(1_700_000_009)
	to := "0xc"
	return &BlockData{
		Block:        &types.Block{Number: 9, Hash: "blk9", ParentHash: "blk8", Timestamp: ts},
		Transactions: []*types.Transaction{{Hash: "t9", BlockNumber: 9, From: "0xa", To: &to, Value: "0", TxType: 2, Status: 1}},
		AddressStats: map[string]*AddressStatsDelta{
			"0xa": {Address: "0xa", TxCountDelta: 1, SentDelta: 1, BlockNumber: 9},
			"0xc": {Address: "0xc", TxCountDelta: 1, BlockNumber: 9},
		},
	}
}

func TestReorgRevertsAddressCounters(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	counters := func(address string) [4]int64 {
		var c [4]int64
		err := d.pool.QueryRow(ctx, `
			SELECT tx_count, sent_count, internal_tx_count, token_transfer_count FROM address_stats WHERE address = $1`, address).
			Scan(&c[0], &c[1], &c[2], &c[3])
		if err == pgx.ErrNoRows {
			return c
		}
		require.NoError(t, err)
		return c
	}

	require.NoError(t, d.InsertBlockDataBatch(ctx, earlierBlock()))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBlock("old")))
	require.NoError(t, d.DeleteBlock(ctx, 10))
	require.Equal(t, [4]int64{1, 1, 0, 0}, counters("0xa"), "the reverted block's counts are taken off, sent included")
	require.Equal(t, [4]int64{0, 0, 0, 0}, counters("0xb"), "an address only the reverted block had seen has no row")
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBlock("new")))
	require.Equal(t, [4]int64{2, 2, 1, 1}, counters("0xa"), "the replacement counts once")
	require.Equal(t, [4]int64{1, 0, 1, 1}, counters("0xb"))
	for b := 0; b < addressBuckets; b++ {
		drifts, err := d.CheckRunning(ctx, b, false)
		require.NoError(t, err)
		for _, dr := range drifts {
			require.NotEqual(t, "address", dr.Family, "address counters agree with the rows: %+v", dr)
		}
	}
}
