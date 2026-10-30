package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

var (
	reorgBalToken = "0x00000000000000000000000000000000000b0020"
	reorgBalNFT   = "0x00000000000000000000000000000000000b0721"
	reorgBalA     = "0x000000000000000000000000000000000000000a"
	reorgBalB     = "0x000000000000000000000000000000000000000b"
	reorgBalO     = "0x0000000000000000000000000000000000000001"
	reorgBalX     = "0x0000000000000000000000000000000000000002"
)

func reorgBalBlock(number uint64, transfers ...*types.TokenTransfer) *BlockData {
	txHash := fmt.Sprintf("tx-bal-%d", number)
	for k, tr := range transfers {
		tr.TxHash, tr.LogIndex, tr.BlockNumber = txHash, k, number
	}
	return &BlockData{
		Block:        &types.Block{Number: number, Hash: fmt.Sprintf("bal-%d", number), ParentHash: fmt.Sprintf("bal-%d", number-1), Timestamp: 1_700_000_000 + number},
		Transactions: []*types.Transaction{{Hash: txHash, BlockNumber: number, From: reorgBalA, To: &reorgBalToken, Value: "0", Status: 1}},
		Transfers:    transfers,
		Tokens: []*types.Token{
			{Address: reorgBalToken, Symbol: "T", Decimals: 18, TokenType: types.TokenTypeERC20, BlockNumber: number},
			{Address: reorgBalNFT, Symbol: "N", TokenType: types.TokenTypeERC721, BlockNumber: number},
		},
		SkipAddressStats: true,
	}
}

func reorgBalTransfer(token, tokenType, from, to, value string, id *string) *types.TokenTransfer {
	transferType := types.TransferTypeTransfer
	if from == zeroAddress {
		transferType = types.TransferTypeMint
	}
	return &types.TokenTransfer{TokenAddress: token, From: from, To: to, Value: types.JSONString(value), TransferType: transferType, TokenType: tokenType, TokenID: id}
}

func reorgBalances(number uint64, rows ...[3]string) []*types.Balance {
	var b []*types.Balance
	for _, r := range rows {
		b = append(b, &types.Balance{Address: r[0], TokenAddress: r[1], BlockNumber: number, Balance: types.JSONString(r[2])})
	}
	return b
}

// A reorg takes off the balances its reverted block wrote: the history rows of the block, the current balance each
// holder had before it, and the holder counts.
func TestReorgRevertsTheBalancesOfItsBlock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	id := "1"

	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil),
		reorgBalTransfer(reorgBalNFT, types.TokenTypeERC721, zeroAddress, reorgBalO, "1", &id))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1,
		[3]string{reorgBalA, reorgBalToken, "10"},
		[3]string{reorgBalO, reorgBalNFT, "1"})))

	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil),
		reorgBalTransfer(reorgBalNFT, types.TokenTypeERC721, reorgBalO, reorgBalX, "1", &id))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2,
		[3]string{reorgBalA, reorgBalToken, "6"},
		[3]string{reorgBalB, reorgBalToken, "4"},
		[3]string{reorgBalO, reorgBalNFT, "0"},
		[3]string{reorgBalX, reorgBalNFT, "1"})))

	require.NoError(t, d.DeleteBlock(ctx, 2))

	var history int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM balances WHERE block_number = 2`).Scan(&history))
	assert.Zero(t, history, "balance history rows of the reverted block")

	current := map[string]string{}
	rows, err := d.pool.Query(ctx, `SELECT token_address, address, balance::TEXT FROM token_balances_current WHERE balance > 0`)
	require.NoError(t, err)
	for rows.Next() {
		var token, holder, balance string
		require.NoError(t, rows.Scan(&token, &holder, &balance))
		current[token+" "+holder] = balance
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]string{
		reorgBalToken + " " + reorgBalA: "10",
		reorgBalNFT + " " + reorgBalO:   "1",
	}, current, "current balances after the revert")

	holders := map[string]int{}
	rows, err = d.pool.Query(ctx, `SELECT address, holder_count FROM tokens`)
	require.NoError(t, err)
	for rows.Next() {
		var token string
		var n int
		require.NoError(t, rows.Scan(&token, &n))
		holders[token] = n
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]int{reorgBalToken: 1, reorgBalNFT: 1}, holders, "holder counts after the revert")
}
