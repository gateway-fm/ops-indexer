package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

// TestGetTransactionsByAddress_TransferOnlyTxNotDropped guards the by-address
// UNION rewrite against a regression found in review: the transfer branch must
// limit by DISTINCT tx_hash, not by transfer-log row. A recent tx that emits
// many Transfer logs to the address (airdrop / batch / swap / NFT mint) must not
// crowd older transfer-only transactions off the page.
//
// Setup: pure-recipient A. Recent tx H1@block100 emits `limit` transfers to A;
// older tx H2@block50 emits 1. With a row-based limit, H1's logs fill the
// subquery and H2 is dropped. The correct result keeps both.
func TestGetTransactionsByAddress_TransferOnlyTxNotDropped(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		A   = "0xaaaa000000000000000000000000000000000001" // pure recipient
		oth = "0xbbbb000000000000000000000000000000000002"
		tok = "0xcccc000000000000000000000000000000000003"
		H1  = "0x1111000000000000000000000000000000000000000000000000000000000001" // block 100
		H2  = "0x2222000000000000000000000000000000000000000000000000000000000002" // block 50
		lim = 3
	)

	seedBlock := func(n uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size, difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 1, $5, 500, '0', '0', '0x0', '', '', '', '')`,
			n, fmt.Sprintf("0xblk%061x", n), fmt.Sprintf("0xblk%061x", n-1), 1_700_000_000+int64(n), oth)
		require.NoError(t, err)
	}
	seedTx := func(hash string, block uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $3, 0, $4, $5, 0, 21000, 1, 0, 0, '0x', 1)`, hash, block, 1_700_000_000+int64(block), oth, oth)
		require.NoError(t, err)
	}
	seedTransfer := func(hash string, logIndex int, block uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, transfer_type, token_type)
			VALUES ($1, $2, $3, $4, $5, 1, $6, $7, 'transfer', 'ERC20')`,
			hash, logIndex, tok, oth, A, block, 1_700_000_000+int64(block))
		require.NoError(t, err)
	}

	seedBlock(50)
	seedBlock(100)
	seedTx(H2, 50)
	seedTx(H1, 100)
	for i := 0; i < lim; i++ { // recent tx emitted `lim` Transfer logs to A
		seedTransfer(H1, i, 100)
	}
	seedTransfer(H2, 0, 50) // one older single transfer

	assertBothReturned := func(label string, txs []types.Transaction) {
		seen := map[string]bool{}
		for _, tx := range txs {
			seen[tx.Hash] = true
		}
		require.Truef(t, seen[H1], "%s: H1 (recent) missing", label)
		require.Truef(t, seen[H2], "%s: H2 (older transfer-only tx) was dropped", label)
		require.Lenf(t, txs, 2, "%s: expected exactly H1 and H2", label)
	}

	txs, err := d.GetTransactionsByAddress(ctx, A, lim, nil)
	require.NoError(t, err)
	assertBothReturned("no-cursor", txs)

	// Deep-pagination branch has the same subquery shape — exercise it too.
	before := uint64(200)
	txs, err = d.GetTransactionsByAddress(ctx, A, lim, &before)
	require.NoError(t, err)
	assertBothReturned("beforeBlock", txs)
}

// TestGetAllTokenTransfers_UnfilteredTotalUsesCounter verifies the global feed's
// unfiltered total is served from the chain_counters running total and stays in
// lock-step with the actual row count as batches land — i.e. the counter is
// wired through the batch writer and not silently stale.
func TestGetAllTokenTransfers_UnfilteredTotalUsesCounter(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	to := "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
	from := "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

	// One batch = one block + one tx + n transfers on that tx.
	insertBatch := func(blockNum uint64, txHash string, nTransfers int) {
		toAddr := to
		transfers := make([]*types.TokenTransfer, nTransfers)
		for i := 0; i < nTransfers; i++ {
			transfers[i] = &types.TokenTransfer{
				TxHash: txHash, LogIndex: i,
				TokenAddress: "0xcccc000000000000000000000000000000000003",
				From:         from, To: to, Value: types.JSONString("1"),
				BlockNumber: blockNum, TransferType: "transfer", TokenType: "ERC20",
			}
		}
		err := d.InsertBlockDataBatch(ctx, &BlockData{
			Block: &types.Block{
				Number: blockNum, Hash: fmt.Sprintf("0xblk%061x", blockNum),
				ParentHash: fmt.Sprintf("0xblk%061x", blockNum-1), Timestamp: 1_700_000_000 + blockNum,
				GasUsed: 21000, GasLimit: 30000000, TransactionCount: 1, Miner: from,
				Size: 500, Difficulty: "0", TotalDifficulty: "0", Nonce: "0x0",
			},
			Transactions: []*types.Transaction{{
				Hash: txHash, BlockNumber: blockNum, TxIndex: 0, From: from, To: &toAddr,
				Value: types.JSONString("0"), GasUsed: 21000, GasPrice: 1, Status: 1,
			}},
			Transfers: transfers,
		})
		require.NoError(t, err)
	}

	counter := func() int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT COALESCE((SELECT count FROM chain_counters WHERE name = 'transfers_total'), -1)`).Scan(&n))
		return n
	}
	liveCount := func() int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_transfers`).Scan(&n))
		return n
	}

	insertBatch(100, "0x1111000000000000000000000000000000000000000000000000000000000001", 3)
	insertBatch(101, "0x2222000000000000000000000000000000000000000000000000000000000002", 2)

	_, total, err := d.GetAllTokenTransfers(ctx, "", 25, 0)
	require.NoError(t, err)
	require.Equal(t, int64(5), total, "unfiltered total should equal rows inserted")
	require.Equal(t, liveCount(), total, "feed total should match COUNT(*)")
	require.Equal(t, counter(), total, "feed total should come from the transfers_total counter")
}
