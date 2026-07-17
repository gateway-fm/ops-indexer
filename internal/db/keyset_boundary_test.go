package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// RD-1148 regression guards: paging the address feeds with the keyset cursor
// must not skip rows when a page boundary falls inside a block. The old bare
// `block_number < $before` bound dropped every remaining row of the boundary
// block; the row-value keyset `(block_number, idx) < (bound)` resumes exactly
// after the cursor row.

// TestGetTransactionsByAddress_KeysetBlockBoundary seeds one address with
// several transactions inside a single block (plus neighbours) and walks the
// feed with limit 2, resuming from the last returned row's (block, tx_index).
// Every transaction must be seen exactly once, in (block DESC, tx_index DESC)
// order.
func TestGetTransactionsByAddress_KeysetBlockBoundary(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		A   = "0xaaaa000000000000000000000000000000000011"
		oth = "0xbbbb000000000000000000000000000000000012"
	)

	seedBlock := func(n uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size, difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 1, $5, 500, '0', '0', '0x0', '', '', '', '')`,
			n, fmt.Sprintf("0xkbt%061x", n), fmt.Sprintf("0xkbt%061x", n-1), 1_700_000_000+int64(n), oth)
		require.NoError(t, err)
	}
	seedTx := func(block uint64, txIndex int) string {
		hash := fmt.Sprintf("0xktx%03d%056x", txIndex, block)
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $3, $4, $5, $6, 0, 21000, 1, 0, 0, '0x', 1)`,
			hash, block, 1_700_000_000+int64(block), txIndex, A, oth)
		require.NoError(t, err)
		return hash
	}

	// Expected feed order (block DESC, tx_index DESC): block 101 has 4 txs —
	// with limit 2 the page boundary lands mid-block twice.
	seedBlock(100)
	seedBlock(101)
	seedBlock(102)
	var want []string
	want = append(want, seedTx(102, 0))
	for i := 3; i >= 0; i-- {
		want = append(want, seedTx(101, i))
	}
	want = append(want, seedTx(100, 1), seedTx(100, 0))

	const limit = 2
	var got []string
	var bound *AddressFeedBound
	for range [10]int{} { // hard stop — the walk must finish well within this
		page, err := d.GetTransactionsByAddress(ctx, A, limit, bound)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, tx := range page {
			got = append(got, tx.Hash)
		}
		last := page[len(page)-1]
		idx := uint32(last.TxIndex)
		bound = &AddressFeedBound{Block: last.BlockNumber, Index: &idx}
	}
	require.Equal(t, want, got, "keyset walk must return every tx exactly once, in feed order")
}

// TestGetTransfersByAddress_KeysetBlockBoundary mirrors the tx test for the
// transfers feed: several transfers share one block (distinct log_index) and
// the walk resumes from (block, log_index).
func TestGetTransfersByAddress_KeysetBlockBoundary(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		A   = "0xaaaa000000000000000000000000000000000021"
		oth = "0xbbbb000000000000000000000000000000000022"
		tok = "0xcccc000000000000000000000000000000000023"
	)

	seedBlock := func(n uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size, difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 1, $5, 500, '0', '0', '0x0', '', '', '', '')`,
			n, fmt.Sprintf("0xkfb%061x", n), fmt.Sprintf("0xkfb%061x", n-1), 1_700_000_000+int64(n), oth)
		require.NoError(t, err)
	}
	seedBlock(200)
	seedBlock(201)
	seedBlock(202)
	seedTransfer := func(block uint64, logIndex int) string {
		hash := fmt.Sprintf("0xkfr%03d%056x", logIndex, block)
		// token_transfers.tx_hash has an FK to transactions — seed the parent
		// tx first (log_index keeps rows distinct within a block).
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $3, $4, $5, $6, 0, 21000, 1, 0, 0, '0x', 1)`,
			hash, block, 1_700_000_000+int64(block), logIndex, oth, oth)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, `
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, transfer_type, token_type)
			VALUES ($1, $2, $3, $4, $5, 1, $6, $7, 'transfer', 'ERC20')`,
			hash, logIndex, tok, oth, A, block, 1_700_000_000+int64(block))
		require.NoError(t, err)
		return fmt.Sprintf("%s/%d", hash, logIndex)
	}

	// Block 201 has 5 transfers; limit 2 puts two page boundaries inside it.
	var want []string
	want = append(want, seedTransfer(202, 0))
	for i := 4; i >= 0; i-- {
		want = append(want, seedTransfer(201, i))
	}
	want = append(want, seedTransfer(200, 0))

	const limit = 2
	var got []string
	var bound *AddressFeedBound
	for range [10]int{} {
		page, err := d.GetTransfersByAddress(ctx, A, limit, bound)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, tr := range page {
			got = append(got, fmt.Sprintf("%s/%d", tr.TxHash, tr.LogIndex))
		}
		last := page[len(page)-1]
		idx := uint32(last.LogIndex)
		bound = &AddressFeedBound{Block: last.BlockNumber, Index: &idx}
	}
	require.Equal(t, want, got, "keyset walk must return every transfer exactly once, in feed order")
}

// TestAddressFeedBound_InclusiveBlock verifies rowBound's whole-block
// normalization: Inclusive=false bounds by block_number < Block — the proto
// block_range.to_block (exclusive) mapping the handlers pass — while
// Inclusive=true bounds by block_number <= Block (the non-proto inclusive
// convenience). A set Index always wins over the whole-block flag.
func TestAddressFeedBound_InclusiveBlock(t *testing.T) {
	b := AddressFeedBound{Block: 100, Inclusive: true}
	blk, idx := b.rowBound()
	require.Equal(t, uint64(101), blk)
	require.Equal(t, uint32(0), idx)

	b = AddressFeedBound{Block: 100}
	blk, idx = b.rowBound()
	require.Equal(t, uint64(100), blk)
	require.Equal(t, uint32(0), idx)

	i := uint32(7)
	b = AddressFeedBound{Block: 100, Index: &i, Inclusive: true} // Index wins
	blk, idx = b.rowBound()
	require.Equal(t, uint64(100), blk)
	require.Equal(t, uint32(7), idx)
}

// TestGetTransactionsByAddress_TransferOnlyKeysetBoundary guards the transfer-
// only branch of the tx feed (RD-1148 / Copilot review on #27): a transaction
// where the address is only a Transfer party (never the tx from/to) must still
// page correctly when a single block holds more than `limit` such txs. The old
// candidate subquery ordered distinct hashes by MAX(block_number) with a bare
// LIMIT, so intra-block ties were unstable and a block with >limit transfer-only
// txs could drop rows the outer keyset never revisited.
func TestGetTransactionsByAddress_TransferOnlyKeysetBoundary(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		A   = "0xaaaa000000000000000000000000000000000031"
		oth = "0xbbbb000000000000000000000000000000000032"
		tok = "0xcccc000000000000000000000000000000000033"
	)

	seedBlock := func(n uint64) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size, difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 1, $5, 500, '0', '0', '0x0', '', '', '', '')`,
			n, fmt.Sprintf("0xtob%061x", n), fmt.Sprintf("0xtob%061x", n-1), 1_700_000_000+int64(n), oth)
		require.NoError(t, err)
	}
	// A is NOT the tx from/to (oth is) — A appears only as a token-transfer
	// recipient, so the tx can surface only via the transfer-only branch.
	seedTransferOnlyTx := func(block uint64, txIndex int) string {
		hash := fmt.Sprintf("0xtot%03d%056x", txIndex, block)
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $3, $4, $5, $6, 0, 21000, 1, 0, 0, '0x', 1)`,
			hash, block, 1_700_000_000+int64(block), txIndex, oth, oth)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, `
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, transfer_type, token_type)
			VALUES ($1, 0, $2, $3, $4, 1, $5, $6, 'transfer', 'ERC20')`,
			hash, tok, oth, A, block, 1_700_000_000+int64(block))
		require.NoError(t, err)
		return hash
	}

	// Block 301 holds 4 transfer-only txs; with limit 2 the candidate LIMIT
	// falls inside the block — the case the old MAX(block_number) ordering could
	// silently drop.
	seedBlock(300)
	seedBlock(301)
	seedBlock(302)
	var want []string
	want = append(want, seedTransferOnlyTx(302, 0))
	for i := 3; i >= 0; i-- {
		want = append(want, seedTransferOnlyTx(301, i))
	}
	want = append(want, seedTransferOnlyTx(300, 0))

	const limit = 2
	var got []string
	var bound *AddressFeedBound
	for range [10]int{} { // hard stop — the walk must finish well within this
		page, err := d.GetTransactionsByAddress(ctx, A, limit, bound)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, tx := range page {
			got = append(got, tx.Hash)
		}
		last := page[len(page)-1]
		idx := uint32(last.TxIndex)
		bound = &AddressFeedBound{Block: last.BlockNumber, Index: &idx}
	}
	require.Equal(t, want, got, "transfer-only keyset walk must return every tx exactly once, in feed order")
}
