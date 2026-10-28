package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

type revertTx struct {
	from      string
	to        string
	transfers [][2]string
	internal  [][2]string
	creates   string
}

// revertTestBlock builds a block and the address deltas the indexer would send with it: every address a
// transaction touches (sender, recipient, transfer parties) counts the transaction once, each transfer and internal
// call counts once per side, a self-transfer once, and a created contract is marked.
func revertTestBlock(number uint64, hash string, txs []revertTx) *BlockData {
	ts := uint64(1_700_000_000) + number
	bd := &BlockData{
		Block:        &types.Block{Number: number, Hash: hash, ParentHash: "parent", Timestamp: ts},
		AddressStats: map[string]*AddressStatsDelta{},
	}
	delta := func(address string) *AddressStatsDelta {
		d, ok := bd.AddressStats[address]
		if !ok {
			d = &AddressStatsDelta{Address: address, BlockNumber: number}
			bd.AddressStats[address] = d
		}
		return d
	}
	for i, tx := range txs {
		txHash := hash + "-tx" + string(rune('a'+i))
		t := &types.Transaction{Hash: txHash, BlockNumber: number, From: tx.from, Value: "0", TxType: 2, Status: 1, TxIndex: i}
		touched := map[string]bool{tx.from: true}
		if tx.to != "" {
			to := tx.to
			t.To = &to
			touched[tx.to] = true
		}
		bd.Transactions = append(bd.Transactions, t)
		for j, tr := range tx.transfers {
			bd.Transfers = append(bd.Transfers, &types.TokenTransfer{TxHash: txHash, LogIndex: i*10 + j, TokenAddress: tx.to,
				From: tr[0], To: tr[1], Value: "1", BlockNumber: number, Timestamp: &ts, TokenType: types.TokenTypeERC20})
			delta(tr[0]).TokenTransferDelta++
			touched[tr[0]] = true
			if tr[1] != tr[0] {
				delta(tr[1]).TokenTransferDelta++
				touched[tr[1]] = true
			}
		}
		for j, it := range tx.internal {
			to := it[1]
			bd.InternalTransactions = append(bd.InternalTransactions, &types.InternalTransaction{TxHash: txHash, BlockNumber: number,
				TraceAddress: string(rune('0' + j)), From: it[0], To: &to, Value: "0", CallType: "call"})
			delta(it[0]).InternalTxCountDelta++
			if it[1] != it[0] {
				delta(it[1]).InternalTxCountDelta++
			}
		}
		for address := range touched {
			delta(address).TxCountDelta++
		}
		if tx.creates != "" {
			bd.Contracts = append(bd.Contracts, &types.Contract{Address: tx.creates, Creator: tx.from, CreationTx: txHash, BlockNumber: number})
			delta(tx.creates).IsContract = true
		}
	}
	return bd
}

type addressRow struct {
	txs, internal, transfers int64
	firstSeen, lastSeen      int64
}

func addressStatsRows(t *testing.T, d *DB) map[string]addressRow {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen FROM address_stats`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]addressRow{}
	for rows.Next() {
		var a string
		var r addressRow
		require.NoError(t, rows.Scan(&a, &r.txs, &r.internal, &r.transfers, &r.firstSeen, &r.lastSeen))
		out[a] = r
	}
	require.NoError(t, rows.Err())
	return out
}

func recountFromRows(t *testing.T, d *DB) map[string]addressRow {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), ledgerAddressRowsSQL, int64(0), int64(1)<<62)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]addressRow{}
	for rows.Next() {
		var a string
		var r addressRow
		require.NoError(t, rows.Scan(&a, &r.txs, &r.internal, &r.transfers, &r.firstSeen, &r.lastSeen))
		out[a] = r
	}
	require.NoError(t, rows.Err())
	return out
}

func TestReorgLeavesAddressStatsEqualToARecountOfTheRows(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	stays := revertTestBlock(9, "b9", []revertTx{{from: "0xa", to: "0xb"}})
	old := []*BlockData{
		revertTestBlock(10, "old10", []revertTx{{from: "0xa", to: "0x7c", transfers: [][2]string{{"0xa", "0xd"}, {"0xd", "0xd"}}, internal: [][2]string{{"0xa", "0xf"}}}}),
		revertTestBlock(11, "old11", []revertTx{{from: "0xc", to: "0xa"}, {from: "0xb", to: "0xb"}}),
		revertTestBlock(12, "old12", []revertTx{{from: "0xe", creates: "0x5c"}, {from: "0xa", to: "0x5c"}}),
	}
	replacement := []*BlockData{
		revertTestBlock(10, "new10", []revertTx{{from: "0xa", to: "0xb"}}),
		revertTestBlock(11, "new11", []revertTx{{from: "0x9", to: "0xa", transfers: [][2]string{{"0x9", "0xa"}}}}),
		revertTestBlock(12, "new12", []revertTx{{from: "0x8", to: "0x5c"}}),
	}

	require.NoError(t, d.InsertBlockDataBatch(ctx, stays))
	for _, b := range old {
		require.NoError(t, d.InsertBlockDataBatch(ctx, b))
	}
	require.Equal(t, recountFromRows(t, d), addressStatsRows(t, d), "the deltas these blocks carry are the ones the rows define")

	for n := uint64(12); n >= 10; n-- {
		require.NoError(t, d.DeleteBlock(ctx, n))
	}
	require.Equal(t, recountFromRows(t, d), addressStatsRows(t, d), "after the revert, address_stats is the recount of the remaining rows")

	for _, b := range replacement {
		require.NoError(t, d.InsertBlockDataBatch(ctx, b))
	}
	require.Equal(t, recountFromRows(t, d), addressStatsRows(t, d), "after the replacement chain, address_stats is the recount of the rows")

	for _, address := range []string{"0xa", "0xd", "0x9"} {
		total, err := d.CountTransfersByAddress(ctx, address)
		require.NoError(t, err)
		var rows int64
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT count(*) FROM token_transfers WHERE from_address = $1 OR to_address = $1`, address).Scan(&rows))
		require.Equal(t, rows, total, "the transfer total of %s is its row count", address)
	}

	var isContract bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT is_contract FROM address_stats WHERE address = '0x5c'`).Scan(&isContract))
	require.False(t, isContract, "the contract's creation was reverted; on the new chain it is a plain recipient")

	var counted, stored int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats)`).
		Scan(&counted, &stored))
	require.Equal(t, stored, counted, "addresses_total counts the address_stats rows")
}
