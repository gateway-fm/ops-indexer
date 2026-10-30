package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func internalFrame(tx string, block uint64, path, callType, from, to, output, errMsg string) *types.InternalTransaction {
	f := &types.InternalTransaction{TxHash: tx, BlockNumber: block, TraceAddress: path, CallType: callType, From: from, Value: "0"}
	if to != "" {
		f.To = &to
	}
	if output != "" {
		f.Output = &output
	}
	if errMsg != "" {
		f.Error = &errMsg
	}
	return f
}

func contractRows(t *testing.T, d *DB) map[string][4]string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT address, bytecode, COALESCE(bytecode_hash, ''), creator, creation_tx FROM contracts`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string][4]string{}
	for rows.Next() {
		var a, code, hash, creator, tx string
		require.NoError(t, rows.Scan(&a, &code, &hash, &creator, &tx))
		out[a] = [4]string{code, hash, creator, tx}
	}
	return out
}

// The retained range is backfilled from internal_transactions by the rule ingest uses: only creations that took
// effect, rows identical to what ingest writes (indexer.contractFromCreation pins the same values), and a rerun
// changes nothing.
func TestContractsBackfillFromInternalTransactions(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	factory := "0x00000000000000000000000000000000000FAC70"
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Block: &types.Block{Number: 5, Hash: "0xb5", Timestamp: 1_700_000_005},
		Transactions: []*types.Transaction{
			{Hash: "0xt1", BlockNumber: 5, TxIndex: 0, From: "0xa1", To: &factory, Value: "0", Status: 1},
			{Hash: "0xt2", BlockNumber: 5, TxIndex: 1, From: "0xa1", To: &factory, Value: "0", Status: 0},
		},
		InternalTransactions: []*types.InternalTransaction{
			internalFrame("0xt1", 5, "", "call", "0xa1", factory, "", ""),
			internalFrame("0xt1", 5, "0", "create2", factory, "0x0000000000000000000000000000000000000C0A", "0x6080aa", ""),
			internalFrame("0xt1", 5, "1", "call", factory, "0xb2", "", "execution reverted"),
			internalFrame("0xt1", 5, "1,0", "create", "0xb2", "0x0000000000000000000000000000000000000c0b", "0x6080bb", ""),
			internalFrame("0xt1", 5, "2", "create", factory, "0x0000000000000000000000000000000000000c0c", "", "out of gas"),
			internalFrame("0xt1", 5, "10", "create", factory, "0x0000000000000000000000000000000000000c10", "0x6080cc", ""),
			internalFrame("0xt2", 5, "", "call", "0xa1", factory, "", "execution reverted"),
			internalFrame("0xt2", 5, "0", "create2", factory, "0x0000000000000000000000000000000000000c0e", "0x6080ee", ""),
		},
	}))

	n, err := d.BackfillContractsFromInternalTransactions(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, n, "c0a, and c10 (not under the reverted frame 1)")
	creator := "0x00000000000000000000000000000000000fac70"
	require.Equal(t, map[string][4]string{
		"0x0000000000000000000000000000000000000c0a": {"6080aa", "0x00000000000000000000000000000000000000000000000000000000006080aa", creator, "0xt1"},
		"0x0000000000000000000000000000000000000c10": {"6080cc", "0x00000000000000000000000000000000000000000000000000000000006080cc", creator, "0xt1"},
	}, contractRows(t, d))

	n, err = d.BackfillContractsFromInternalTransactions(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "a rerun writes nothing")

	have, err := d.ExistingContracts(ctx, []string{"0x0000000000000000000000000000000000000c0a", "0x0000000000000000000000000000000000000c0b"})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"0x0000000000000000000000000000000000000c0a": true}, have)
}

// contracts rows are current state: dropping the history range a contract was created in keeps its row.
func TestContractsSurviveAPartitionDrop(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	require.NoError(t, d.InsertContract(ctx, &types.Contract{
		Address: "0x0000000000000000000000000000000000000c05", Bytecode: "6080", Creator: "0xfac70", CreationTx: "tx5", BlockNumber: 5}))
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	attached, _ := legacyBlocksPartition(t, d)
	require.False(t, attached, "the range holding block 5 is gone")
	require.Contains(t, contractRows(t, d), "0x0000000000000000000000000000000000000c05")
}
