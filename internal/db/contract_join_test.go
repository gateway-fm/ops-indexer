package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A transaction's contract_address is the contract it deployed itself (to IS NULL), not one a factory it called
// deployed: those are internal creations, shown with the internal transactions, as Blockscout does.
func TestTransactionContractAddressIsOnlyATopLevelCreation(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	factory, child, deployed := "0x00000000000000000000000000000000000fac70", "0x0000000000000000000000000000000000000c0a", "0x0000000000000000000000000000000000000c01"
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Block: &types.Block{Number: 1, Hash: "0xb1", Timestamp: 1_700_000_001},
		Transactions: []*types.Transaction{
			{Hash: "0xcreate", BlockNumber: 1, TxIndex: 0, From: "0xa1", Value: "0", Status: 1},
			{Hash: "0xviafactory", BlockNumber: 1, TxIndex: 1, From: "0xa1", To: &factory, Value: "0", Status: 1},
		},
		Contracts: []*types.Contract{
			{Address: deployed, Bytecode: "6080", Creator: "0xa1", CreationTx: "0xcreate", BlockNumber: 1},
			{Address: child, Bytecode: "6080", Creator: factory, CreationTx: "0xviafactory", BlockNumber: 1},
		},
	}))
	created, err := d.GetTransactionWithCategories(ctx, "0xcreate")
	require.NoError(t, err)
	require.NotNil(t, created.ContractAddress)
	require.Equal(t, deployed, *created.ContractAddress)

	viaFactory, err := d.GetTransactionWithCategories(ctx, "0xviafactory")
	require.NoError(t, err)
	require.Nil(t, viaFactory.ContractAddress)
	var contracts int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM contracts`).Scan(&contracts))
	require.Equal(t, 2, contracts, "the factory's child is a contract")
}

// A reorg that reverts a block removes the contracts it deployed, at the top level or from a factory.
func TestRevertedBlockTakesItsContracts(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	factory := "0x00000000000000000000000000000000000fac70"
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Block:        &types.Block{Number: 7, Hash: "0xb7", Timestamp: 1_700_000_007},
		Transactions: []*types.Transaction{{Hash: "0xviafactory7", BlockNumber: 7, From: "0xa1", To: &factory, Value: "0", Status: 1}},
		Contracts:    []*types.Contract{{Address: "0x0000000000000000000000000000000000000c07", Bytecode: "6080", Creator: factory, CreationTx: "0xviafactory7", BlockNumber: 7}},
	}))
	require.NoError(t, d.DeleteBlock(ctx, 7))
	var contracts int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM contracts`).Scan(&contracts))
	require.Zero(t, contracts)
}
