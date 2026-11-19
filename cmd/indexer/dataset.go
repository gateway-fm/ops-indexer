package main

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

type datasetIdentityStore interface {
	EnsureDatasetIdentity(context.Context, string, []byte) error
}

func initializeGenesis(ctx context.Context, store interface {
	EnsureInitialGenesis(context.Context, *types.Block) error
}, node interface {
	RawBlockByNumber(context.Context, uint64) (*rpc.RawBlock, error)
}) error {
	b, err := node.RawBlockByNumber(ctx, 0)
	if err != nil {
		return err
	}
	if b == nil || b.Number == nil || b.Number.ToInt().Sign() != 0 || len(b.Transactions) != 0 {
		return errors.New("configured node returned an invalid or nonempty genesis")
	}
	return store.EnsureInitialGenesis(ctx, &types.Block{Number: 0, Hash: b.Hash.Hex(), ParentHash: b.ParentHash.Hex(), Timestamp: uint64(b.Timestamp), GasUsed: uint64(b.GasUsed), GasLimit: uint64(b.GasLimit), BaseFeePerGas: b.BaseFeeU64(), TransactionCount: 0, Size: uint64(b.Size), Difficulty: b.DifficultyString(), TotalDifficulty: b.TotalDifficultyString(), Nonce: b.NonceHex(), Miner: b.Miner.Hex(), ExtraData: hex.EncodeToString(b.ExtraData), StateRoot: b.StateRoot.Hex(), TransactionsRoot: b.TransactionsRoot.Hex(), ReceiptsRoot: b.ReceiptsRoot.Hex()})
}

type datasetIdentityNode interface {
	ChainID(context.Context) (*big.Int, error)
	RawBlockHash(context.Context, uint64) (string, error)
}

func initializeDatasetIdentity(ctx context.Context, store datasetIdentityStore, node datasetIdentityNode) error {
	chainID, err := node.ChainID(ctx)
	if err != nil {
		return err
	}
	if chainID == nil || chainID.Sign() < 0 || chainID.BitLen() > 256 {
		return errors.New("invalid configured-node chain ID")
	}
	hash, err := node.RawBlockHash(ctx, 0)
	if err != nil {
		return err
	}
	genesis, err := hex.DecodeString(strings.TrimPrefix(hash, "0x"))
	if err != nil || len(genesis) != 32 {
		return errors.New("invalid configured-node genesis hash")
	}
	return store.EnsureDatasetIdentity(ctx, chainID.String(), genesis)
}
