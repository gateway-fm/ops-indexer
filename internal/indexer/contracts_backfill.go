package indexer

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// ContractsBackfillDB is what the contracts backfill needs from the database.
type ContractsBackfillDB interface {
	BackfillContractsFromInternalTransactions(ctx context.Context) (int64, error)
	ExistingContracts(ctx context.Context, addresses []string) (map[string]bool, error)
	InsertContract(ctx context.Context, c *types.Contract) error
}

// ContractsBackfillNode is what the contracts backfill needs from the node.
type ContractsBackfillNode interface {
	TraceTransaction(ctx context.Context, txHash common.Hash) (*rpc.CallFrame, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*rpclient.Receipt, error)
	GetCode(ctx context.Context, address common.Address) ([]byte, error)
}

// Creation is one listed internal creation: a contract and the transaction that deployed it.
type Creation struct {
	Address string
	TxHash  string
	Block   uint64
}

// ReadCreationsCSV reads address,creation_tx_hash,block_number rows, with or without that header row.
func ReadCreationsCSV(r io.Reader) ([]Creation, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	first := 0
	if len(rows) > 0 && len(rows[0]) > 0 && strings.TrimSpace(rows[0][0]) == "address" {
		first = 1
	}
	out := make([]Creation, 0, len(rows)-first)
	for i, row := range rows[first:] {
		if len(row) != 3 || !strings.HasPrefix(row[0], "0x") || !strings.HasPrefix(row[1], "0x") {
			return nil, fmt.Errorf("row %d: want address,creation_tx_hash,block_number", i+first+1)
		}
		block, err := strconv.ParseUint(strings.TrimSpace(row[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: block_number: %w", i+first+1, err)
		}
		out = append(out, Creation{Address: strings.ToLower(strings.TrimSpace(row[0])), TxHash: strings.ToLower(strings.TrimSpace(row[1])), Block: block})
	}
	return out, nil
}

type ContractsBackfillReport struct {
	FromInternalTransactions int64
	TxsTraced, TxsSkipped    int
	// Written counts rows sent with ON CONFLICT DO NOTHING; a row another writer added meanwhile stays as it was.
	Written int
	// NotCreated are listed contracts the node's trace of the listed transaction did not create.
	NotCreated []string
	// Unlisted are contracts the traces created that the list did not name; they are written too.
	Unlisted []string
	Failed   map[string]error
}

// BackfillContracts writes the contracts rows that contracts deployed by other contracts were missing (PRST-5426),
// exactly as ingest writes them for new blocks: first from the internal creation frames still in the database,
// then, for the listed creations, by tracing each listed transaction once on the node. The list only says where to
// look; every row comes from the node's trace. A transaction whose listed contracts all exist is skipped, so a
// rerun resumes, and existing rows are never changed. Node calls are limited to perSecond.
func BackfillContracts(ctx context.Context, d ContractsBackfillDB, node ContractsBackfillNode, listed []Creation, perSecond float64) (*ContractsBackfillReport, error) {
	report := &ContractsBackfillReport{Failed: map[string]error{}}
	n, err := d.BackfillContractsFromInternalTransactions(ctx)
	if err != nil {
		return report, fmt.Errorf("from internal_transactions: %w", err)
	}
	report.FromInternalTransactions = n
	log.Info("contracts backfill: written from internal_transactions", "rows", n)

	byTx := map[string][]Creation{}
	for _, c := range listed {
		byTx[c.TxHash] = append(byTx[c.TxHash], c)
	}
	txs := make([]string, 0, len(byTx))
	for tx := range byTx {
		txs = append(txs, tx)
	}
	sort.Slice(txs, func(i, j int) bool {
		a, b := byTx[txs[i]][0].Block, byTx[txs[j]][0].Block
		return a < b || a == b && txs[i] < txs[j]
	})

	limiter := rate.NewLimiter(rate.Limit(perSecond), 1)
	getCode := func(ctx context.Context, a common.Address) ([]byte, error) {
		if err := limiter.Wait(ctx); err != nil {
			return nil, err
		}
		return node.GetCode(ctx, a)
	}
	for i, tx := range txs {
		want := byTx[tx]
		addresses := make([]string, len(want))
		for k, c := range want {
			addresses[k] = c.Address
		}
		have, err := d.ExistingContracts(ctx, addresses)
		if err != nil {
			return report, err
		}
		if len(have) == len(addresses) {
			report.TxsSkipped++
			continue
		}
		created, err := tracedCreations(ctx, node, limiter, getCode, tx)
		if err != nil {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			report.Failed[tx] = err
			log.Warn("contracts backfill: tracing failed; rerun to retry", "tx", tx, "error", err)
			continue
		}
		report.TxsTraced++
		made := map[string]bool{}
		for _, c := range created {
			made[c.Address] = true
			if have[c.Address] {
				continue
			}
			if err := d.InsertContract(ctx, c); err != nil {
				return report, err
			}
			report.Written++
		}
		for _, c := range want {
			if !made[c.Address] {
				report.NotCreated = append(report.NotCreated, c.Address)
			}
			delete(made, c.Address)
		}
		for a := range made {
			report.Unlisted = append(report.Unlisted, a)
		}
		if (i+1)%100 == 0 {
			log.Info("contracts backfill: progress", "txs", i+1, "of", len(txs), "written", report.Written)
		}
	}
	return report, nil
}

func tracedCreations(ctx context.Context, node ContractsBackfillNode, limiter *rate.Limiter,
	getCode func(context.Context, common.Address) ([]byte, error), tx string) ([]*types.Contract, error) {
	hash := common.HexToHash(tx)
	if err := limiter.Wait(ctx); err != nil {
		return nil, err
	}
	receipt, err := node.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("receipt: %w", err)
	}
	if receipt == nil || receipt.BlockNumber == nil {
		return nil, fmt.Errorf("the node has no receipt")
	}
	if err := limiter.Wait(ctx); err != nil {
		return nil, err
	}
	frame, err := node.TraceTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	block := receipt.BlockNumber.Uint64()
	var out []*types.Contract
	for _, it := range internalCreations(rpc.FlattenCallFrame(frame, hash.Hex(), block, nil)) {
		if c := contractFromCreation(ctx, getCode, it, block); c != nil {
			out = append(out, c)
		}
	}
	return out, nil
}
