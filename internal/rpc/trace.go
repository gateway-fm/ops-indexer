package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/gateway-fm/chain-indexer/internal/log"
	explorerTypes "github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type CallFrame struct {
	Type    string       `json:"type"`
	From    string       `json:"from"`
	To      string       `json:"to,omitempty"`
	Value   *hexutil.Big `json:"value,omitempty"`
	Gas     hexutil.Uint64 `json:"gas,omitempty"`
	GasUsed hexutil.Uint64 `json:"gasUsed,omitempty"`
	Input   string       `json:"input,omitempty"`
	Output  string       `json:"output,omitempty"`
	Error   string       `json:"error,omitempty"`
	Calls   []CallFrame  `json:"calls,omitempty"`
}

type TraceConfig struct {
	Tracer string `json:"tracer"`
}

type TraceBlockResult struct {
	TxHash string    `json:"txHash"`
	Result CallFrame `json:"result"`
}

// methodAbsentSignals are the error substrings that mean the node genuinely
// lacks debug_traceBlockByNumber, as opposed to a transient failure.
var methodAbsentSignals = []string{
	"method not found",
	"not supported",
	"does not exist",
	"not available",
	"unsupported method",
}

func (c *Client) CheckTracingSupport(ctx context.Context) (bool, error) {
	// Probe a recent block, not genesis: many nodes cannot trace block 0
	// (Infura returns "genesis is not traceable"), which makes the probe
	// ambiguous even where tracing works on the blocks the indexer reads.
	blockNumber, err := c.BlockNumber(ctx)
	if err != nil {
		return false, fmt.Errorf("tracing support check: block number: %w", err)
	}
	if blockNumber > 3 {
		blockNumber -= 3
	}

	var result json.RawMessage
	err = c.raw.CallContext(ctx, &result, "debug_traceBlockByNumber", fmt.Sprintf("0x%x", blockNumber), &TraceConfig{Tracer: "callTracer"})
	if err != nil {
		msg := strings.ToLower(err.Error())
		for _, signal := range methodAbsentSignals {
			if strings.Contains(msg, signal) {
				return false, nil
			}
		}
		// Anything else is transient: report it instead of assuming support.
		return false, fmt.Errorf("tracing support check: debug_traceBlockByNumber on block %d: %w", blockNumber, err)
	}
	return true, nil
}

func (c *Client) TraceBlock(ctx context.Context, blockNumber uint64) (map[string]*CallFrame, error) {
	blockHex := fmt.Sprintf("0x%x", blockNumber)

	var results []struct {
		TxHash string    `json:"txHash"`
		Result CallFrame `json:"result"`
	}

	err := c.raw.CallContext(ctx, &results, "debug_traceBlockByNumber", blockHex, &TraceConfig{Tracer: "callTracer"})
	if err != nil {
		return nil, fmt.Errorf("debug_traceBlockByNumber failed: %w", err)
	}

	traces := make(map[string]*CallFrame)
	for _, r := range results {
		result := r.Result // Copy to avoid pointer issues
		traces[r.TxHash] = &result
	}

	return traces, nil
}

func (c *Client) TraceTransaction(ctx context.Context, txHash common.Hash) (*CallFrame, error) {
	var result CallFrame

	err := c.raw.CallContext(ctx, &result, "debug_traceTransaction", txHash.Hex(), &TraceConfig{Tracer: "callTracer"})
	if err != nil {
		return nil, fmt.Errorf("debug_traceTransaction failed: %w", err)
	}

	return &result, nil
}

func FlattenCallFrame(frame *CallFrame, txHash string, blockNumber uint64, timestamp *uint64) []*explorerTypes.InternalTransaction {
	var result []*explorerTypes.InternalTransaction
	flattenCallFrameRecursive(frame, txHash, blockNumber, timestamp, []int{}, &result)
	return result
}

func flattenCallFrameRecursive(frame *CallFrame, txHash string, blockNumber uint64, timestamp *uint64, path []int, result *[]*explorerTypes.InternalTransaction) {
	traceAddr := ""
	if len(path) > 0 {
		parts := make([]string, len(path))
		for i, p := range path {
			parts[i] = strconv.Itoa(p)
		}
		traceAddr = strings.Join(parts, ",")
	}

	value := "0"
	if frame.Value != nil {
		value = (*big.Int)(frame.Value).String()
	}

	callType := strings.ToLower(frame.Type)
	switch callType {
	case "call", "delegatecall", "staticcall", "create", "create2":
	default:
		callType = "call"
	}

	var to *string
	if frame.To != "" {
		toAddr := frame.To
		to = &toAddr
	}

	var gas, gasUsed *uint64
	if frame.Gas > 0 {
		g := uint64(frame.Gas)
		gas = &g
	}
	if frame.GasUsed > 0 {
		gu := uint64(frame.GasUsed)
		gasUsed = &gu
	}

	var input, output, errStr *string
	if frame.Input != "" && frame.Input != "0x" {
		input = &frame.Input
	}
	if frame.Output != "" && frame.Output != "0x" {
		output = &frame.Output
	}
	if frame.Error != "" {
		errStr = &frame.Error
	}

	internalTx := &explorerTypes.InternalTransaction{
		TxHash:       txHash,
		BlockNumber:  blockNumber,
		TraceAddress: traceAddr,
		From:         frame.From,
		To:           to,
		Value:        explorerTypes.JSONString(value),
		Gas:          gas,
		GasUsed:      gasUsed,
		Input:        input,
		Output:       output,
		CallType:     callType,
		Error:        errStr,
		Timestamp:    timestamp,
	}

	*result = append(*result, internalTx)

	for i, child := range frame.Calls {
		childPath := append(append([]int{}, path...), i)
		flattenCallFrameRecursive(&child, txHash, blockNumber, timestamp, childPath, result)
	}
}

type TraceResult struct {
	TxHash       string
	InternalTxs  []*explorerTypes.InternalTransaction
	Err          error
}

func (c *Client) FetchTracesBatch(ctx context.Context, txHashes []common.Hash, blockNumber uint64, timestamp uint64, workers int, rateLimit int) ([]*explorerTypes.InternalTransaction, error) {
	if len(txHashes) == 0 {
		return nil, nil
	}

	blockTraces, err := c.TraceBlock(ctx, blockNumber)
	if err == nil && len(blockTraces) > 0 {
		var allInternalTxs []*explorerTypes.InternalTransaction
		ts := timestamp

		for _, txHash := range txHashes {
			frame, ok := blockTraces[txHash.Hex()]
			if !ok {
				continue
			}
			internalTxs := FlattenCallFrame(frame, txHash.Hex(), blockNumber, &ts)
			if len(internalTxs) > 1 { // skip root call
				allInternalTxs = append(allInternalTxs, internalTxs[1:]...)
			}
		}

		return allInternalTxs, nil
	}

	log.Warn("block tracing failed, falling back to per-tx tracing", "block", blockNumber, "error", err)

	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit/10+1)
	results := make([]*explorerTypes.InternalTransaction, 0)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	ts := timestamp

	for _, hash := range txHashes {
		hash := hash
		g.Go(func() error {
			if err := limiter.Wait(ctx); err != nil {
				return nil // Don't fail the whole batch
			}

			frame, err := c.TraceTransaction(ctx, hash)
			if err != nil {
				log.Warn("failed to trace tx", "tx", hash.Hex(), "error", err)
				return nil
			}

			internalTxs := FlattenCallFrame(frame, hash.Hex(), blockNumber, &ts)
			if len(internalTxs) > 1 {
				mu.Lock()
				results = append(results, internalTxs[1:]...)
				mu.Unlock()
			}

			return nil
		})
	}

	g.Wait()
	return results, nil
}
