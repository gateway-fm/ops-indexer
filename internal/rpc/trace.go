package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	explorerTypes "github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
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

// TraceBlockResult is one transaction's entry in a block trace. A node reports a transaction whose own trace failed
// with an error and no result.
type TraceBlockResult struct {
	TxHash string     `json:"txHash"`
	Result *CallFrame `json:"result"`
	Error  string     `json:"error"`
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
		if TracingAbsent(err) {
			return false, nil
		}
		// Anything else is transient: report it instead of assuming support.
		return false, fmt.Errorf("tracing support check: debug_traceBlockByNumber on block %d: %w", blockNumber, err)
	}
	return true, nil
}

// missingStateSignals are the error substrings of a node that traces but lacks the block's state, such as
// "historical state is not available", which methodAbsentSignals' "not available" would otherwise take for no tracing.
var missingStateSignals = []string{
	"state",
	"missing trie node",
}

// ErrNotTraced is the node answering a trace without one for a transaction: no entry for it, a null trace, or an
// error entry other than a timeout.
var ErrNotTraced = errors.New("the node returned no trace")

// NodeRefused is whether err is the node answering that it cannot serve what was asked: no trace for a transaction,
// or an error saying it lacks the block's state. A timeout, a rate limit or a node that does not answer is not.
func NodeRefused(err error) bool {
	if errors.Is(err, ErrNotTraced) {
		return true
	}
	var answer *rpclient.RPCError
	if !errors.As(err, &answer) {
		return false
	}
	msg := strings.ToLower(answer.Message)
	for _, signal := range missingStateSignals {
		if strings.Contains(msg, signal) {
			return true
		}
	}
	return false
}

// noTrace is the error for a transaction a block's traces answered without a trace; msg is its error entry.
func noTrace(blockNumber uint64, txHash common.Hash, msg string) error {
	if strings.Contains(strings.ToLower(msg), "timeout") {
		return fmt.Errorf("block %d: trace of transaction %s timed out: %q", blockNumber, txHash.Hex(), msg)
	}
	return fmt.Errorf("block %d: transaction %s: %w: %q", blockNumber, txHash.Hex(), ErrNotTraced, msg)
}

// TracingAbsent is whether a trace call's error says the node has no debug_traceBlockByNumber at all.
func TracingAbsent(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, signal := range missingStateSignals {
		if strings.Contains(msg, signal) {
			return false
		}
	}
	for _, signal := range methodAbsentSignals {
		if strings.Contains(msg, signal) {
			return true
		}
	}
	return false
}

func (c *Client) TraceBlock(ctx context.Context, blockNumber uint64) (map[string]TraceBlockResult, error) {
	blockHex := fmt.Sprintf("0x%x", blockNumber)

	var results []TraceBlockResult

	err := c.raw.CallContext(ctx, &results, "debug_traceBlockByNumber", blockHex, &TraceConfig{Tracer: "callTracer"})
	if err != nil {
		return nil, fmt.Errorf("debug_traceBlockByNumber failed: %w", err)
	}

	traces := make(map[string]TraceBlockResult)
	for _, r := range results {
		traces[r.TxHash] = r
	}

	return traces, nil
}

// TraceTransaction fails on a null or empty result: a node that answers so has no trace of the transaction.
func (c *Client) TraceTransaction(ctx context.Context, txHash common.Hash) (*CallFrame, error) {
	var result *CallFrame

	err := c.raw.CallContext(ctx, &result, "debug_traceTransaction", txHash.Hex(), &TraceConfig{Tracer: "callTracer"})
	if err != nil {
		return nil, fmt.Errorf("debug_traceTransaction failed: %w", err)
	}
	if !traced(result) {
		return nil, fmt.Errorf("debug_traceTransaction of %s: %w", txHash.Hex(), ErrNotTraced)
	}

	return result, nil
}

// traced is whether a call tracer's frame is a trace: every root frame has a call type.
func traced(frame *CallFrame) bool {
	return frame != nil && frame.Type != ""
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

// traceMode is what a block's trace fetch does with a transaction it gets no trace for.
type traceMode int

const (
	// skipFailures leaves the transaction out, as ingest does.
	skipFailures traceMode = iota
	// failOnAny fails the fetch.
	failOnAny
	// skipRefusals leaves it out when the node answered that it cannot trace it, and fails the fetch otherwise.
	skipRefusals
)

func (c *Client) FetchTracesBatch(ctx context.Context, txHashes []common.Hash, blockNumber uint64, timestamp uint64, workers int, rateLimit int) ([]*explorerTypes.InternalTransaction, error) {
	return c.fetchTraces(ctx, txHashes, blockNumber, timestamp, workers, rateLimit, skipFailures)
}

// FetchTracesBatchStrict is FetchTracesBatch for counts that replace or extend the ledger: a transaction whose trace
// is missing from the block's traces, or whose own trace fails, fails the batch instead of being left out.
func (c *Client) FetchTracesBatchStrict(ctx context.Context, txHashes []common.Hash, blockNumber uint64, timestamp uint64, workers int, rateLimit int) ([]*explorerTypes.InternalTransaction, error) {
	return c.fetchTraces(ctx, txHashes, blockNumber, timestamp, workers, rateLimit, failOnAny)
}

// FetchTracesBatchSkippingRefusals is FetchTracesBatch for a count that goes on without the traces the node cannot
// serve: a transaction the node answers it cannot trace is left out, and any other failure fails the batch.
func (c *Client) FetchTracesBatchSkippingRefusals(ctx context.Context, txHashes []common.Hash, blockNumber uint64, timestamp uint64, workers int, rateLimit int) ([]*explorerTypes.InternalTransaction, error) {
	return c.fetchTraces(ctx, txHashes, blockNumber, timestamp, workers, rateLimit, skipRefusals)
}

// fails is whether a transaction's missing trace, err, fails the fetch.
func (m traceMode) fails(err error) bool {
	return m == failOnAny || m == skipRefusals && !NodeRefused(err)
}

func (c *Client) fetchTraces(ctx context.Context, txHashes []common.Hash, blockNumber uint64, timestamp uint64, workers int, rateLimit int, mode traceMode) ([]*explorerTypes.InternalTransaction, error) {
	if len(txHashes) == 0 {
		return nil, nil
	}

	blockTraces, err := c.TraceBlock(ctx, blockNumber)
	if err == nil && len(blockTraces) > 0 {
		var allInternalTxs []*explorerTypes.InternalTransaction
		ts := timestamp

		for _, txHash := range txHashes {
			entry, ok := blockTraces[txHash.Hex()]
			if !ok || entry.Error != "" || !traced(entry.Result) {
				if err := noTrace(blockNumber, txHash, entry.Error); mode.fails(err) {
					return nil, err
				}
				continue
			}
			internalTxs := FlattenCallFrame(entry.Result, txHash.Hex(), blockNumber, &ts)
			if len(internalTxs) > 1 { // skip root call
				allInternalTxs = append(allInternalTxs, internalTxs[1:]...)
			}
		}

		metrics.SetTraceStrategy("batch")
		return allInternalTxs, nil
	}

	metrics.SetTraceStrategy("per_tx")
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
				if mode.fails(err) {
					return err
				}
				return nil // Don't fail the whole batch
			}

			frame, err := c.TraceTransaction(ctx, hash)
			if err != nil {
				if mode.fails(err) {
					return fmt.Errorf("trace of transaction %s: %w", hash.Hex(), err)
				}
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

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
