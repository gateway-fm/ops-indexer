package indexer

import (
	"context"
	"errors"
	"math/big"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// limitedRPC holds every call a background job makes to the node to the job's budget: each call waits for a token
// of the job's limiter, and a batch call, which can fall back to one request per transaction or token, runs that
// fallback at no more than perCall requests a second, the budget divided among the job's concurrent callers.
type limitedRPC struct {
	inner   RPCClient
	limiter *rate.Limiter
	perCall int
}

// errNotSupported: the node client behind the limit does not offer the call.
var errNotSupported = errors.New("not supported by the node client")

func newLimitedRPC(inner RPCClient, limiter *rate.Limiter, rps, callers int) *limitedRPC {
	return &limitedRPC{inner: inner, limiter: limiter, perCall: max(rps/max(callers, 1), 1)}
}

func (l *limitedRPC) rate(r int) int { return max(min(r, l.perCall), 1) }

func (l *limitedRPC) BlockNumber(ctx context.Context) (uint64, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	return l.inner.BlockNumber(ctx)
}

func (l *limitedRPC) RawBlockByNumber(ctx context.Context, number uint64) (*rpc.RawBlock, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.RawBlockByNumber(ctx, number)
}

func (l *limitedRPC) RawBlockHash(ctx context.Context, number uint64) (string, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return "", err
	}
	return l.inner.RawBlockHash(ctx, number)
}

func (l *limitedRPC) FetchReceiptsBatch(ctx context.Context, txHashes []common.Hash, workers int, rateLimit int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.FetchReceiptsBatch(ctx, txHashes, workers, l.rate(rateLimit), blockNumber...)
}

func (l *limitedRPC) GetTotalDifficulty(ctx context.Context, blockNumber uint64) string {
	if err := l.limiter.Wait(ctx); err != nil {
		return ""
	}
	return l.inner.GetTotalDifficulty(ctx, blockNumber)
}

func (l *limitedRPC) CheckTracingSupport(ctx context.Context) (bool, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return false, err
	}
	return l.inner.CheckTracingSupport(ctx)
}

func (l *limitedRPC) SubscribeNewHead(ctx context.Context, ch chan<- *rpclient.Header) (rpclient.Subscription, error) {
	return l.inner.SubscribeNewHead(ctx, ch)
}

func (l *limitedRPC) CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.CallContract(ctx, to, data)
}

func (l *limitedRPC) GetCode(ctx context.Context, address common.Address) ([]byte, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.GetCode(ctx, address)
}

func (l *limitedRPC) GetCodeAt(ctx context.Context, address common.Address, block uint64) ([]byte, error) {
	r, ok := l.inner.(codeAtReader)
	if !ok {
		return nil, errNotSupported
	}
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return r.GetCodeAt(ctx, address, block)
}

func (l *limitedRPC) FetchTracesBatch(ctx context.Context, txHashes []common.Hash, startBlock, endBlock uint64, workers, rateLimit int) ([]*types.InternalTransaction, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.FetchTracesBatch(ctx, txHashes, startBlock, endBlock, workers, l.rate(rateLimit))
}

func (l *limitedRPC) FetchTracesBatchStrict(ctx context.Context, txHashes []common.Hash, blockNumber, timestamp uint64, workers, rateLimit int) ([]*types.InternalTransaction, error) {
	s, ok := l.inner.(strictTracer)
	if !ok {
		return nil, errNotSupported
	}
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return s.FetchTracesBatchStrict(ctx, txHashes, blockNumber, timestamp, workers, l.rate(rateLimit))
}

func (l *limitedRPC) FetchTracesBatchSkippingRefusals(ctx context.Context, txHashes []common.Hash, blockNumber, timestamp uint64, workers, rateLimit int) ([]*types.InternalTransaction, error) {
	s, ok := l.inner.(refusalTracer)
	if !ok {
		return nil, errNotSupported
	}
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return s.FetchTracesBatchSkippingRefusals(ctx, txHashes, blockNumber, timestamp, workers, l.rate(rateLimit))
}

func (l *limitedRPC) FetchTokenMetadataBatch(ctx context.Context, addresses []common.Address, workers int, rateLimit int) (map[common.Address]*rpc.TokenMetadataResult, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.FetchTokenMetadataBatch(ctx, addresses, workers, l.rate(rateLimit))
}

func (l *limitedRPC) FetchTokenURIsBatch(ctx context.Context, reqs []rpc.NFTURIRequest, workers int, rateLimit int) map[int]string {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil
	}
	return l.inner.FetchTokenURIsBatch(ctx, reqs, workers, l.rate(rateLimit))
}

func (l *limitedRPC) ChainID(ctx context.Context) (*big.Int, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.ChainID(ctx)
}

func (l *limitedRPC) TransactionReceipt(ctx context.Context, txHash common.Hash) (*rpclient.Receipt, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.TransactionReceipt(ctx, txHash)
}

func (l *limitedRPC) CallContractAt(ctx context.Context, to common.Address, data []byte, block uint64) ([]byte, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return l.inner.CallContractAt(ctx, to, data, block)
}

func (l *limitedRPC) TransactionCountAt(ctx context.Context, address common.Address, block uint64) (uint64, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	return l.inner.TransactionCountAt(ctx, address, block)
}

func (l *limitedRPC) BlockTransactionCounts(ctx context.Context, from, to uint64, batchSize int) (uint64, error) {
	if err := l.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	return l.inner.BlockTransactionCounts(ctx, from, to, batchSize)
}
