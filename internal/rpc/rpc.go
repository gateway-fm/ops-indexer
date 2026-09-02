package rpc

import (
	"context"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	explorerTypes "github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type contextKey string

const (
	ContextKeyAuthToken contextKey = "authToken"
)

type authTransport struct {
	underlying http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if token, ok := req.Context().Value(ContextKeyAuthToken).(string); ok && token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return t.underlying.RoundTrip(req)
}

type Client struct {
	raw *rpclient.Client
}

func New(url string) (*Client, error) {
	httpClient := &http.Client{
		Transport: &authTransport{underlying: http.DefaultTransport},
	}

	raw := rpclient.New(url, httpClient)
	return &Client{raw: raw}, nil
}

func (c *Client) Raw() *rpclient.Client {
	return c.raw
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	return c.raw.BlockNumber(ctx)
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*rpclient.Receipt, error) {
	return c.raw.TransactionReceipt(ctx, txHash)
}

func (c *Client) TransactionByHash(ctx context.Context, txHash common.Hash) (*rpclient.Transaction, bool, error) {
	return c.raw.TransactionByHash(ctx, txHash)
}

func (c *Client) GetBalance(ctx context.Context, address common.Address) (*big.Int, error) {
	return c.raw.BalanceAt(ctx, address, nil)
}

func (c *Client) GetCode(ctx context.Context, address common.Address) ([]byte, error) {
	return c.raw.CodeAt(ctx, address, nil)
}

func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	return c.raw.ChainID(ctx)
}

func (c *Client) CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	msg := rpclient.CallMsg{
		To:   &to,
		Data: data,
	}
	return c.raw.CallContract(ctx, msg, nil)
}

func (c *Client) GetTransactionByHash(ctx context.Context, hash common.Hash) (*explorerTypes.Transaction, error) {
	tx, isPending, err := c.raw.TransactionByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	receipt, err := c.raw.TransactionReceipt(ctx, hash)
	if err != nil {
		log.Warn("failed to fetch receipt for tx lookup", "tx", hash.Hex(), "error", err)
	}

	var to *string
	if tx.To != nil {
		toStr := tx.To.Hex()
		to = &toStr
	}

	inputData := ""
	if len(tx.Input) > 0 {
		if len(tx.Input) >= 4 {
			inputData = common.Bytes2Hex(tx.Input[:4])
		} else {
			inputData = common.Bytes2Hex(tx.Input)
		}
	}

	var blockNum uint64
	var txIndex int
	var gasUsed uint64
	var status int

	if receipt != nil {
		if !isPending && receipt.BlockNumber != nil {
			blockNum = receipt.BlockNumber.Uint64()
		}
		txIndex = int(receipt.TransactionIndex)
		gasUsed = receipt.GasUsed
		status = int(receipt.Status)
	} else {
		gasUsed = uint64(tx.Gas)
		status = 1
	}

	txValue := "0"
	if tx.Value != nil {
		txValue = tx.Value.ToInt().String()
	}

	txGasPrice := uint64(0)
	if tx.GasPrice != nil {
		txGasPrice = tx.GasPrice.ToInt().Uint64()
	}

	return &explorerTypes.Transaction{
		Hash:        tx.Hash.Hex(),
		BlockNumber: blockNum,
		TxIndex:     txIndex,
		From:        tx.From.Hex(),
		To:          to,
		Value:       explorerTypes.JSONString(txValue),
		GasUsed:     gasUsed,
		GasPrice:    txGasPrice,
		InputData:   inputData,
		Status:      status,
		CreatedAt:   time.Now(),
	}, nil
}

type ReceiptResult struct {
	TxHash  common.Hash
	Receipt *rpclient.Receipt
	Err     error
}

// FetchBlockReceipts fetches all receipts for a block in a single RPC call
// using eth_getBlockReceipts. Returns nil if the method is not supported.
func (c *Client) FetchBlockReceipts(ctx context.Context, blockNumber uint64) ([]*rpclient.Receipt, error) {
	var receipts []*rpclient.Receipt
	err := c.raw.CallContext(ctx, &receipts, "eth_getBlockReceipts", toHex(blockNumber))
	if err != nil {
		return nil, err
	}
	return receipts, nil
}

// fetchLogsAsReceipts is a fallback for nodes that refuse receipts on certain
// blocks — observed on op-reth in OP-Stack mode when the block-builder doesn't
// include the L1-info system transaction at index 0, which makes the node 500
// every eth_getBlockReceipts / eth_getTransactionReceipt with
// "unexpected l1 block info tx calldata length". eth_getLogs works because
// it reads logs from the block-receipt index without parsing tx[0].
//
// The synthetic receipts here are LOG-ONLY: Status defaults to 1 (assumed
// success — there's no way to recover it without the real receipt), GasUsed
// stays 0, ContractAddress is left zero. The caller (processBlockParallelRaw)
// already tolerates these gaps when receipt fields aren't authoritative.
//
// Returned map is keyed by tx hash; txs with no logs simply won't appear.
func (c *Client) fetchLogsAsReceipts(ctx context.Context, blockNumber uint64) (map[common.Hash]*rpclient.Receipt, error) {
	blockBig := new(big.Int).SetUint64(blockNumber)
	logs, err := c.raw.FilterLogs(ctx, rpclient.FilterQuery{
		FromBlock: blockBig,
		ToBlock:   blockBig,
	})
	if err != nil {
		return nil, err
	}

	receipts := make(map[common.Hash]*rpclient.Receipt)
	for i := range logs {
		l := &logs[i]
		r, ok := receipts[l.TxHash]
		if !ok {
			r = &rpclient.Receipt{
				TxHash:      l.TxHash,
				BlockNumber: blockBig,
				Status:      1,
			}
			receipts[l.TxHash] = r
		}
		r.Logs = append(r.Logs, l)
	}
	return receipts, nil
}

func (c *Client) FetchReceiptsBatch(ctx context.Context, txHashes []common.Hash, workers int, rateLimit int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	if len(txHashes) == 0 {
		return make(map[common.Hash]*rpclient.Receipt), nil
	}

	// Try eth_getBlockReceipts first (single RPC call for all receipts)
	if len(blockNumber) > 0 {
		receipts, err := c.FetchBlockReceipts(ctx, blockNumber[0])
		if err == nil && receipts != nil {
			results := make(map[common.Hash]*rpclient.Receipt, len(receipts))
			for _, r := range receipts {
				results[r.TxHash] = r
			}
			log.Info("fetched receipts via eth_getBlockReceipts",
				"block", blockNumber[0], "count", len(results))
			return results, nil
		}
		if err != nil {
			// If the error indicates the method doesn't exist, fall through to per-TX.
			// For any other error (data errors, node issues), try eth_getLogs as a
			// last resort so we can at least index events/token transfers — op-reth
			// in OP-Stack mode refuses receipts on blocks whose tx[0] isn't the L1
			// info system tx, but eth_getLogs still works on those same blocks.
			errMsg := err.Error()
			if strings.Contains(errMsg, "method not found") || strings.Contains(errMsg, "not supported") || strings.Contains(errMsg, "does not exist") {
				log.Warn("eth_getBlockReceipts not supported, falling back to per-TX fetch",
					"block", blockNumber[0], "error", err)
			} else {
				logReceipts, lerr := c.fetchLogsAsReceipts(ctx, blockNumber[0])
				if lerr == nil {
					log.Warn("eth_getBlockReceipts failed; recovered logs via eth_getLogs (status/gasUsed not available)",
						"block", blockNumber[0], "receipts_err", err, "txs_with_logs", len(logReceipts), "total_txs", len(txHashes))
					return logReceipts, nil
				}
				log.Warn("eth_getBlockReceipts failed and eth_getLogs fallback also failed; skipping receipts for block",
					"block", blockNumber[0], "receipts_err", err, "logs_err", lerr, "txs", len(txHashes))
				return make(map[common.Hash]*rpclient.Receipt), nil
			}
		}
	}

	// Fallback: fetch receipts individually per transaction (only when eth_getBlockReceipts is not supported)
	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit/10+1)

	results := make(map[common.Hash]*rpclient.Receipt)
	var mu sync.Mutex
	var failCount int

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, hash := range txHashes {
		hash := hash
		g.Go(func() error {
			if err := limiter.Wait(ctx); err != nil {
				return err
			}

			receipt, err := c.raw.TransactionReceipt(ctx, hash)
			if err != nil {
				mu.Lock()
				failCount++
				mu.Unlock()
				log.Warn("failed to fetch receipt",
					"tx", hash.Hex(), "error", err)
				return nil
			}

			mu.Lock()
			results[hash] = receipt
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if failCount > 0 {
		log.Warn("some receipts could not be fetched",
			"failed", failCount, "total", len(txHashes))
	}

	return results, nil
}

type TokenMetadataResult struct {
	Address  common.Address
	Symbol   string
	Name     *string
	Decimals int
	Err      error
}

func (c *Client) FetchTokenMetadataBatch(ctx context.Context, addresses []common.Address, workers int, rateLimit int) (map[common.Address]*TokenMetadataResult, error) {
	if len(addresses) == 0 {
		return make(map[common.Address]*TokenMetadataResult), nil
	}

	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit/10+1)

	results := make(map[common.Address]*TokenMetadataResult)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, addr := range addresses {
		addr := addr // capture loop variable
		g.Go(func() error {
			result := &TokenMetadataResult{Address: addr}

			if err := limiter.Wait(ctx); err != nil {
				result.Err = err
				mu.Lock()
				results[addr] = result
				mu.Unlock()
				return nil // Don't fail the whole batch
			}
			symbolData, err := c.CallContract(ctx, addr, common.FromHex("0x95d89b41"))
			if err != nil {
				result.Symbol = "UNKNOWN"
			} else {
				result.Symbol = parseStringResult(symbolData)
				if result.Symbol == "" {
					result.Symbol = "UNKNOWN"
				}
			}

			if err := limiter.Wait(ctx); err != nil {
				mu.Lock()
				results[addr] = result
				mu.Unlock()
				return nil
			}
			nameData, err := c.CallContract(ctx, addr, common.FromHex("0x06fdde03"))
			if err == nil {
				name := parseStringResult(nameData)
				if name != "" {
					result.Name = &name
				}
			}

			if err := limiter.Wait(ctx); err != nil {
				mu.Lock()
				results[addr] = result
				mu.Unlock()
				return nil
			}
			decimalsData, err := c.CallContract(ctx, addr, common.FromHex("0x313ce567"))
			if err == nil && len(decimalsData) >= 32 {
				result.Decimals = int(new(big.Int).SetBytes(decimalsData).Int64())
			} else {
				result.Decimals = 18 // Default
			}

			mu.Lock()
			results[addr] = result
			mu.Unlock()
			return nil
		})
	}

	g.Wait() // Errors are captured in results
	return results, nil
}

// NFTURIRequest identifies a single token whose metadata URI should be fetched.
// Selector is the 4-byte accessor selector (hex, e.g. "0x0e89341c" for ERC-1155
// uri(uint256)); empty defaults to ERC-721 tokenURI(uint256).
type NFTURIRequest struct {
	Address  common.Address
	TokenID  *big.Int
	Selector string
}

// FetchTokenURIsBatch calls tokenURI(uint256) for each request concurrently and
// returns a map keyed by the request's index in reqs. A missing entry means the
// call reverted or returned no string (tolerated — many ERC721s omit metadata).
func (c *Client) FetchTokenURIsBatch(ctx context.Context, reqs []NFTURIRequest, workers int, rateLimit int) map[int]string {
	results := make(map[int]string, len(reqs))
	if len(reqs) == 0 {
		return results
	}

	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit/10+1)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for i, req := range reqs {
		i, req := i, req // capture loop variables
		g.Go(func() error {
			if err := limiter.Wait(ctx); err != nil {
				return nil
			}
			// selector + 32-byte big-endian id. Defaults to tokenURI(uint256)
			// (0xc87b56dd); ERC-1155 passes uri(uint256) (0x0e89341c).
			selector := req.Selector
			if selector == "" {
				selector = "0xc87b56dd"
			}
			data := make([]byte, 4+32)
			copy(data, common.FromHex(selector))
			if req.TokenID != nil {
				req.TokenID.FillBytes(data[4:])
			}
			out, err := c.CallContract(ctx, req.Address, data)
			if err != nil {
				return nil // tolerate reverts
			}
			if uri := parseStringResult(out); uri != "" {
				mu.Lock()
				results[i] = uri
				mu.Unlock()
			}
			return nil
		})
	}

	g.Wait()
	return results
}

func parseStringResult(data []byte) string {
	if len(data) < 64 {
		result := string(data)
		for i := 0; i < len(result); i++ {
			if result[i] == 0 {
				return result[:i]
			}
		}
		return result
	}

	offset := new(big.Int).SetBytes(data[:32]).Uint64()
	if offset >= uint64(len(data)) {
		return ""
	}

	if offset+32 > uint64(len(data)) {
		return ""
	}

	length := new(big.Int).SetBytes(data[offset : offset+32]).Uint64()
	if offset+32+length > uint64(len(data)) {
		return ""
	}

	result := string(data[offset+32 : offset+32+length])
	for i := 0; i < len(result); i++ {
		if result[i] == 0 {
			return result[:i]
		}
	}
	return result
}

func (c *Client) FetchBalanceBatch(ctx context.Context, requests []BalanceRequest, workers int, rateLimit int) map[string]*big.Int {
	if len(requests) == 0 {
		return make(map[string]*big.Int)
	}

	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit/10+1)
	results := make(map[string]*big.Int)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, req := range requests {
		req := req
		g.Go(func() error {
			if err := limiter.Wait(ctx); err != nil {
				return nil
			}

			addrPadded := common.LeftPadBytes(req.Address.Bytes(), 32)
			callData := append(common.FromHex("0x70a08231"), addrPadded...)

			balanceData, err := c.CallContract(ctx, req.TokenAddress, callData)
			if err != nil || len(balanceData) < 32 {
				return nil
			}

			balance := new(big.Int).SetBytes(balanceData)
			key := req.Address.Hex() + ":" + req.TokenAddress.Hex()

			mu.Lock()
			results[key] = balance
			mu.Unlock()
			return nil
		})
	}

	g.Wait()
	return results
}

type BalanceRequest struct {
	Address      common.Address
	TokenAddress common.Address
	BlockNumber  uint64
}

func (c *Client) SubscribeNewHead(ctx context.Context, ch chan<- *rpclient.Header) (rpclient.Subscription, error) {
	return c.raw.SubscribeNewHead(ctx, ch)
}

type RawBlockResponse struct {
	TotalDifficulty string `json:"totalDifficulty"`
}

func (c *Client) GetTotalDifficulty(ctx context.Context, blockNumber uint64) string {
	var result RawBlockResponse
	err := c.raw.CallContext(ctx, &result, "eth_getBlockByNumber", toHex(blockNumber), false)
	if err != nil || result.TotalDifficulty == "" {
		return "0"
	}
	td := new(big.Int)
	if _, ok := td.SetString(result.TotalDifficulty, 0); !ok {
		return "0"
	}
	return td.String()
}

func toHex(n uint64) string {
	return "0x" + new(big.Int).SetUint64(n).Text(16)
}
