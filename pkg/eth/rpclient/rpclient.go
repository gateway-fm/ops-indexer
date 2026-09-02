package rpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
)

// JSON-RPC call outcomes reported to Observer.
const (
	OutcomeOK             = "ok"
	OutcomeTransportError = "transport_error"
	OutcomeRPCError       = "rpc_error"
	OutcomeDecodeError    = "decode_error"
)

// Observer, if set, is called once per JSON-RPC call. It is the single funnel
// through which every call in the binary passes, so wiring it here covers block,
// receipt, trace, call, balance and code fetches uniformly.
//
// Set it once at startup, before any call is made: it is read on the hot path
// without synchronisation.
var Observer func(method, outcome string, d time.Duration)

func observe(method, outcome string, start time.Time) {
	if Observer != nil {
		Observer(method, outcome, time.Since(start))
	}
}

type Client struct {
	url        string
	httpClient *http.Client
	nextID     atomic.Uint64
}

func New(url string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{url: url, httpClient: httpClient}
}

func Dial(url string) *Client {
	return New(url, nil)
}

type jsonrpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      uint64 `json:"id"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonrpcError   `json:"error"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonrpcError) Error() string {
	return fmt.Sprintf("rpc error: code %d, message: %s", e.Code, e.Message)
}

func (c *Client) CallContext(ctx context.Context, result any, method string, args ...any) error {
	start := time.Now()
	outcome := OutcomeTransportError
	defer func() { observe(method, outcome, start) }()

	if args == nil {
		args = []any{}
	}

	reqBody := jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  args,
		ID:      c.nextID.Add(1),
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("rpc: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("rpc: read response: %w", err)
	}

	var rpcResp jsonrpcResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		outcome = OutcomeDecodeError
		return fmt.Errorf("rpc: unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		outcome = OutcomeRPCError
		return rpcResp.Error
	}

	if result == nil {
		outcome = OutcomeOK
		return nil
	}

	if err := json.Unmarshal(rpcResp.Result, result); err != nil {
		outcome = OutcomeDecodeError
		return err
	}

	outcome = OutcomeOK
	return nil
}

type Receipt struct {
	Status           uint64         `json:"-"`
	GasUsed          uint64         `json:"-"`
	BlockNumber      *big.Int       `json:"-"`
	TransactionIndex uint64         `json:"-"`
	ContractAddress  common.Address `json:"-"`
	Logs             []*Log         `json:"logs"`
	TxHash           common.Hash    `json:"-"`
}

type receiptJSON struct {
	Status           hexutil.Uint64 `json:"status"`
	GasUsed          hexutil.Uint64 `json:"gasUsed"`
	BlockNumber      *hexutil.Big   `json:"blockNumber"`
	TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
	ContractAddress  common.Address `json:"contractAddress"`
	Logs             []*Log         `json:"logs"`
	TxHash           common.Hash    `json:"transactionHash"`
}

func (r *Receipt) UnmarshalJSON(data []byte) error {
	var rj receiptJSON
	if err := json.Unmarshal(data, &rj); err != nil {
		return err
	}
	r.Status = uint64(rj.Status)
	r.GasUsed = uint64(rj.GasUsed)
	if rj.BlockNumber != nil {
		r.BlockNumber = rj.BlockNumber.ToInt()
	}
	r.TransactionIndex = uint64(rj.TransactionIndex)
	r.ContractAddress = rj.ContractAddress
	r.Logs = rj.Logs
	r.TxHash = rj.TxHash
	return nil
}

type Log struct {
	Address     common.Address `json:"address"`
	Topics      []common.Hash  `json:"topics"`
	Data        hexutil.Bytes  `json:"data"`
	BlockNumber uint64         `json:"-"`
	TxHash      common.Hash    `json:"-"`
	Index       uint           `json:"-"`
	Removed     bool           `json:"removed"`
	BlockHash   common.Hash    `json:"-"`
}

type logJSON struct {
	Address     common.Address `json:"address"`
	Topics      []common.Hash  `json:"topics"`
	Data        hexutil.Bytes  `json:"data"`
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	TxHash      common.Hash    `json:"transactionHash"`
	Index       hexutil.Uint64 `json:"logIndex"`
	Removed     bool           `json:"removed"`
	BlockHash   common.Hash    `json:"blockHash"`
}

func (l *Log) UnmarshalJSON(data []byte) error {
	var lj logJSON
	if err := json.Unmarshal(data, &lj); err != nil {
		return err
	}
	l.Address = lj.Address
	l.Topics = lj.Topics
	l.Data = []byte(lj.Data)
	l.BlockNumber = uint64(lj.BlockNumber)
	l.TxHash = lj.TxHash
	l.Index = uint(lj.Index)
	l.Removed = lj.Removed
	l.BlockHash = lj.BlockHash
	return nil
}

type Header struct {
	Number *big.Int
	Time   uint64
}

type headerJSON struct {
	Number *hexutil.Big   `json:"number"`
	Time   hexutil.Uint64 `json:"timestamp"`
}

func (h *Header) UnmarshalJSON(data []byte) error {
	var hj headerJSON
	if err := json.Unmarshal(data, &hj); err != nil {
		return err
	}
	if hj.Number != nil {
		h.Number = hj.Number.ToInt()
	}
	h.Time = uint64(hj.Time)
	return nil
}

type Transaction struct {
	Hash        common.Hash     `json:"hash"`
	BlockNumber *hexutil.Big    `json:"blockNumber"`
	From        common.Address  `json:"from"`
	To          *common.Address `json:"to"`
	Value       *hexutil.Big    `json:"value"`
	Gas         hexutil.Uint64  `json:"gas"`
	GasPrice    *hexutil.Big    `json:"gasPrice"`
	Input       hexutil.Bytes   `json:"input"`
	Nonce       hexutil.Uint64  `json:"nonce"`
	Type        hexutil.Uint64  `json:"type"`
}

type CallMsg struct {
	To   *common.Address
	Data []byte
}

type FilterQuery struct {
	FromBlock *big.Int
	ToBlock   *big.Int
	Addresses []common.Address
	Topics    [][]common.Hash
}

type Subscription interface {
	Err() <-chan error
	Unsubscribe()
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var result hexutil.Uint64
	if err := c.CallContext(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}
	return uint64(result), nil
}

func (c *Client) BalanceAt(ctx context.Context, address common.Address, block *big.Int) (*big.Int, error) {
	blockArg := "latest"
	if block != nil {
		blockArg = "0x" + block.Text(16)
	}
	var result hexutil.Big
	if err := c.CallContext(ctx, &result, "eth_getBalance", address.Hex(), blockArg); err != nil {
		return nil, err
	}
	return result.ToInt(), nil
}

func (c *Client) CodeAt(ctx context.Context, address common.Address, block *big.Int) ([]byte, error) {
	blockArg := "latest"
	if block != nil {
		blockArg = "0x" + block.Text(16)
	}
	var result hexutil.Bytes
	if err := c.CallContext(ctx, &result, "eth_getCode", address.Hex(), blockArg); err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	var result hexutil.Big
	if err := c.CallContext(ctx, &result, "eth_chainId"); err != nil {
		return nil, err
	}
	return result.ToInt(), nil
}

func (c *Client) CallContract(ctx context.Context, msg CallMsg, block *big.Int) ([]byte, error) {
	blockArg := "latest"
	if block != nil {
		blockArg = "0x" + block.Text(16)
	}
	callObj := map[string]string{
		"data": "0x" + common.Bytes2Hex(msg.Data),
	}
	if msg.To != nil {
		callObj["to"] = msg.To.Hex()
	}
	var result hexutil.Bytes
	if err := c.CallContext(ctx, &result, "eth_call", callObj, blockArg); err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*Receipt, error) {
	var receipt Receipt
	if err := c.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txHash.Hex()); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func (c *Client) TransactionByHash(ctx context.Context, txHash common.Hash) (*Transaction, bool, error) {
	var tx Transaction
	if err := c.CallContext(ctx, &tx, "eth_getTransactionByHash", txHash.Hex()); err != nil {
		return nil, false, err
	}
	isPending := tx.BlockNumber == nil
	return &tx, isPending, nil
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*Header, error) {
	blockArg := "latest"
	if number != nil {
		blockArg = "0x" + number.Text(16)
	}
	var header Header
	if err := c.CallContext(ctx, &header, "eth_getBlockByNumber", blockArg, false); err != nil {
		return nil, err
	}
	return &header, nil
}

func (c *Client) FilterLogs(ctx context.Context, q FilterQuery) ([]Log, error) {
	arg := map[string]any{}
	if q.FromBlock != nil {
		arg["fromBlock"] = "0x" + q.FromBlock.Text(16)
	}
	if q.ToBlock != nil {
		arg["toBlock"] = "0x" + q.ToBlock.Text(16)
	}
	if len(q.Addresses) > 0 {
		addrs := make([]string, len(q.Addresses))
		for i, a := range q.Addresses {
			addrs[i] = a.Hex()
		}
		arg["address"] = addrs
	}
	if len(q.Topics) > 0 {
		topics := make([]any, len(q.Topics))
		for i, topicSet := range q.Topics {
			if len(topicSet) == 1 {
				topics[i] = topicSet[0].Hex()
			} else if len(topicSet) > 1 {
				hashes := make([]string, len(topicSet))
				for j, t := range topicSet {
					hashes[j] = t.Hex()
				}
				topics[i] = hashes
			} else {
				topics[i] = nil
			}
		}
		arg["topics"] = topics
	}
	var logs []Log
	if err := c.CallContext(ctx, &logs, "eth_getLogs", arg); err != nil {
		return nil, err
	}
	return logs, nil
}

func (c *Client) SubscribeNewHead(ctx context.Context, ch chan<- *Header) (Subscription, error) {
	return nil, fmt.Errorf("rpclient: WebSocket subscriptions not supported, use polling fallback")
}
