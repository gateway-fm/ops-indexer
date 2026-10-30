//go:build faults

package faultkit

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fault is what a hook makes the node answer instead of the real result.
type Fault struct {
	Null  bool          // result null
	Err   string        // JSON-RPC error with this message
	HTTP  int           // HTTP status instead of a JSON-RPC reply (the whole batch)
	Delay time.Duration // wait before answering (a timeout, with the client's deadline)
	// Result, when set, is answered as the result.
	Result any
}

// Hook sees every call (each element of a batch) and may return a fault; nil answers normally.
type Hook func(method string, params []json.RawMessage, call int) *Fault

// Node is a JSON-RPC node over HTTP serving a deterministic chain: block n has one ERC-20 transfer of token
// TokenAddr from Addr(n%5) to Addr(10+n%7), timestamp Base+n*BlockTime. Forks replace blocks from a height on.
type Node struct {
	mu        sync.Mutex
	Head      uint64
	Base      uint64
	BlockTime uint64
	forks     map[uint64]uint64 // from height -> fork id (0 = canonical)
	headerFk  map[uint64]uint64 // per-height fork for header-only lookups (they disagree with full reads)
	lagAbove  *uint64           // blocks above this answer null (full and header) though Head says more
	hooks     []Hook
	calls     map[string]int
	srv       *httptest.Server
}

const TokenAddr = "0x00000000000000000000000000000000000000aa"

// Addr is the deterministic address i.
func Addr(i uint64) string { return fmt.Sprintf("0x%040x", 0x1000+i) }

// NewNode serves a chain with head at the given height.
func NewNode(t testing.TB, head uint64) *Node {
	t.Helper()
	n := &Node{Head: head, Base: 1_767_225_600, BlockTime: 600, forks: map[uint64]uint64{}, headerFk: map[uint64]uint64{},
		calls: map[string]int{}}
	n.srv = httptest.NewServer(n)
	t.Cleanup(n.srv.Close)
	return n
}

func (n *Node) URL() string { return n.srv.URL }

func (n *Node) SetHead(h uint64) { n.mu.Lock(); n.Head = h; n.mu.Unlock() }
func (n *Node) GetHead() uint64  { n.mu.Lock(); defer n.mu.Unlock(); return n.Head }

// Fork puts every block from height on fork id (a reorg at height when the indexer has stored the old ones).
func (n *Node) Fork(height, id uint64) { n.mu.Lock(); n.forks[height] = id; n.mu.Unlock() }

// HeaderFork makes header-only lookups of height answer fork id while full reads answer the real one.
func (n *Node) HeaderFork(height, id uint64) { n.mu.Lock(); n.headerFk[height] = id; n.mu.Unlock() }

// LagAbove makes blocks above h answer null (a load-balanced node behind the head it reports); nil clears it.
func (n *Node) LagAbove(h *uint64) { n.mu.Lock(); n.lagAbove = h; n.mu.Unlock() }

// AddHook adds a fault hook; ClearHooks removes them.
func (n *Node) AddHook(h Hook) { n.mu.Lock(); n.hooks = append(n.hooks, h); n.mu.Unlock() }
func (n *Node) ClearHooks()    { n.mu.Lock(); n.hooks = nil; n.mu.Unlock() }

// Calls reports how many calls of method the node answered or faulted.
func (n *Node) Calls(method string) int { n.mu.Lock(); defer n.mu.Unlock(); return n.calls[method] }

func (n *Node) forkOf(h uint64) uint64 {
	var best, id uint64
	found := false
	for from, f := range n.forks {
		if from <= h && (!found || from > best) {
			best, id, found = from, f, true
		}
	}
	return id
}

// Hash is block h's hash on fork f; TxHash its transaction's.
func Hash(f, h uint64) string   { return fmt.Sprintf("0x%016x%048x", f+1, h) }
func TxHash(f, h uint64) string { return fmt.Sprintf("0x%016x%048x", 0xff00+f, h) }

// BlockHash is block h's hash on the fork the node serves now.
func (n *Node) BlockHash(h uint64) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Hash(n.forkOf(h), h)
}

func hx(v uint64) string { return fmt.Sprintf("0x%x", v) }

func (n *Node) txJSON(f, h uint64) map[string]any {
	from, to := Addr(h%5), Addr(10+h%7)
	input := "0xa9059cbb" + fmt.Sprintf("%064s", strings.TrimPrefix(to, "0x")) + fmt.Sprintf("%064x", h+1)
	return map[string]any{"hash": TxHash(f, h), "blockHash": Hash(f, h), "blockNumber": hx(h), "transactionIndex": "0x0",
		"from": from, "to": TokenAddr, "value": "0x0", "gas": "0x186a0", "gasPrice": "0x3b9aca00", "input": input,
		"nonce": hx(h), "type": "0x0", "v": "0x1b", "r": "0x1", "s": "0x1"}
}

func topicAddr(a string) string { return "0x" + fmt.Sprintf("%064s", strings.TrimPrefix(a, "0x")) }

func (n *Node) receiptJSON(f, h uint64) map[string]any {
	from, to := Addr(h%5), Addr(10+h%7)
	return map[string]any{"status": "0x1", "gasUsed": "0xc350", "blockNumber": hx(h), "blockHash": Hash(f, h),
		"transactionIndex": "0x0", "contractAddress": nil, "transactionHash": TxHash(f, h), "cumulativeGasUsed": "0xc350",
		"from": from, "to": TokenAddr, "effectiveGasPrice": "0x3b9aca00", "type": "0x0",
		"logs": []any{map[string]any{"address": TokenAddr, "topics": []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", topicAddr(from), topicAddr(to)},
			"data": fmt.Sprintf("0x%064x", h+1), "blockNumber": hx(h), "transactionHash": TxHash(f, h), "logIndex": "0x0",
			"removed": false, "blockHash": Hash(f, h), "transactionIndex": "0x0"}}}
}

func (n *Node) blockJSON(h uint64, full, header bool) any {
	if h > n.Head || (n.lagAbove != nil && h > *n.lagAbove) {
		return nil
	}
	f := n.forkOf(h)
	if header {
		if hf, ok := n.headerFk[h]; ok {
			f = hf
		}
	}
	var pf uint64
	if h > 0 {
		pf = n.forkOf(h - 1)
	}
	parent := "0x" + strings.Repeat("0", 64)
	if h > 0 {
		parent = Hash(pf, h-1)
	}
	var txs []any
	if full {
		txs = []any{n.txJSON(f, h)}
	} else {
		txs = []any{TxHash(f, h)}
	}
	return map[string]any{"number": hx(h), "hash": Hash(f, h), "parentHash": parent, "timestamp": hx(n.Base + h*n.BlockTime),
		"gasUsed": "0xc350", "gasLimit": "0x1c9c380", "baseFeePerGas": "0x7", "miner": Addr(99), "difficulty": "0x0",
		"totalDifficulty": "0x0", "size": "0x300", "nonce": "0x0000000000000000", "extraData": "0x",
		"stateRoot": Hash(9, h), "transactionsRoot": Hash(9, h), "receiptsRoot": Hash(9, h), "transactions": txs,
		"uncles": []any{}, "logsBloom": "0x" + strings.Repeat("0", 512), "sha3Uncles": Hash(9, 0), "mixHash": Hash(9, 0)}
}

func parseNum(raw json.RawMessage, head uint64) uint64 {
	var s string
	_ = json.Unmarshal(raw, &s)
	switch s {
	case "latest", "safe", "finalized", "pending":
		return head
	case "earliest":
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	return v
}

type req struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func (n *Node) answer(r req) (map[string]any, time.Duration, int) {
	n.mu.Lock()
	n.calls[r.Method]++
	call := n.calls[r.Method]
	hooks := append([]Hook(nil), n.hooks...)
	n.mu.Unlock()
	resp := map[string]any{"jsonrpc": "2.0", "id": r.ID}
	for _, h := range hooks {
		if f := h(r.Method, r.Params, call); f != nil {
			switch {
			case f.HTTP != 0:
				return nil, f.Delay, f.HTTP
			case f.Result != nil:
				resp["result"] = f.Result
			case f.Err != "":
				resp["error"] = map[string]any{"code": -32000, "message": f.Err}
			default:
				resp["result"] = nil
			}
			return resp, f.Delay, 0
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	switch r.Method {
	case "eth_blockNumber":
		resp["result"] = hx(n.Head)
	case "eth_chainId", "net_version":
		resp["result"] = "0x1"
	case "eth_getBlockByNumber":
		var full bool
		if len(r.Params) > 1 {
			_ = json.Unmarshal(r.Params[1], &full)
		}
		resp["result"] = n.blockJSON(parseNum(r.Params[0], n.Head), full, !full)
	case "eth_getBlockByHash":
		var hs string
		_ = json.Unmarshal(r.Params[0], &hs)
		var full bool
		if len(r.Params) > 1 {
			_ = json.Unmarshal(r.Params[1], &full)
		}
		resp["result"] = nil
		if v, err := strconv.ParseUint(hs[len(hs)-48:], 16, 64); err == nil && Hash(n.forkOf(v), v) == hs {
			resp["result"] = n.blockJSON(v, full, false)
		}
	case "eth_getBlockReceipts":
		h := parseNum(r.Params[0], n.Head)
		if n.blockJSON(h, false, false) == nil {
			resp["result"] = nil
		} else {
			resp["result"] = []any{n.receiptJSON(n.forkOf(h), h)}
		}
	case "eth_getTransactionReceipt", "eth_getTransactionByHash":
		var hs string
		_ = json.Unmarshal(r.Params[0], &hs)
		resp["result"] = nil
		if v, err := strconv.ParseUint(hs[len(hs)-48:], 16, 64); err == nil && TxHash(n.forkOf(v), v) == hs && v <= n.Head {
			if r.Method == "eth_getTransactionReceipt" {
				resp["result"] = n.receiptJSON(n.forkOf(v), v)
			} else {
				resp["result"] = n.txJSON(n.forkOf(v), v)
			}
		}
	case "eth_getLogs":
		var q struct {
			From string `json:"fromBlock"`
			To   string `json:"toBlock"`
		}
		_ = json.Unmarshal(r.Params[0], &q)
		from, to := parseNum(json.RawMessage(strconv.Quote(q.From)), n.Head), parseNum(json.RawMessage(strconv.Quote(q.To)), n.Head)
		var logs []any
		for h := from; h <= to && h <= n.Head; h++ {
			logs = append(logs, n.receiptJSON(n.forkOf(h), h)["logs"].([]any)...)
		}
		resp["result"] = logs
	case "eth_call", "eth_getCode":
		resp["result"] = "0x"
	case "eth_getBalance":
		resp["result"] = "0xde0b6b3a7640000"
	case "eth_gasPrice":
		resp["result"] = "0x3b9aca00"
	case "eth_syncing":
		resp["result"] = false
	default:
		resp["error"] = map[string]any{"code": -32601, "message": "the method " + r.Method + " does not exist"}
	}
	return resp, 0, 0
}

func (n *Node) ServeHTTP(w http.ResponseWriter, hr *http.Request) {
	body, _ := io.ReadAll(hr.Body)
	w.Header().Set("Content-Type", "application/json")
	trim := strings.TrimSpace(string(body))
	if strings.HasPrefix(trim, "[") {
		var reqs []req
		_ = json.Unmarshal(body, &reqs)
		out := make([]any, 0, len(reqs))
		var delay time.Duration
		for _, r := range reqs {
			resp, d, status := n.answer(r)
			if status != 0 {
				time.Sleep(d)
				w.WriteHeader(status)
				return
			}
			delay = max(delay, d)
			out = append(out, resp)
		}
		time.Sleep(delay)
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	var r req
	_ = json.Unmarshal(body, &r)
	resp, d, status := n.answer(r)
	time.Sleep(d)
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// MethodHook faults every call of method for which pick(number, call) is true (number from the first param when
// it is a block number, else 0).
func MethodHook(method string, f Fault, pick func(number uint64, call int) bool) Hook {
	return func(m string, params []json.RawMessage, call int) *Fault {
		if m != method {
			return nil
		}
		var num uint64
		if len(params) > 0 {
			num = parseNum(params[0], 0)
		}
		if pick == nil || pick(num, call) {
			ff := f
			return &ff
		}
		return nil
	}
}

