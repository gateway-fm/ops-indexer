package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

var (
	txA = common.HexToHash("0x0a")
	txB = common.HexToHash("0x0b")
)

type rpcReply struct {
	result any
	err    string
}

func fakeNode(t *testing.T, reply func(method string, params []json.RawMessage) rpcReply) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rep := reply(req.Method, req.Params)
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if rep.err != "" {
			resp["error"] = map[string]any{"code": -32000, "message": rep.err}
		} else {
			resp["result"] = rep.result
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	c, err := New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func receiptJSON(h common.Hash) map[string]any {
	return map[string]any{"transactionHash": h.Hex(), "status": "0x1", "gasUsed": "0x5208", "blockNumber": "0x2a", "logs": []any{}}
}

func paramHash(params []json.RawMessage) common.Hash {
	var s string
	_ = json.Unmarshal(params[0], &s)
	return common.HexToHash(s)
}

func fetch(c *Client) (map[common.Hash]*rpclient.Receipt, error) {
	return c.FetchReceiptsBatch(context.Background(), []common.Hash{txA, txB}, 2, 100, 42)
}

func TestFetchReceiptsBatchFailsWhenBothReceiptPathsFail(t *testing.T) {
	c := fakeNode(t, func(method string, _ []json.RawMessage) rpcReply {
		return rpcReply{err: method + ": upstream unavailable"}
	})
	if receipts, err := fetch(c); err == nil {
		t.Fatalf("got %d receipts and no error; want an error so the block is retried", len(receipts))
	}
}

func TestFetchReceiptsBatchFailsWhenBlockReceiptsAreNull(t *testing.T) {
	c := fakeNode(t, func(method string, params []json.RawMessage) rpcReply {
		if method == "eth_getTransactionReceipt" {
			return rpcReply{result: receiptJSON(paramHash(params))}
		}
		return rpcReply{result: nil}
	})
	if receipts, err := fetch(c); err == nil {
		t.Fatalf("got %d receipts and no error for a null eth_getBlockReceipts; want an error", len(receipts))
	}
}

func TestFetchReceiptsBatchFailsWhenBlockReceiptsMissATransaction(t *testing.T) {
	c := fakeNode(t, func(method string, _ []json.RawMessage) rpcReply {
		return rpcReply{result: []any{receiptJSON(txA)}}
	})
	if receipts, err := fetch(c); err == nil {
		t.Fatalf("got %d receipts and no error with tx B missing; want an error", len(receipts))
	}
}

func TestFetchReceiptsBatchFailsWhenOnePerTxReceiptFails(t *testing.T) {
	c := fakeNode(t, func(method string, params []json.RawMessage) rpcReply {
		if method == "eth_getBlockReceipts" {
			return rpcReply{err: "the method eth_getBlockReceipts does not exist"}
		}
		if paramHash(params) == txB {
			return rpcReply{err: "upstream unavailable"}
		}
		return rpcReply{result: receiptJSON(txA)}
	})
	if receipts, err := fetch(c); err == nil {
		t.Fatalf("got %d receipts and no error with tx B's receipt failing; want an error", len(receipts))
	}
}

func TestFetchReceiptsBatchFailsWhenAPerTxReceiptIsNull(t *testing.T) {
	c := fakeNode(t, func(method string, params []json.RawMessage) rpcReply {
		if method == "eth_getBlockReceipts" {
			return rpcReply{err: "the method eth_getBlockReceipts does not exist"}
		}
		if paramHash(params) == txB {
			return rpcReply{result: nil}
		}
		return rpcReply{result: receiptJSON(txA)}
	})
	_, err := fetch(c)
	if !errors.Is(err, rpclient.ErrReceiptNotFound) {
		t.Fatalf("got error %v; want ErrReceiptNotFound for tx B's null receipt", err)
	}
}

func TestFetchReceiptsBatchReturnsEveryReceipt(t *testing.T) {
	for name, blockReceipts := range map[string]rpcReply{
		"batch":  {result: []any{receiptJSON(txA), receiptJSON(txB)}},
		"per_tx": {err: "the method eth_getBlockReceipts does not exist"},
	} {
		t.Run(name, func(t *testing.T) {
			c := fakeNode(t, func(method string, params []json.RawMessage) rpcReply {
				if method == "eth_getBlockReceipts" {
					return blockReceipts
				}
				return rpcReply{result: receiptJSON(paramHash(params))}
			})
			receipts, err := fetch(c)
			if err != nil {
				t.Fatal(err)
			}
			if receipts[txA] == nil || receipts[txB] == nil || receipts[txB].Status != 1 {
				t.Fatalf("got %v; want receipts for both transactions", receipts)
			}
		})
	}
}
