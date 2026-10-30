package nodelookup

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
)

// receiptsNode is a JSON-RPC node holding block 7 with two transactions, the second one reverted. receipts sets what
// eth_getBlockReceipts answers: "null", "partial" (the first receipt only) or "full".
type receiptsNode struct {
	mu            sync.Mutex
	receipts      string
	blockCalls    atomic.Int64
	receiptsCalls atomic.Int64
}

func (n *receiptsNode) mode() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.receipts
}

func (n *receiptsNode) setMode(m string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.receipts = m
}

func (n *receiptsNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	_ = json.Unmarshal(body, &req)
	var result any
	switch req.Method {
	case "eth_getBlockByNumber":
		n.blockCalls.Add(1)
		number := hexutil.Big(*big.NewInt(7))
		to := common.HexToAddress("0xb0b")
		result = &rpc.RawBlock{Number: &number, Hash: blockHash(7), Timestamp: 1_700_000_007, GasLimit: 30_000_000,
			Transactions: []rpc.RawTransaction{
				{Hash: txHash(70), From: common.HexToAddress("0xa11ce"), To: &to, Gas: 50_000},
				{Hash: txHash(71), From: common.HexToAddress("0xa11ce"), To: &to, Gas: 90_000},
			}}
	case "eth_getTransactionByHash":
		result = map[string]any{"blockNumber": "0x7"}
	case "eth_getBlockReceipts":
		n.receiptsCalls.Add(1)
		first := map[string]any{"transactionHash": txHash(70), "status": "0x1", "gasUsed": "0x5208", "logs": []any{}}
		second := map[string]any{"transactionHash": txHash(71), "status": "0x0", "gasUsed": "0x7530", "logs": []any{}}
		switch n.mode() {
		case "partial":
			result = []any{first}
		case "full":
			result = []any{first, second}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func TestBlockWithoutAllItsReceiptsIsUnavailableAndNotCached(t *testing.T) {
	for _, mode := range []string{"null", "partial"} {
		t.Run(mode, func(t *testing.T) {
			node := &receiptsNode{receipts: mode}
			srv := httptest.NewServer(node)
			defer srv.Close()
			client, err := rpc.New(srv.URL)
			require.NoError(t, err)
			l := New(NewNodeSource(client), Config{Timeout: 5 * time.Second})
			ctx := context.Background()

			for i := 0; i < 2; i++ {
				b, err := l.BlockByNumber(ctx, 7)
				require.ErrorIs(t, err, ErrReceiptsUnavailable, "the node's receipts for block 7 are %s", mode)
				require.Nil(t, b, "no block is made up without its receipts")
			}
			require.EqualValues(t, 2, node.receiptsCalls.Load(), "nothing was cached: the second request asked the node again")

			node.setMode("full")
			b, err := l.BlockByNumber(ctx, 7)
			require.NoError(t, err)
			require.Len(t, b.Txs, 2)
			require.Equal(t, 1, b.Txs[0].Status)
			require.EqualValues(t, 21_000, b.Txs[0].GasUsed)
			require.Equal(t, 0, b.Txs[1].Status, "the reverted transaction is served as reverted")
			require.EqualValues(t, 30_000, b.Txs[1].GasUsed)
			tx, err := l.Transaction(ctx, txHash(71).Hex(), 100)
			require.NoError(t, err)
			require.Equal(t, 0, tx.Status)
		})
	}
}
