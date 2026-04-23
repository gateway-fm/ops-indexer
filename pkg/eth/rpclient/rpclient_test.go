package rpclient

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
)

func TestReceipt_UnmarshalJSON(t *testing.T) {
	data := `{
		"status": "0x1",
		"gasUsed": "0x5208",
		"blockNumber": "0xa",
		"transactionIndex": "0x0",
		"contractAddress": null,
		"logs": [],
		"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000001"
	}`

	var r Receipt
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatal(err)
	}
	if r.Status != 1 {
		t.Errorf("Status = %d, want 1", r.Status)
	}
	if r.GasUsed != 0x5208 {
		t.Errorf("GasUsed = %d, want %d", r.GasUsed, 0x5208)
	}
	if r.BlockNumber == nil || r.BlockNumber.Int64() != 10 {
		t.Errorf("BlockNumber = %v, want 10", r.BlockNumber)
	}
	if r.TransactionIndex != 0 {
		t.Errorf("TransactionIndex = %d, want 0", r.TransactionIndex)
	}
	if r.ContractAddress != (common.Address{}) {
		t.Errorf("ContractAddress should be zero for null, got %s", r.ContractAddress.Hex())
	}
}

func TestReceipt_UnmarshalJSON_WithContract(t *testing.T) {
	data := `{
		"status": "0x1",
		"gasUsed": "0x10000",
		"blockNumber": "0x5",
		"transactionIndex": "0x2",
		"contractAddress": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"logs": [],
		"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000002"
	}`

	var r Receipt
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatal(err)
	}
	if r.ContractAddress == (common.Address{}) {
		t.Error("ContractAddress should not be zero")
	}
	expected := common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	if r.ContractAddress != expected {
		t.Errorf("ContractAddress = %s, want %s", r.ContractAddress.Hex(), expected.Hex())
	}
}

func TestReceipt_UnmarshalJSON_NilBlockNumber(t *testing.T) {
	data := `{
		"status": "0x1",
		"gasUsed": "0x5208",
		"blockNumber": null,
		"transactionIndex": "0x0",
		"contractAddress": null,
		"logs": [],
		"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000001"
	}`

	var r Receipt
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatal(err)
	}
	if r.BlockNumber != nil {
		t.Errorf("BlockNumber should be nil for pending, got %v", r.BlockNumber)
	}
}

func TestReceipt_UnmarshalJSON_WithLogs(t *testing.T) {
	data := `{
		"status": "0x1",
		"gasUsed": "0x5208",
		"blockNumber": "0x1",
		"transactionIndex": "0x0",
		"contractAddress": null,
		"logs": [{
			"address": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
			"topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
			"data": "0x0000000000000000000000000000000000000000000000000000000000000001",
			"blockNumber": "0x1",
			"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000001",
			"logIndex": "0x0",
			"removed": false,
			"blockHash": "0x0000000000000000000000000000000000000000000000000000000000000001"
		}],
		"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000001"
	}`

	var r Receipt
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Logs) != 1 {
		t.Fatalf("Logs length = %d, want 1", len(r.Logs))
	}
	if r.Logs[0].BlockNumber != 1 {
		t.Errorf("Log.BlockNumber = %d, want 1", r.Logs[0].BlockNumber)
	}
	if len(r.Logs[0].Topics) != 1 {
		t.Errorf("Log.Topics length = %d, want 1", len(r.Logs[0].Topics))
	}
}

func TestLog_UnmarshalJSON(t *testing.T) {
	data := `{
		"address": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"topics": [
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000000000000000000000000000000000000000000001"
		],
		"data": "0xdeadbeef",
		"blockNumber": "0x64",
		"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000abc",
		"logIndex": "0x3",
		"removed": false,
		"blockHash": "0x0000000000000000000000000000000000000000000000000000000000000def"
	}`

	var l Log
	if err := json.Unmarshal([]byte(data), &l); err != nil {
		t.Fatal(err)
	}
	if l.BlockNumber != 100 {
		t.Errorf("BlockNumber = %d, want 100", l.BlockNumber)
	}
	if l.Index != 3 {
		t.Errorf("Index = %d, want 3", l.Index)
	}
	if len(l.Topics) != 2 {
		t.Errorf("Topics length = %d, want 2", len(l.Topics))
	}
	if len(l.Data) != 4 {
		t.Errorf("Data length = %d, want 4", len(l.Data))
	}
	if l.Removed {
		t.Error("Removed should be false")
	}
}

func TestHeader_UnmarshalJSON(t *testing.T) {
	data := `{
		"number": "0x100",
		"timestamp": "0x60000000"
	}`

	var h Header
	if err := json.Unmarshal([]byte(data), &h); err != nil {
		t.Fatal(err)
	}
	if h.Number == nil || h.Number.Int64() != 256 {
		t.Errorf("Number = %v, want 256", h.Number)
	}
	if h.Time != 0x60000000 {
		t.Errorf("Time = %d, want %d", h.Time, 0x60000000)
	}
}

func TestHeader_UnmarshalJSON_NilNumber(t *testing.T) {
	data := `{"number": null, "timestamp": "0x1"}`

	var h Header
	if err := json.Unmarshal([]byte(data), &h); err != nil {
		t.Fatal(err)
	}
	if h.Number != nil {
		t.Errorf("Number should be nil, got %v", h.Number)
	}
}

func TestTransaction_UnmarshalJSON(t *testing.T) {
	data := `{
		"hash": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"blockNumber": "0xa",
		"from": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"to": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
		"value": "0xde0b6b3a7640000",
		"gas": "0x5208",
		"gasPrice": "0x3b9aca00",
		"input": "0x",
		"nonce": "0x0",
		"type": "0x2"
	}`

	var tx Transaction
	if err := json.Unmarshal([]byte(data), &tx); err != nil {
		t.Fatal(err)
	}
	if tx.BlockNumber == nil {
		t.Fatal("BlockNumber should not be nil")
	}
	if tx.BlockNumber.ToInt().Int64() != 10 {
		t.Errorf("BlockNumber = %d, want 10", tx.BlockNumber.ToInt().Int64())
	}
	if tx.To == nil {
		t.Fatal("To should not be nil")
	}
	if uint64(tx.Gas) != 0x5208 {
		t.Errorf("Gas = %d, want %d", tx.Gas, 0x5208)
	}
	if uint64(tx.Type) != 2 {
		t.Errorf("Type = %d, want 2", tx.Type)
	}
}

func TestTransaction_UnmarshalJSON_ContractCreation(t *testing.T) {
	data := `{
		"hash": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"blockNumber": "0x1",
		"from": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"to": null,
		"value": "0x0",
		"gas": "0x100000",
		"gasPrice": "0x0",
		"input": "0x6080604052",
		"nonce": "0x0",
		"type": "0x0"
	}`

	var tx Transaction
	if err := json.Unmarshal([]byte(data), &tx); err != nil {
		t.Fatal(err)
	}
	if tx.To != nil {
		t.Error("To should be nil for contract creation")
	}
}

func TestTransaction_UnmarshalJSON_PendingTx(t *testing.T) {
	data := `{
		"hash": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"blockNumber": null,
		"from": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"to": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
		"value": "0x0",
		"gas": "0x5208",
		"gasPrice": "0x0",
		"input": "0x",
		"nonce": "0x0",
		"type": "0x0"
	}`

	var tx Transaction
	if err := json.Unmarshal([]byte(data), &tx); err != nil {
		t.Fatal(err)
	}
	if tx.BlockNumber != nil {
		t.Error("BlockNumber should be nil for pending tx")
	}
}

func TestSubscribeNewHead_ReturnsError(t *testing.T) {
	c := Dial("http://localhost:1")
	_, err := c.SubscribeNewHead(t.Context(), nil)
	if err == nil {
		t.Error("SubscribeNewHead should always return error")
	}
}

func TestClient_New(t *testing.T) {
	c := New("http://example.com", nil)
	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.url != "http://example.com" {
		t.Errorf("url = %q, want %q", c.url, "http://example.com")
	}
	if c.httpClient == nil {
		t.Error("httpClient should default to non-nil")
	}
}

func TestClient_Dial(t *testing.T) {
	c := Dial("http://example.com")
	if c == nil {
		t.Fatal("Dial returned nil")
	}
}

func TestJsonrpcError_Error(t *testing.T) {
	e := &jsonrpcError{Code: -32600, Message: "invalid request"}
	got := e.Error()
	if got != "rpc error: code -32600, message: invalid request" {
		t.Errorf("Error() = %q", got)
	}
}

func TestFilterQuery_Fields(t *testing.T) {
	q := FilterQuery{
		FromBlock: big.NewInt(1),
		ToBlock:   big.NewInt(100),
		Addresses: []common.Address{common.HexToAddress("0x0000000000000000000000000000000000000001")},
		Topics:    [][]common.Hash{{common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")}},
	}
	if q.FromBlock.Int64() != 1 {
		t.Error("FromBlock wrong")
	}
	if len(q.Addresses) != 1 {
		t.Error("Addresses wrong")
	}
	if len(q.Topics) != 1 {
		t.Error("Topics wrong")
	}
}

func TestCallMsg_Fields(t *testing.T) {
	addr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	msg := CallMsg{
		To:   &addr,
		Data: []byte{0x01, 0x02},
	}
	if msg.To == nil {
		t.Error("To should not be nil")
	}
	if len(msg.Data) != 2 {
		t.Error("Data wrong")
	}
}

func TestBig_NullHandling(t *testing.T) {
	type wrapper struct {
		Value *hexutil.Big `json:"value"`
	}
	data := `{"value": null}`
	var w wrapper
	if err := json.Unmarshal([]byte(data), &w); err != nil {
		t.Fatal(err)
	}
	if w.Value != nil {
		t.Error("null hexutil.Big should unmarshal as nil pointer")
	}
}
