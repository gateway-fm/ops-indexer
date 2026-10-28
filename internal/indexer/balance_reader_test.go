package indexer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

type readerRPC struct {
	*MockRPCClient
	calls   [][]common.Address
	failing map[common.Address]bool
	errTok  common.Address
}

func (r *readerRPC) BalancesOf(_ context.Context, token common.Address, holders []common.Address, _ bool) ([]*big.Int, []bool, error) {
	r.calls = append(r.calls, append([]common.Address{token}, holders...))
	if token == r.errTok {
		return nil, nil, errors.New("call failed")
	}
	values := make([]*big.Int, len(holders))
	ok := make([]bool, len(holders))
	for i, h := range holders {
		values[i] = new(big.Int).SetBytes(h.Bytes())
		ok[i] = !r.failing[h]
	}
	return values, ok, nil
}

func TestFetchReaderGroupsByTokenFoldsRepeatsAndSkipsFailures(t *testing.T) {
	tok, tok2, tokErr := common.HexToAddress("0x01"), common.HexToAddress("0x02"), common.HexToAddress("0x03")
	a, b, c, d := common.HexToAddress("0xa"), common.HexToAddress("0xb"), common.HexToAddress("0xc"), common.HexToAddress("0xd")
	rpc := &readerRPC{MockRPCClient: &MockRPCClient{}, failing: map[common.Address]bool{b: true}, errTok: tokErr}
	p := NewBalanceWorkerPool(nil, rpc, 0, 0)
	p.SetReaderBatch(100, true)
	if p.balancesReader == nil {
		t.Fatal("reader not enabled for a client that implements BalancesOf")
	}

	in := make(chan BalanceWork, 10)
	for _, w := range []BalanceWork{
		{Address: b, TokenAddress: tok, BlockNumber: 6},
		{Address: a, TokenAddress: tok, BlockNumber: 9},
		{Address: c, TokenAddress: tok2, BlockNumber: 7},
		{Address: a, TokenAddress: tok, BlockNumber: 8},
		{Address: d, TokenAddress: tokErr, BlockNumber: 7},
	} {
		in <- w
	}
	out := p.fetchReader(BalanceWork{Address: a, TokenAddress: tok, BlockNumber: 5}, in)

	if len(rpc.calls) != 3 {
		t.Fatalf("%d reader calls, want 3 (one per token)", len(rpc.calls))
	}
	if got := rpc.calls[0]; len(got) != 3 || got[0] != tok || got[1] != a || got[2] != b {
		t.Fatalf("first call %v, want token 0x01 with a, b once each", got)
	}
	got := map[string]uint64{}
	for _, bal := range out {
		got[bal.Address] = bal.BlockNumber
	}
	if len(got) != 2 || got[a.Hex()] != 9 || got[c.Hex()] != 7 {
		t.Fatalf("written %v, want a at 9 and c at 7 (b failed, d's call failed)", got)
	}
}

// A batched call the node rejects, as a node without eth_call state overrides rejects every one, is logged with its
// token, not only counted: its balances stay stale until their next transfer.
func TestFetchReaderLogsAFailedCall(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Default()
	log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer log.SetDefault(prev)

	tokErr, d := common.HexToAddress("0x03"), common.HexToAddress("0xd")
	rpc := &readerRPC{MockRPCClient: &MockRPCClient{}, errTok: tokErr}
	p := NewBalanceWorkerPool(nil, rpc, 0, 0)
	p.SetReaderBatch(100, false)
	if out := p.fetchReader(BalanceWork{Address: d, TokenAddress: tokErr, BlockNumber: 7}, make(chan BalanceWork)); len(out) != 0 {
		t.Fatalf("written %v, want nothing", out)
	}
	if got := buf.String(); !strings.Contains(got, "batched balance call failed") || !strings.Contains(got, tokErr.Hex()) {
		t.Fatalf("log %q, want the failed call with its token", got)
	}
}
