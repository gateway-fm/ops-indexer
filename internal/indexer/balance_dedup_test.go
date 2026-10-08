package indexer

import (
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

func TestCoalesceFoldsRepeatsAtHighestBlock(t *testing.T) {
	p := NewBalanceWorkerPool(nil, nil, 0, 0)
	p.SetDedupWindow(50 * time.Millisecond)
	p.Start()
	defer p.cancel()

	token := common.HexToAddress("0x01")
	a, b := common.HexToAddress("0xa"), common.HexToAddress("0xb")
	for _, w := range []BalanceWork{
		{Address: a, TokenAddress: token, BlockNumber: 5},
		{Address: b, TokenAddress: token, BlockNumber: 6},
		{Address: a, TokenAddress: token, BlockNumber: 9},
		{Address: a, TokenAddress: token, BlockNumber: 7},
	} {
		p.QueueWork(w)
	}

	got := map[common.Address]uint64{}
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case w := <-p.fetchChan:
			if _, dup := got[w.Address]; dup {
				t.Fatalf("key %s forwarded twice inside one window", w.Address.Hex())
			}
			got[w.Address] = w.BlockNumber
		case <-timeout:
			t.Fatalf("forwarded %d of 2 keys", len(got))
		}
	}
	if got[a] != 9 || got[b] != 6 {
		t.Fatalf("got %v, want a at 9 and b at 6", got)
	}
	if n := p.QueueSize(); n != 0 {
		t.Fatalf("queue size %d after forwarding, want 0", n)
	}

	p.QueueWork(BalanceWork{Address: a, TokenAddress: token, BlockNumber: 12})
	select {
	case w := <-p.fetchChan:
		if w.Address != a || w.BlockNumber != 12 {
			t.Fatalf("got %+v, want a at 12 in a new window", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a key that changes after it was forwarded must open a new window")
	}
}
