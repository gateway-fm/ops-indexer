package indexer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// A contract whose balanceOf returns fewer than 32 bytes yields no balance. The worker must skip it: a nil entry in
// the batch panics in the flush and takes the indexer down, and it panics again on the same token after a restart.
func TestWorkerSkipsTokenWithShortBalanceResponse(t *testing.T) {
	bad, good := common.HexToAddress("0xbad"), common.HexToAddress("0x01")
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	mockRPC.On("CallContract", mock.Anything, bad, mock.Anything).Return([]byte{0x01, 0x02, 0x03, 0x04}, nil)
	mockRPC.On("CallContract", mock.Anything, good, mock.Anything).Return(make([]byte, 32), nil)
	flushed := make(chan []*types.Balance, 1)
	mockDB.On("InsertBalancesBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		flushed <- append([]*types.Balance(nil), args.Get(1).([]*types.Balance)...)
	}).Return(nil)

	p := NewBalanceWorkerPool(mockDB, mockRPC, 1, 0)
	p.Start()
	defer p.Stop()
	p.QueueWork(BalanceWork{Address: common.HexToAddress("0xa1"), TokenAddress: bad, BlockNumber: 1})
	p.QueueWork(BalanceWork{Address: common.HexToAddress("0xa2"), TokenAddress: good, BlockNumber: 1})

	select {
	case batch := <-flushed:
		if len(batch) != 1 || batch[0] == nil || batch[0].TokenAddress != good.Hex() {
			t.Fatalf("flushed %v; want only the good token's balance", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no flush")
	}
}
