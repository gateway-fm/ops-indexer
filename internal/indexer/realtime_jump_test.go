package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/db"
)

func realtimeAt(mockDB *MockDatabase, cursor, maxBehind uint64) *RealtimeIndexer {
	rt := NewRealtimeIndexer(mockDB, new(MockRPCClient), &RealtimeConfig{ConfirmationBlocks: 1}, &Config{RealtimeMaxBehind: maxBehind},
		NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(cursor)
	return rt
}

// On a production deployment, after a long stall realtime walked the backlog one block at a time, slower than the chain, and the
// explorer's head stayed minutes old. Far behind, it jumps to the head as a fresh start does and hands the skipped
// blocks to catch-up.
func TestRealtimeJumpsToTheHeadAfterALongStall(t *testing.T) {
	ctx := context.Background()

	mockDB := new(MockDatabase)
	rt := realtimeAt(mockDB, 10_000, 1_000)
	// Nothing is stored below the cursor here, so the reorg check before the jump has nothing to compare.
	mockDB.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	mockDB.On("SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 10_001, ToNumber: 14_999}}).Return(nil).Once()
	assert.Equal(t, uint64(14_999), rt.jumpIfFarBehind(ctx, 15_000), "the next block realtime writes is the safe head")
	assert.Equal(t, uint64(14_999), rt.GetLastProcessedBlock())
	mockDB.AssertExpectations(t)

	for name, rt := range map[string]*RealtimeIndexer{
		"within the limit": realtimeAt(new(MockDatabase), 14_000, 1_000),
		"jump off":         realtimeAt(new(MockDatabase), 10_000, 0),
	} {
		assert.Equal(t, rt.GetLastProcessedBlock(), rt.jumpIfFarBehind(ctx, 15_000), name)
	}

	failing := new(MockDatabase)
	rt = realtimeAt(failing, 10_000, 1_000)
	failing.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	failing.On("SaveMissingRanges", mock.Anything, mock.Anything).Return(errors.New("down")).Once()
	assert.Equal(t, uint64(10_000), rt.jumpIfFarBehind(ctx, 15_000), "no jump unless catch-up has the skipped blocks")
}

// The closer's head-lag check reads realtime's cursor: during the stall catch-up stored blocks near the node head, the
// highest stored block said "caught up", and the closer ran a span.
func TestIngestHeadIsRealtimesCursor(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockDatabase)
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(20_000), nil)
	idx := NewWithConfig(mockDB, new(MockRPCClient), 0, 0, &Config{})

	head, err := idx.IngestHead(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(20_000), head, "before realtime runs: the highest stored block")

	idx.liveRealtime.Store(realtimeAt(mockDB, 12_000, 0))
	head, err = idx.IngestHead(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12_000), head, "realtime's cursor, not catch-up's highest block")
}
