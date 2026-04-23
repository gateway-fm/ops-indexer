package indexer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMissingRangeCollector_InitialScan(t *testing.T) {
	tests := []struct {
		name           string
		firstBlock     uint64
		lastBlock      uint64
		batchSize      uint64
		missingRanges  map[string][]db.BlockRange // key: "from-to"
		expectSaveCalls int
		expectSavedRanges []db.BlockRange
	}{
		{
			name:       "gap_at_start_1_to_9",
			firstBlock: 0,
			lastBlock:  142,
			batchSize:  200, // single batch covers 0-142
			missingRanges: map[string][]db.BlockRange{
				"0-142": {{FromNumber: 1, ToNumber: 9}},
			},
			expectSaveCalls:   1,
			expectSavedRanges: []db.BlockRange{{FromNumber: 1, ToNumber: 9}},
		},
		{
			name:       "gap_in_middle_50_to_60",
			firstBlock: 0,
			lastBlock:  142,
			batchSize:  200,
			missingRanges: map[string][]db.BlockRange{
				"0-142": {{FromNumber: 50, ToNumber: 60}},
			},
			expectSaveCalls:   1,
			expectSavedRanges: []db.BlockRange{{FromNumber: 50, ToNumber: 60}},
		},
		{
			name:       "multiple_gaps",
			firstBlock: 0,
			lastBlock:  142,
			batchSize:  200,
			missingRanges: map[string][]db.BlockRange{
				"0-142": {{FromNumber: 1, ToNumber: 9}, {FromNumber: 100, ToNumber: 110}},
			},
			expectSaveCalls:   1,
			expectSavedRanges: []db.BlockRange{{FromNumber: 1, ToNumber: 9}, {FromNumber: 100, ToNumber: 110}},
		},
		{
			name:       "no_gaps_all_blocks_present",
			firstBlock: 0,
			lastBlock:  142,
			batchSize:  200,
			missingRanges: map[string][]db.BlockRange{
				"0-142": {}, // empty = no missing blocks
			},
			expectSaveCalls: 0,
		},
		{
			name:       "empty_db_all_blocks_missing",
			firstBlock: 0,
			lastBlock:  142,
			batchSize:  200,
			missingRanges: map[string][]db.BlockRange{
				"0-142": {{FromNumber: 0, ToNumber: 142}},
			},
			expectSaveCalls:   1,
			expectSavedRanges: []db.BlockRange{{FromNumber: 0, ToNumber: 142}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := new(MockDatabase)
			mockRPC := new(MockRPCClient)

			cfg := &MissingRangeCollectorConfig{
				BatchSize:            tt.batchSize,
				BackwardScanInterval: 1 * time.Millisecond,
				ForwardScanInterval:  1 * time.Millisecond,
				FirstBlock:           tt.firstBlock,
				LastBlock:            tt.lastBlock,
			}

			collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)

			// Set up FindMissingBlocksInRange expectations based on batch boundaries
			for fromBlock := tt.firstBlock; fromBlock <= tt.lastBlock; fromBlock += tt.batchSize {
				toBlock := fromBlock + tt.batchSize - 1
				if toBlock > tt.lastBlock {
					toBlock = tt.lastBlock
				}
				key := fmt.Sprintf("%d-%d", fromBlock, toBlock)
				ranges, ok := tt.missingRanges[key]
				if !ok {
					ranges = []db.BlockRange{}
				}
				mockDB.On("FindMissingBlocksInRange", mock.Anything, fromBlock, toBlock).Return(ranges, nil).Once()
			}

			// Set up SaveMissingRanges expectations
			if tt.expectSaveCalls > 0 {
				mockDB.On("SaveMissingRanges", mock.Anything, tt.expectSavedRanges).Return(nil).Once()
			}

			// GetTotalMissingBlocks is called at end of initialScan
			totalMissing := int64(0)
			for _, ranges := range tt.missingRanges {
				for _, r := range ranges {
					totalMissing += int64(r.ToNumber - r.FromNumber + 1)
				}
			}
			mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(totalMissing, nil).Once()

			err := collector.initialScan(context.Background(), tt.lastBlock)
			assert.NoError(t, err)
			assert.True(t, collector.initialScanDone)

			mockDB.AssertExpectations(t)

			// Verify SaveMissingRanges was NOT called when no gaps
			if tt.expectSaveCalls == 0 {
				mockDB.AssertNotCalled(t, "SaveMissingRanges", mock.Anything, mock.Anything)
			}
		})
	}
}

func TestMissingRangeCollector_Start_FreshDB(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	chainHead := uint64(142)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            200,
		BackwardScanInterval: 100 * time.Millisecond,
		ForwardScanInterval:  100 * time.Millisecond,
		FirstBlock:           0,
		LastBlock:            0, // use chain head
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)

	// BlockNumber returns chain head
	mockRPC.On("BlockNumber", mock.Anything).Return(chainHead, nil)

	// No saved progress
	mockDB.On("GetIndexerProgress", mock.Anything).Return(nil, fmt.Errorf("not found"))

	// GetMinMaxIndexedBlocks: empty DB, no blocks indexed
	mockDB.On("GetMinMaxIndexedBlocks", mock.Anything).Return(uint64(0), uint64(0), nil)

	// Since maxIndexed == 0, minFetchedBlock = chainHead = 142, maxFetchedBlock = 142
	mockDB.On("UpdateIndexerProgress", mock.Anything, chainHead, chainHead, false).Return(nil).Once()

	// initialScan from 0 to 142 — all missing (fresh DB)
	mockDB.On("FindMissingBlocksInRange", mock.Anything, uint64(0), chainHead).
		Return([]db.BlockRange{{FromNumber: 0, ToNumber: 142}}, nil).Once()
	mockDB.On("SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 0, ToNumber: 142}}).Return(nil).Once()
	mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(143), nil).Once()

	// Allow background goroutine calls
	mockDB.On("UpdateIndexerProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("FindMissingBlocksInRange", mock.Anything, mock.Anything, mock.Anything).Return([]db.BlockRange{}, nil).Maybe()

	err := collector.Start(context.Background())
	assert.NoError(t, err)

	// Verify initial state: minFetchedBlock should be chainHead (fresh DB case)
	minFetched, maxFetched, head, _ := collector.GetProgress()
	assert.Equal(t, chainHead, head)
	// minFetchedBlock and maxFetchedBlock start at chainHead for fresh DB
	// but the goroutines may have already modified them, so just check they're set
	assert.True(t, minFetched <= chainHead)
	assert.True(t, maxFetched >= chainHead || maxFetched == chainHead)

	collector.Stop()
	mockDB.AssertExpectations(t)
}

func TestMissingRangeCollector_Start_WithProgress(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	chainHead := uint64(142)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            200,
		BackwardScanInterval: 100 * time.Millisecond,
		ForwardScanInterval:  100 * time.Millisecond,
		FirstBlock:           0,
		LastBlock:            0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)

	mockRPC.On("BlockNumber", mock.Anything).Return(chainHead, nil)

	// Saved progress: min=0, max=142, backfillComplete=true
	mockDB.On("GetIndexerProgress", mock.Anything).Return(&db.IndexerProgress{
		MinFetchedBlock:  0,
		MaxFetchedBlock:  142,
		BackfillComplete: true,
	}, nil)

	// initialScan still runs with restored boundaries — no gaps remaining
	mockDB.On("FindMissingBlocksInRange", mock.Anything, uint64(0), chainHead).
		Return([]db.BlockRange{}, nil).Once()
	mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(0), nil).Once()

	// Allow background goroutine calls
	mockDB.On("UpdateIndexerProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("FindMissingBlocksInRange", mock.Anything, mock.Anything, mock.Anything).Return([]db.BlockRange{}, nil).Maybe()

	err := collector.Start(context.Background())
	assert.NoError(t, err)

	// Verify state was restored
	minFetched, maxFetched, _, backfillComplete := collector.GetProgress()
	assert.Equal(t, uint64(0), minFetched)
	assert.Equal(t, uint64(142), maxFetched)
	assert.True(t, backfillComplete)

	collector.Stop()
}

// TestMissingRangeCollector_Start_WithMinBlockZero_StillFindsGaps is the CRITICAL scenario.
//
// When block 0 is indexed but blocks 1-9 are not:
//   - GetMinMaxIndexedBlocks returns (0, 142)
//   - The "else" branch sets minFetchedBlock=0, maxFetchedBlock=142
//   - backwardScanner sees minFetchedBlock(0) <= FirstBlock(0) and returns immediately
//   - BUT initialScan should still find [1,9] as a missing range
//
// This test verifies that initialScan correctly detects gaps even when
// minFetchedBlock is already at the genesis block.
func TestMissingRangeCollector_Start_WithMinBlockZero_StillFindsGaps(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	chainHead := uint64(142)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            200,
		BackwardScanInterval: 100 * time.Millisecond,
		ForwardScanInterval:  100 * time.Millisecond,
		FirstBlock:           0,
		LastBlock:            0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)

	mockRPC.On("BlockNumber", mock.Anything).Return(chainHead, nil)

	// No saved progress
	mockDB.On("GetIndexerProgress", mock.Anything).Return(nil, fmt.Errorf("not found"))

	// Block 0 is indexed, block 142 is max — but blocks 1-9 are missing
	mockDB.On("GetMinMaxIndexedBlocks", mock.Anything).Return(uint64(0), uint64(142), nil)

	// Since maxIndexed(142) > 0, minFetchedBlock=0, maxFetchedBlock=142
	mockDB.On("UpdateIndexerProgress", mock.Anything, uint64(0), uint64(142), false).Return(nil).Once()

	// CRITICAL: initialScan must scan [0,142] and find the gap at [1,9]
	mockDB.On("FindMissingBlocksInRange", mock.Anything, uint64(0), uint64(142)).
		Return([]db.BlockRange{{FromNumber: 1, ToNumber: 9}}, nil).Once()
	mockDB.On("SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 1, ToNumber: 9}}).Return(nil).Once()
	mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(9), nil).Once()

	// Allow background goroutine calls
	mockDB.On("UpdateIndexerProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("FindMissingBlocksInRange", mock.Anything, mock.Anything, mock.Anything).Return([]db.BlockRange{}, nil).Maybe()

	err := collector.Start(context.Background())
	assert.NoError(t, err)

	// Verify: FindMissingBlocksInRange WAS called
	mockDB.AssertCalled(t, "FindMissingBlocksInRange", mock.Anything, uint64(0), uint64(142))
	// Verify: SaveMissingRanges WAS called with the gap
	mockDB.AssertCalled(t, "SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 1, ToNumber: 9}})

	collector.Stop()
	mockDB.AssertExpectations(t)
}

func TestScanBackward_StopsWhenAtFirstBlock(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            100000,
		BackwardScanInterval: 1 * time.Millisecond,
		ForwardScanInterval:  1 * time.Millisecond,
		FirstBlock:           0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)
	collector.minFetchedBlock = 0 // Already at genesis

	// scanBackward should return immediately, no DB calls
	collector.scanBackward()

	// Verify NO DB calls were made
	mockDB.AssertNotCalled(t, "FindMissingBlocksInRange", mock.Anything, mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "SaveMissingRanges", mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "UpdateIndexerProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestScanBackward_FindsMissingRangesAndUpdatesProgress(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            100000,
		BackwardScanInterval: 1 * time.Millisecond,
		ForwardScanInterval:  1 * time.Millisecond,
		FirstBlock:           0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)
	collector.minFetchedBlock = 10
	collector.maxFetchedBlock = 142

	// scanBackward should scan from 0 to 9 (minFetchedBlock - 1)
	mockDB.On("FindMissingBlocksInRange", mock.Anything, uint64(0), uint64(9)).
		Return([]db.BlockRange{{FromNumber: 3, ToNumber: 5}}, nil).Once()
	mockDB.On("SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 3, ToNumber: 5}}).Return(nil).Once()
	// After scan, minFetchedBlock should be updated to fromBlock (0)
	mockDB.On("UpdateIndexerProgress", mock.Anything, uint64(0), uint64(142), false).Return(nil).Once()

	collector.scanBackward()

	assert.Equal(t, uint64(0), collector.minFetchedBlock)
	mockDB.AssertExpectations(t)
}

func TestScanForward_ExtendsScanWhenChainAdvances(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            100000,
		BackwardScanInterval: 1 * time.Millisecond,
		ForwardScanInterval:  1 * time.Millisecond,
		FirstBlock:           0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)
	collector.maxFetchedBlock = 100
	collector.minFetchedBlock = 0

	newChainHead := uint64(150)
	mockRPC.On("BlockNumber", mock.Anything).Return(newChainHead, nil).Once()

	// Should scan from 101 to 150
	mockDB.On("FindMissingBlocksInRange", mock.Anything, uint64(101), uint64(150)).
		Return([]db.BlockRange{{FromNumber: 120, ToNumber: 130}}, nil).Once()
	mockDB.On("SaveMissingRanges", mock.Anything, []db.BlockRange{{FromNumber: 120, ToNumber: 130}}).Return(nil).Once()
	mockDB.On("UpdateIndexerProgress", mock.Anything, uint64(0), uint64(150), false).Return(nil).Once()

	collector.scanForward()

	assert.Equal(t, uint64(150), collector.maxFetchedBlock)
	assert.Equal(t, newChainHead, collector.chainHead)
	mockDB.AssertExpectations(t)
}

func TestScanForward_NoopWhenChainNotAdvanced(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &MissingRangeCollectorConfig{
		BatchSize:            100000,
		BackwardScanInterval: 1 * time.Millisecond,
		ForwardScanInterval:  1 * time.Millisecond,
		FirstBlock:           0,
	}

	collector := NewMissingRangeCollector(mockDB, mockRPC, cfg)
	collector.maxFetchedBlock = 150

	// Chain head hasn't advanced
	mockRPC.On("BlockNumber", mock.Anything).Return(uint64(150), nil).Once()

	collector.scanForward()

	// No scan should happen
	mockDB.AssertNotCalled(t, "FindMissingBlocksInRange", mock.Anything, mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "SaveMissingRanges", mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "UpdateIndexerProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
