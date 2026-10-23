package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A read that fails when retention starts is retried, and partitions are still kept
// ahead of the head.
func TestRetentionRetriesAFailedStartupRead(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 0, PartitionBlocks: 2_000, Interval: 50 * time.Millisecond,
		EnsureInterval: 50 * time.Millisecond}
	seedHistory(t, d, 0, 4_001, 1)
	require.NoError(t, d.retentionPass(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}))

	_, err := d.pool.Exec(ctx, `ALTER TABLE history_partitions RENAME TO hp_away`)
	require.NoError(t, err)
	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go d.RunRetention(runCtx, cfg)
	time.Sleep(300 * time.Millisecond)
	_, err = d.pool.Exec(ctx, `ALTER TABLE hp_away RENAME TO history_partitions`)
	require.NoError(t, err)

	var top uint64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT max(hi) FROM history_partitions`).Scan(&top))
	require.NoError(t, insertCutTestBlock(d, top-1))
	time.Sleep(time.Second)
	require.NoError(t, insertCutTestBlock(d, top), "partitions kept ahead of the head")
}

// Partitions are kept ahead of the node head too, so a block realtime writes at the node head after a
// jump has a partition to land in.
func TestPartitionsAreKeptAheadOfTheNodeHead(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 4_001, 1)
	require.NoError(t, d.retentionPass(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}))
	stored, err := d.GetLatestBlockNumber(ctx)
	require.NoError(t, err)
	node := stored + 3*2_000 + 1_000

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go d.RunRetention(runCtx, RetentionConfig{RetainBlocks: 0, PartitionBlocks: 2_000, Interval: time.Hour,
		EnsureInterval: 50 * time.Millisecond, NodeHead: func(context.Context) (uint64, error) { return node, nil }})
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, insertCutTestBlock(d, node), "a block at the node head has a partition")
}
