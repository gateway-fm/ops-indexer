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
