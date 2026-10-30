package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// While a long transaction holds a history table (a consumer reading the database directly), the cut
// does not request its ACCESS EXCLUSIVE lock, so ingest's inserts do not queue behind the request.
func TestCutDoesNotQueueIngestBehindALongReader(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 4_001, 1)

	reader, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer reader.Rollback(ctx)
	var n int64
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM blocks`).Scan(&n))
	time.Sleep(1200 * time.Millisecond)

	cutCtx, cancel := context.WithCancel(ctx)
	cut := make(chan error, 1)
	go func() { cut <- d.retentionPass(cutCtx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}) }()
	defer func() { cancel(); <-cut }()

	var slowest time.Duration
	for b, end := uint64(4_002), time.Now().Add(3*time.Second); time.Now().Before(end); b++ {
		start := time.Now()
		require.NoError(t, insertCutTestBlock(d, b))
		slowest = max(slowest, time.Since(start))
	}
	require.Less(t, slowest, time.Second, "an insert waited behind the cut's lock request")
}
