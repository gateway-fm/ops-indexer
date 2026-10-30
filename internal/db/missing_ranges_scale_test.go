package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// storeBlocksExcept stores blocks from..to except the given numbers, in one statement.
func storeBlocksExcept(t *testing.T, d *DB, from, to int64, missing []int64) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT n, '0xhash' || n, '0xparent' || n, 1000, 21000, 30000000, 0
		FROM generate_series($1::bigint, $2::bigint) AS n
		WHERE n <> ALL($3::bigint[])`, from, to, missing)
	require.NoError(t, err)
	_, err = d.pool.Exec(context.Background(), `ANALYZE blocks`)
	require.NoError(t, err)
}

var twoMillionGaps = []int64{100, 1_000_000, 1_000_001, 1_000_002, 2_499_999}

// A gap scan over millions of blocks, the size of a forward scan up to realtime after a long stall, finishes in
// seconds and finds exactly the gaps. Excluding the stored blocks with NOT IN did not fit work_mem as a hash and fell
// back to re-reading the stored blocks for every number in the range: it ran for hours, holding back vacuum.
func TestFindMissingBlocksInRangeOverMillionsOfBlocks(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	storeBlocksExcept(t, d, 1, 2_500_000, twoMillionGaps)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	ranges, err := d.FindMissingBlocksInRange(ctx, 1, 2_500_000)
	require.NoError(t, err)
	t.Logf("2.5M blocks scanned in %s", time.Since(start))
	require.Equal(t, []BlockRange{{FromNumber: 2_499_999, ToNumber: 2_499_999}, {FromNumber: 1_000_000, ToNumber: 1_000_002},
		{FromNumber: 100, ToNumber: 100}}, ranges)
}

// The gap scan's plan excludes stored blocks with an anti join, never with a subplan run per number.
func TestFindMissingBlocksInRangePlansAnAntiJoin(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	storeBlocksExcept(t, d, 1, 200_000, nil)

	rows, err := d.pool.Query(context.Background(), `EXPLAIN `+findMissingBlocksSQL, int64(1), int64(2_500_000))
	require.NoError(t, err)
	var plan []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan = append(plan, line)
	}
	require.NoError(t, rows.Err())
	text := strings.Join(plan, "\n")
	require.Contains(t, text, "Anti Join", text)
	require.NotContains(t, text, "SubPlan", text)
}

// A gap scan that runs past its statement timeout is cancelled by the server, so it cannot hold back vacuum, and the
// next scan of the same range succeeds.
func TestFindMissingBlocksInRangeIsCancelledAtItsTimeout(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	storeBlocksExcept(t, d, 1, 2_500_000, twoMillionGaps)

	d.SetMissingScanTimeout(time.Millisecond)
	_, err := d.FindMissingBlocksInRange(context.Background(), 1, 2_500_000)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr) && pgErr.Code == "57014", "want a statement timeout (57014), got %v", err)

	d.SetMissingScanTimeout(0)
	ranges, err := d.FindMissingBlocksInRange(context.Background(), 1, 2_500_000)
	require.NoError(t, err)
	require.Len(t, ranges, 3)
}
