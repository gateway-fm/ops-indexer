package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/config"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func pipelineBlock(n uint64) *BlockData {
	to := "0xb"
	return &BlockData{
		Block: &types.Block{Number: n, Hash: fmt.Sprintf("b%d", n), ParentHash: fmt.Sprintf("b%d", n-1), Timestamp: 1_700_000_000 + n},
		Transactions: []*types.Transaction{{Hash: fmt.Sprintf("t%d", n), BlockNumber: n, From: "0xa", To: &to,
			Value: "0", Status: 1}},
		SkipAddressStats: true,
	}
}

// runPipelineWithLaterBlocksFirst opens the writes of blocks 2..depth first, so they hold their connections, while
// the rest of the indexer holds beside connections, and then opens block 1 and finishes every block in order, as
// the pipelined loop does. It returns the error that stopped it.
func runPipelineWithLaterBlocksFirst(t *testing.T, d *DB, depth, beside int) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < beside; i++ {
		c, err := d.pool.Acquire(ctx)
		require.NoError(t, err)
		defer c.Release()
	}

	writes := make([]*BlockWrite, depth+1)
	defer func() {
		for _, w := range writes {
			if w != nil {
				w.Rollback(context.Background())
			}
		}
	}()
	errs := make(chan error, depth)
	for n := 2; n <= depth; n++ {
		go func(n int) {
			w, err := d.BeginBlockData(ctx, pipelineBlock(uint64(n)))
			writes[n] = w
			errs <- err
		}(n)
	}
	for n := 2; n <= depth; n++ {
		if err := <-errs; err != nil {
			return err
		}
	}

	first, err := d.BeginBlockData(ctx, pipelineBlock(1))
	if err != nil {
		return err
	}
	writes[1] = first
	for n := 1; n <= depth; n++ {
		if err := writes[n].Finish(ctx); err != nil {
			return err
		}
		writes[n] = nil
	}
	return nil
}

func withPoolSize(t *testing.T, d *DB, maxConns int) *DB {
	t.Helper()
	cfg := d.pool.Config()
	cfg.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &DB{pool: pool}
}

// At the deepest COMMIT_PIPELINE the startup check allows, the pipeline commits every block even when the later
// blocks take their connections first and the rest of the indexer holds all of its own. One connection fewer and
// the first block never gets one: the hang the check refuses to start into.
func TestPipelineAtTheAllowedDepthCommitsWhenLaterBlocksTakeConnectionsFirst(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := config.Config{RPCURL: "http://node", APIPort: 8080, RPCWorkers: 1, RPCRateLimit: 1,
		EnableAsyncBalance: true, BalanceWorkers: 3, CommitPipelineDepth: 6}
	beside := cfg.ConnectionsBesidePipeline()
	maxConns := cfg.CommitPipelineDepth + beside
	sep := "?"
	if strings.Contains(d.pool.Config().ConnString(), "?") {
		sep = "&"
	}
	cfg.DatabaseURL = d.pool.Config().ConnString() + sep + "pool_max_conns=" + strconv.Itoa(maxConns)
	require.NoError(t, cfg.Validate(), "the depth under test must be the one the check allows")
	cfg.CommitPipelineDepth++
	require.Error(t, cfg.Validate(), "one block deeper must be refused")
	cfg.CommitPipelineDepth--

	require.NoError(t, runPipelineWithLaterBlocksFirst(t, withPoolSize(t, d, maxConns), cfg.CommitPipelineDepth, beside))
	var stored int
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT count(*) FROM blocks`).Scan(&stored))
	require.Equal(t, cfg.CommitPipelineDepth, stored, "blocks committed")

	err := runPipelineWithLaterBlocksFirst(t, withPoolSize(t, d, maxConns-1), cfg.CommitPipelineDepth, beside)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "with one connection fewer the first block waits for one: %v", err)
}
