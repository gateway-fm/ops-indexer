package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// createsChild is block n with one transaction whose call creates child by CREATE2 (and, under EIP-6780, may
// destroy it in the same transaction, so a later block can create it again at the same address).
func createsChild(n uint64, child string) *BlockData {
	b := pipelineBlock(n)
	tx := b.Transactions[0].Hash
	output := "0x6080"
	b.InternalTransactions = []*types.InternalTransaction{{TxHash: tx, BlockNumber: n, TraceAddress: "0",
		From: "0xb", To: &child, Value: "0", CallType: "create2", Output: &output, Timestamp: &b.Block.Timestamp}}
	b.Contracts = []*types.Contract{{Address: child, Bytecode: "6080", Creator: "0xb", CreationTx: tx, BlockNumber: n}}
	return b
}

// Two blocks in the commit pipeline create the same contract address. The later block may begin first; the earlier
// one's write must not then wait on it, because the later block commits only after the earlier one.
func TestPipelinedBlocksCreatingOneAddressCommitWhenTheLaterBeginsFirst(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	d = withPoolSize(t, d, 16)
	const child = "0x00000000000000000000000000000000000c41d0"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	second, err := d.BeginBlockData(ctx, createsChild(2, child))
	require.NoError(t, err)
	defer second.Rollback(context.Background())

	begun := make(chan error, 1)
	var first *BlockWrite
	go func() {
		w, err := d.BeginBlockData(ctx, createsChild(1, child))
		first = w
		begun <- err
	}()
	select {
	case err := <-begun:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		var waiting int
		_ = d.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting)
		second.Rollback(context.Background())
		<-begun
		if first != nil {
			first.Rollback(context.Background())
		}
		t.Fatalf("block 1's write waited 5 s on block 2's open transaction (%d sessions waiting on a lock)", waiting)
	}
	defer first.Rollback(context.Background())

	require.NoError(t, first.Finish(ctx))
	require.NoError(t, second.Finish(ctx))

	var block int64
	var rows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*), min(block_number) FROM contracts WHERE address = $1`, child).Scan(&rows, &block))
	assert.Equal(t, 1, rows)
	assert.EqualValues(t, 1, block, "the earlier block's creation")
	var blocks int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM blocks`).Scan(&blocks))
	assert.Equal(t, 2, blocks, "both blocks committed")
}

func contractBlocks(t *testing.T, d *DB) map[string]int64 {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT address, block_number FROM contracts`)
	require.NoError(t, err)
	got := map[string]int64{}
	for rows.Next() {
		var a string
		var n int64
		require.NoError(t, rows.Scan(&a, &n))
		got[a] = n
	}
	require.NoError(t, rows.Err())
	return got
}

// The contracts rows move with the block's transaction through every way a write ends: rolled back (a failed
// commit, a crash before Finish), written again after a lost commit acknowledgement, and reverted.
func TestContractsCommitAndRevertWithTheirBlock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const child = "0x00000000000000000000000000000000000c41d0"

	w, err := d.BeginBlockData(ctx, createsChild(1, child))
	require.NoError(t, err)
	w.Rollback(ctx)
	assert.Empty(t, contractBlocks(t, d), "rolled back with the block")

	require.NoError(t, d.InsertBlockDataBatch(ctx, createsChild(1, child)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, createsChild(1, child)), "the block written again after a lost acknowledgement")
	require.NoError(t, d.InsertBlockDataBatch(ctx, createsChild(2, child)))
	assert.Equal(t, map[string]int64{child: 1}, contractBlocks(t, d))

	require.NoError(t, d.DeleteBlock(ctx, 2))
	assert.Equal(t, map[string]int64{child: 1}, contractBlocks(t, d), "block 1's creation outlives block 2's revert")
	require.NoError(t, d.DeleteBlock(ctx, 1))
	assert.Empty(t, contractBlocks(t, d))
}

// One block creating an address twice, in either case, writes one row: the first creation's.
func TestContractsOfABlockAreWrittenOncePerAddress(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	b := createsChild(1, "0x00000000000000000000000000000000000C41D0")
	again := *b.Contracts[0]
	again.Address, again.Bytecode = "0x00000000000000000000000000000000000c41d0", "60aa"
	other := *b.Contracts[0]
	other.Address = "0x00000000000000000000000000000000000c41c0"
	b.Contracts = append(b.Contracts, &again, &other)
	require.NoError(t, d.InsertBlockDataBatch(ctx, b))

	assert.Equal(t, map[string]int64{"0x00000000000000000000000000000000000c41d0": 1, "0x00000000000000000000000000000000000c41c0": 1},
		contractBlocks(t, d), "stored lower case, one row per address")
	var code string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT bytecode FROM contracts WHERE address = '0x00000000000000000000000000000000000c41d0'`).Scan(&code))
	assert.Equal(t, "6080", code, "the first creation's")
}

// Writers finishing at once (catch-up workers, catch-up beside realtime) whose blocks create the same addresses in
// opposite orders take them in one order, so neither waits on the other in a cycle.
func TestConcurrentBlocksCreatingTheSameContractsCommit(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const x, y = "0x00000000000000000000000000000000000c41d1", "0x00000000000000000000000000000000000c41d2"

	for round := uint64(0); round < 10; round++ {
		a, b := createsChild(10+2*round, x), createsChild(11+2*round, y)
		ac, bc := *a.Contracts[0], *b.Contracts[0]
		ac.Address, bc.Address = y, x
		a.Contracts = append(a.Contracts, &ac)
		b.Contracts = append(b.Contracts, &bc)
		wa, err := d.BeginBlockData(ctx, a)
		require.NoError(t, err)
		defer wa.Rollback(context.Background())
		wb, err := d.BeginBlockData(ctx, b)
		require.NoError(t, err)
		defer wb.Rollback(context.Background())
		errs := make(chan error, 2)
		go func() { errs <- wa.Finish(ctx) }()
		go func() { errs <- wb.Finish(ctx) }()
		require.NoError(t, <-errs)
		require.NoError(t, <-errs)
		_, err = d.pool.Exec(ctx, `DELETE FROM contracts`)
		require.NoError(t, err)
	}
}
