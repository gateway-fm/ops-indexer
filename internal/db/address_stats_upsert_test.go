package db

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func addressStatsWrite(block uint64, deltas ...*AddressStatsDelta) *BlockData {
	stats := make(map[string]*AddressStatsDelta, len(deltas))
	for _, d := range deltas {
		d.BlockNumber = block
		stats[d.Address] = d
	}
	return &BlockData{AddressStats: stats}
}

type addressStatsRow struct {
	txs, transfers, internal, firstSeen, lastSeen int64
	isContract                                    bool
}

func readAddressStats(t *testing.T, d *DB, address string) addressStatsRow {
	t.Helper()
	var r addressStatsRow
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT tx_count, token_transfer_count, internal_tx_count, first_seen, last_seen, is_contract
		FROM address_stats WHERE address = $1`, address).
		Scan(&r.txs, &r.transfers, &r.internal, &r.firstSeen, &r.lastSeen, &r.isContract))
	return r
}

// The head path and catch-up (or the stats backfill) write the same address in either block order: the row must
// come out the same.
func TestAddressStatsUpsertIsOrderIndependent(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	newer := func() *BlockData {
		return addressStatsWrite(10, &AddressStatsDelta{Address: "0xa1", TxCountDelta: 2, IsContract: true}, &AddressStatsDelta{Address: "0xb2", TxCountDelta: 1})
	}
	older := func() *BlockData {
		return addressStatsWrite(5, &AddressStatsDelta{Address: "0xa1", TokenTransferDelta: 3}, &AddressStatsDelta{Address: "0xb2", InternalTxCountDelta: 4})
	}

	require.NoError(t, d.InsertBlockDataBatch(ctx, newer()))
	require.NoError(t, d.InsertBlockDataBatch(ctx, older()))
	headFirst := [2]addressStatsRow{readAddressStats(t, d, "0xa1"), readAddressStats(t, d, "0xb2")}

	_, err := d.pool.Exec(ctx, `TRUNCATE address_stats`)
	require.NoError(t, err)
	require.NoError(t, d.InsertBlockDataBatch(ctx, older()))
	require.NoError(t, d.InsertBlockDataBatch(ctx, newer()))
	oldFirst := [2]addressStatsRow{readAddressStats(t, d, "0xa1"), readAddressStats(t, d, "0xb2")}

	require.Equal(t, headFirst, oldFirst)
	require.Equal(t, addressStatsRow{txs: 2, transfers: 3, firstSeen: 5, lastSeen: 10, isContract: true}, oldFirst[0],
		"first_seen is the oldest block, last_seen the newest, and a contract stays a contract")
}

func TestAddressStatsConcurrentWritersDoNotDeadlock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	shared := make([]string, 40)
	for i := range shared {
		shared[i] = fmt.Sprintf("0x%040x", i)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4*25)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				deltas := make([]*AddressStatsDelta, len(shared))
				for k, addr := range shared {
					deltas[k] = &AddressStatsDelta{Address: addr, TxCountDelta: 1}
				}
				errs <- d.InsertBlockDataBatch(ctx, addressStatsWrite(uint64(w*100+i), deltas...))
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(100), readAddressStats(t, d, shared[0]).txs)
}
