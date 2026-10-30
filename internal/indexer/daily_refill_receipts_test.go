package indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// lackingChain answers block 7's receipts (three transactions) as null, or with the first one only, for the first
// lacking batches that ask for it.
type lackingChain struct {
	*refillChain
	partial bool
	lacking int
}

func (c *lackingChain) BlockReceipts(ctx context.Context, numbers []uint64) ([][]*rpclient.Receipt, error) {
	out, err := c.refillChain.BlockReceipts(ctx, numbers)
	for i, n := range numbers {
		if n == 7 && c.lacking > 0 {
			c.lacking--
			if c.partial {
				out[i] = out[i][:1]
			} else {
				out[i] = nil
			}
		}
	}
	return out, err
}

// refillCheckpoints is a DailyRefillDB that records the days checkpointed.
type refillCheckpoints struct {
	mu    sync.Mutex
	saved []db.RefillDay
}

func (r *refillCheckpoints) SavedRefillDay(context.Context, time.Time) (bool, bool, bool, error) {
	return false, false, false, nil
}

func (r *refillCheckpoints) SaveRefillDay(_ context.Context, d db.RefillDay) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = append(r.saved, d)
	return nil
}

func (r *refillCheckpoints) SavedRefillDays(context.Context) ([]db.RefillDay, error) { return nil, nil }

func (r *refillCheckpoints) WriteRefilledDailyStats(context.Context, []db.RefillDay, bool) ([]types.DailyStats, error) {
	return nil, nil
}

func TestRefillDoesNotCountADayWithoutAllItsReceipts(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "null"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			full := newRefillChain()
			require.Equal(t, 3, full.node[7].TxCount)
			newRefill := func(chain DailyRefillNode) *dailyRefill {
				r := &dailyRefill{node: chain, opts: DailyRefillOptions{Batch: 10, Receipts: true}, limiter: rate.NewLimiter(rate.Inf, 1),
					ts: map[uint64]uint64{}, retryPause: time.Millisecond}
				r.head = uint64(len(full.blocks))
				return r
			}
			want, err := newRefill(full).countDay(context.Background(), DayRange{Date: full.day0, First: 5, End: 40})
			require.NoError(t, err)

			chain := &lackingChain{refillChain: full, partial: partial, lacking: 2}
			got, err := newRefill(chain).countDay(context.Background(), DayRange{Date: full.day0, First: 5, End: 40})
			require.NoError(t, err, "the node answered block 7's receipts on the third try")
			require.Zero(t, chain.lacking)
			require.Equal(t, *want.SuccessfulTxs, *got.SuccessfulTxs)
			require.Equal(t, *want.FailedTxs, *got.FailedTxs)
			require.Equal(t, *want.TokenTransfers, *got.TokenTransfers)

			chain.lacking = 100
			_, err = newRefill(chain).countDay(context.Background(), DayRange{Date: full.day0, First: 5, End: 40})
			require.ErrorIs(t, err, ErrRefillReceiptsIncomplete, "block 7 never gets all its receipts: the day is not counted")

			chain.lacking = 100
			saved := &refillCheckpoints{}
			_, err = RefillDailyStats(context.Background(), saved, chain, []time.Time{full.day(7)},
				DailyRefillOptions{Receipts: true, BatchesPerSecond: 1e6, Batch: 7})
			require.ErrorIs(t, err, ErrRefillReceiptsIncomplete)
			require.Empty(t, saved.saved, "the day is not checkpointed, so a rerun counts it again")
		})
	}
}
