package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// RefillBlock is what a daily_stats refill reads of a block: its header fields, its transaction count and, with
// bodies, each transaction's sender, recipient and gas price.
type RefillBlock struct {
	Number, Timestamp, GasUsed, Size uint64
	TxCount                          int
	Txs                              []RefillTx
}

type RefillTx struct {
	From     string
	To       *string
	GasPrice *big.Int
}

// DailyRefillNode is the node, as the refill reads it; Blocks and BlockReceipts are one batch request each.
type DailyRefillNode interface {
	BlockNumber(ctx context.Context) (uint64, error)
	Blocks(ctx context.Context, numbers []uint64, bodies bool) ([]*RefillBlock, error)
	BlockReceipts(ctx context.Context, numbers []uint64) ([][]*rpclient.Receipt, error)
}

type DailyRefillDB interface {
	SavedRefillDay(ctx context.Context, date time.Time) (saved, bodies, receipts bool, err error)
	SaveRefillDay(ctx context.Context, r db.RefillDay) error
	SavedRefillDays(ctx context.Context) ([]db.RefillDay, error)
	WriteRefilledDailyStats(ctx context.Context, days []db.RefillDay, dry bool) ([]types.DailyStats, error)
}

type DailyRefillOptions struct {
	// Bodies fetches full transactions (avg_gas_price, active_addresses); Receipts adds block receipts
	// (successful/failed, token_transfer_count).
	Bodies, Receipts bool
	// BatchesPerSecond limits the batch requests to the node; Batch is the calls per batch.
	BatchesPerSecond float64
	Batch            int
	// Plan only finds each day's blocks and reports what a refill would cost. Dry counts and reports the rows and
	// writes nothing to daily_stats; it does checkpoint the counted days, so the real run after it reads nothing
	// from the node again.
	Plan, Dry bool
}

// Duration is how long counting blocks takes at the request rate.
func (o DailyRefillOptions) Duration(blocks uint64) time.Duration {
	perBlock := uint64(1)
	if o.Receipts {
		perBlock = 2
	}
	batches := (blocks*perBlock + uint64(o.Batch) - 1) / uint64(o.Batch)
	return time.Duration(float64(batches) / o.BatchesPerSecond * float64(time.Second))
}

type DailyRefillReport struct {
	Days     []DayRange
	Duration time.Duration
	Requests int
	Calls    int
	Rows     []types.DailyStats
}

type DayRange struct {
	Date      time.Time
	First     uint64
	End       uint64 // exclusive
	FromSaved bool
}

func (d DayRange) Blocks() uint64 { return d.End - d.First }

// RefillDailyStats recounts the given UTC days' daily_stats from the node: the days whose blocks catch-up stored
// below the daily_stats watermark, now pruned (PRST-5436). Each day is counted as daily_stats_advance counts stored
// blocks: blocks, transactions, gas, size, the gap to the previous block (the previous day's last for the first),
// and with bodies / receipts the transaction-level parts by ingest's own rules. A counted day is checkpointed, so a
// stopped run resumes; every checkpointed day is written in one transaction at the end.
func RefillDailyStats(ctx context.Context, d DailyRefillDB, node DailyRefillNode, days []time.Time, opts DailyRefillOptions) (*DailyRefillReport, error) {
	if opts.Batch <= 0 {
		opts.Batch = 100
	}
	if opts.BatchesPerSecond <= 0 {
		opts.BatchesPerSecond = 2
	}
	r := &dailyRefill{node: node, opts: opts, limiter: rate.NewLimiter(rate.Limit(opts.BatchesPerSecond), 1), ts: map[uint64]uint64{},
		retryPause: 2 * time.Second}
	head, err := node.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	r.head = head
	report := &DailyRefillReport{}
	var toCount []DayRange
	var blocks uint64
	for _, day := range days {
		day = day.UTC().Truncate(24 * time.Hour)
		if !opts.Plan {
			saved, bodies, receipts, err := d.SavedRefillDay(ctx, day)
			if err != nil {
				return report, err
			}
			if saved && (bodies || !opts.Bodies) && (receipts || !opts.Receipts) {
				report.Days = append(report.Days, DayRange{Date: day, FromSaved: true})
				log.Info("refill-daily-stats: already counted", "day", day.Format("2006-01-02"))
				continue
			}
		}
		first, err := r.firstBlockAtOrAfter(ctx, uint64(day.Unix()))
		if err != nil {
			return report, err
		}
		end, err := r.firstBlockAtOrAfter(ctx, uint64(day.Add(24*time.Hour).Unix()))
		if err != nil {
			return report, err
		}
		if end > head {
			return report, fmt.Errorf("%s is not over at the node's head %d", day.Format("2006-01-02"), head)
		}
		dr := DayRange{Date: day, First: first, End: end}
		report.Days = append(report.Days, dr)
		toCount = append(toCount, dr)
		blocks += dr.Blocks()
	}
	report.Duration = opts.Duration(blocks)
	log.Info("refill-daily-stats: plan", "days_to_count", len(toCount), "blocks", blocks, "batch", opts.Batch,
		"batches_per_second", opts.BatchesPerSecond, "expected_duration", report.Duration.Round(time.Second))
	if opts.Plan {
		report.Requests, report.Calls = r.requests, r.calls
		return report, nil
	}
	for _, dr := range toCount {
		rd, err := r.countDay(ctx, dr)
		if err != nil {
			return report, err
		}
		log.Info("refill-daily-stats: day counted", "day", dr.Date.Format("2006-01-02"), "blocks", rd.Blocks, "txs", rd.Txs)
		if err := d.SaveRefillDay(ctx, rd); err != nil {
			return report, err
		}
	}
	report.Requests, report.Calls = r.requests, r.calls
	counted, err := d.SavedRefillDays(ctx)
	if err != nil {
		return report, err
	}
	report.Rows, err = d.WriteRefilledDailyStats(ctx, counted, opts.Dry)
	return report, err
}

type dailyRefill struct {
	node            DailyRefillNode
	opts            DailyRefillOptions
	limiter         *rate.Limiter
	head            uint64
	ts              map[uint64]uint64
	requests, calls int
	retryPause      time.Duration
}

// withRetry runs one batch request, again after a pause on a 429 or 5xx (a throttling proxy), up to 6 times.
func (r *dailyRefill) withRetry(ctx context.Context, calls int, fn func() error) error {
	pause := r.retryPause
	for attempt := 1; ; attempt++ {
		if err := r.limiter.Wait(ctx); err != nil {
			return err
		}
		r.requests++
		r.calls += calls
		err := fn()
		var status *rpclient.HTTPStatusError
		if err == nil || attempt == 6 || !errors.As(err, &status) || (status.Code != 429 && status.Code < 500) {
			return err
		}
		log.Warn("refill-daily-stats: node refused a batch; retrying", "status", status.Code, "attempt", attempt, "pause", pause)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
		pause *= 2
	}
}

func (r *dailyRefill) blocks(ctx context.Context, numbers []uint64, bodies bool) ([]*RefillBlock, error) {
	var out []*RefillBlock
	err := r.withRetry(ctx, len(numbers), func() (err error) {
		out, err = r.node.Blocks(ctx, numbers, bodies)
		return err
	})
	if err != nil {
		return nil, err
	}
	for i, b := range out {
		if b == nil {
			return nil, fmt.Errorf("the node has no block %d", numbers[i])
		}
		r.ts[b.Number] = b.Timestamp
	}
	return out, nil
}

func (r *dailyRefill) timestamp(ctx context.Context, n uint64) (uint64, error) {
	if ts, ok := r.ts[n]; ok {
		return ts, nil
	}
	b, err := r.blocks(ctx, []uint64{n}, false)
	if err != nil {
		return 0, err
	}
	return b[0].Timestamp, nil
}

// firstBlockAtOrAfter is the first block whose timestamp is at or after ts, or head+1.
func (r *dailyRefill) firstBlockAtOrAfter(ctx context.Context, ts uint64) (uint64, error) {
	lo, hi := uint64(0), r.head+1
	for lo < hi {
		mid := lo + (hi-lo)/2
		t, err := r.timestamp(ctx, mid)
		if err != nil {
			return 0, err
		}
		if t >= ts {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

func (r *dailyRefill) countDay(ctx context.Context, dr DayRange) (db.RefillDay, error) {
	rd := db.RefillDay{Date: dr.Date, FirstBlock: dr.First, LastBlock: dr.End - 1}
	var gasPrice *big.Int
	active := map[string]struct{}{}
	var successful, failed, transfers int64
	if r.opts.Bodies {
		gasPrice = new(big.Int)
	}
	prev := int64(-1)
	if dr.First > 0 {
		t, err := r.timestamp(ctx, dr.First-1)
		if err != nil {
			return rd, err
		}
		prev = int64(t)
	}
	for lo := dr.First; lo < dr.End; lo += uint64(r.opts.Batch) {
		hi := min(lo+uint64(r.opts.Batch), dr.End)
		numbers := make([]uint64, 0, hi-lo)
		for n := lo; n < hi; n++ {
			numbers = append(numbers, n)
		}
		blocks, err := r.blocks(ctx, numbers, r.opts.Bodies)
		if err != nil {
			return rd, err
		}
		for _, b := range blocks {
			rd.Blocks++
			rd.Txs += int64(b.TxCount)
			rd.GasUsed += int64(b.GasUsed)
			rd.SizeSum += int64(b.Size)
			rd.SizeN++
			if prev >= 0 {
				rd.BlockTimeSum += int64(b.Timestamp) - prev
				rd.BlockTimeN++
			}
			prev = int64(b.Timestamp)
			for _, tx := range b.Txs {
				if tx.GasPrice != nil {
					gasPrice.Add(gasPrice, tx.GasPrice)
				}
				active[strings.ToLower(tx.From)] = struct{}{}
				if tx.To != nil {
					active[strings.ToLower(*tx.To)] = struct{}{}
				}
			}
			delete(r.ts, b.Number)
		}
		if r.opts.Receipts {
			var receipts [][]*rpclient.Receipt
			if err := r.withRetry(ctx, len(numbers), func() (err error) {
				receipts, err = r.node.BlockReceipts(ctx, numbers)
				return err
			}); err != nil {
				return rd, err
			}
			for _, block := range receipts {
				for _, rc := range block {
					if rc.Status == 1 {
						successful++
					} else {
						failed++
					}
					for _, lg := range rc.Logs {
						if len(lg.Topics) >= 3 && lg.Topics[0] == transferTopic {
							transfers++
						}
					}
				}
			}
		}
	}
	if r.opts.Bodies {
		n := int64(len(active))
		rd.GasPriceSum, rd.ActiveAddresses = gasPrice, &n
	}
	if r.opts.Receipts {
		rd.SuccessfulTxs, rd.FailedTxs, rd.TokenTransfers = &successful, &failed, &transfers
	}
	return rd, nil
}

// RefillNode is DailyRefillNode over a JSON-RPC client, with batch requests.
type RefillNode struct{ C *rpclient.Client }

type refillBlockJSON struct {
	Number       hexutil.Uint64    `json:"number"`
	Timestamp    hexutil.Uint64    `json:"timestamp"`
	GasUsed      hexutil.Uint64    `json:"gasUsed"`
	Size         hexutil.Uint64    `json:"size"`
	Transactions []json.RawMessage `json:"transactions"`
}

func (n RefillNode) BlockNumber(ctx context.Context) (uint64, error) {
	var h hexutil.Uint64
	err := n.C.CallContext(ctx, &h, "eth_blockNumber")
	return uint64(h), err
}

func (n RefillNode) Blocks(ctx context.Context, numbers []uint64, bodies bool) ([]*RefillBlock, error) {
	args := make([][]any, len(numbers))
	raw := make([]*refillBlockJSON, len(numbers))
	results := make([]any, len(numbers))
	for i, num := range numbers {
		args[i] = []any{fmt.Sprintf("0x%x", num), bodies}
		results[i] = &raw[i]
	}
	if err := n.C.BatchCallContext(ctx, "eth_getBlockByNumber", args, results); err != nil {
		return nil, err
	}
	out := make([]*RefillBlock, len(numbers))
	for i, b := range raw {
		if b == nil {
			continue
		}
		rb := &RefillBlock{Number: uint64(b.Number), Timestamp: uint64(b.Timestamp), GasUsed: uint64(b.GasUsed),
			Size: uint64(b.Size), TxCount: len(b.Transactions)}
		if bodies {
			for _, t := range b.Transactions {
				var tx struct {
					From     common.Address  `json:"from"`
					To       *common.Address `json:"to"`
					GasPrice *hexutil.Big    `json:"gasPrice"`
				}
				if err := json.Unmarshal(t, &tx); err != nil {
					return nil, fmt.Errorf("block %d: %w", rb.Number, err)
				}
				rt := RefillTx{From: tx.From.Hex(), GasPrice: new(big.Int)}
				if tx.To != nil {
					to := tx.To.Hex()
					rt.To = &to
				}
				if tx.GasPrice != nil {
					rt.GasPrice = new(big.Int).SetUint64(tx.GasPrice.ToInt().Uint64())
				}
				rb.Txs = append(rb.Txs, rt)
			}
		}
		out[i] = rb
	}
	return out, nil
}

func (n RefillNode) BlockReceipts(ctx context.Context, numbers []uint64) ([][]*rpclient.Receipt, error) {
	args := make([][]any, len(numbers))
	out := make([][]*rpclient.Receipt, len(numbers))
	results := make([]any, len(numbers))
	for i, num := range numbers {
		args[i] = []any{fmt.Sprintf("0x%x", num)}
		results[i] = &out[i]
	}
	return out, n.C.BatchCallContext(ctx, "eth_getBlockReceipts", args, results)
}

// RecomputeDailyStats writes the checkpointed days again and recounts every day's history-wide columns, without the
// node: for after a fix to the recount, or after address_stats changed (a repair, a contracts backfill).
func RecomputeDailyStats(ctx context.Context, d DailyRefillDB, dry bool) ([]types.DailyStats, error) {
	days, err := d.SavedRefillDays(ctx)
	if err != nil {
		return nil, err
	}
	return d.WriteRefilledDailyStats(ctx, days, dry)
}
