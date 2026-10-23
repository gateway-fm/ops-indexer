package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/indexer"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// runRefillDailyStats is `indexer refill-daily-stats --from YYYY-MM-DD --to YYYY-MM-DD [--bodies] [--receipts]
// [--rate N] [--plan] [--dry-run]` (PRST-5436): recount whole UTC days of daily_stats from the node, for days whose
// blocks never reached daily_stats and are now pruned, and recompute the history-wide columns of every day.
// --plan only finds each day's blocks and prints the requests a refill would make; --dry-run counts and prints the
// rows it would write. A stopped run resumes from its checkpointed days.
func runRefillDailyStats(args []string, database *db.DB, rpcURL string) int {
	fs := flag.NewFlagSet("refill-daily-stats", flag.ExitOnError)
	from := fs.String("from", "", "first UTC day, YYYY-MM-DD")
	to := fs.String("to", "", "last UTC day, YYYY-MM-DD (inclusive)")
	bodies := fs.Bool("bodies", false, "fetch full transactions: avg_gas_price, active_addresses")
	receipts := fs.Bool("receipts", false, "fetch block receipts: successful/failed, token_transfer_count")
	perSecond := fs.Float64("rate", 10, "batch requests per second (100 calls each)")
	plan := fs.Bool("plan", false, "find each day's blocks and print the cost; count nothing")
	dry := fs.Bool("dry-run", false, "count and print the rows; write nothing")
	_ = fs.Parse(args)

	first, err1 := time.Parse("2006-01-02", *from)
	last, err2 := time.Parse("2006-01-02", *to)
	if err1 != nil || err2 != nil || last.Before(first) {
		log.Error("refill-daily-stats: --from and --to must be YYYY-MM-DD, from <= to")
		return 1
	}
	var days []time.Time
	for d := first; !d.After(last); d = d.Add(24 * time.Hour) {
		days = append(days, d)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := indexer.RefillDailyStats(ctx, database, indexer.RefillNode{C: rpclient.Dial(rpcURL)}, days, indexer.DailyRefillOptions{
		Bodies: *bodies, Receipts: *receipts, BatchesPerSecond: *perSecond, Plan: *plan, Dry: *dry})
	if report != nil {
		var blocks uint64
		for _, d := range report.Days {
			blocks += d.Blocks()
			log.Info("refill-daily-stats: day", "date", d.Date.Format("2006-01-02"), "first_block", d.First,
				"blocks", d.Blocks(), "already_counted", d.FromSaved)
		}
		perBlock := 1
		if *receipts {
			perBlock = 2
		}
		log.Info("refill-daily-stats: requests", "made", report.Requests, "calls", report.Calls,
			"refill_calls", blocks*uint64(perBlock), "refill_batches", (blocks*uint64(perBlock)+99)/100)
		for _, r := range report.Rows {
			fmt.Printf("%s blocks=%d txs=%d gas_used=%d avg_gas_price=%d ok=%d failed=%d active=%d new_addresses=%d "+
				"avg_block_time=%.3f avg_block_size=%d new_contracts=%d token_transfers=%d cum_txs=%d cum_addresses=%d cum_contracts=%d\n",
				r.Date, r.TotalBlocks, r.TotalTransactions, r.TotalGasUsed, r.AvgGasPrice, r.SuccessfulTxs, r.FailedTxs,
				r.ActiveAddresses, r.NewAddresses, r.AvgBlockTime, r.AvgBlockSize, r.NewContracts, r.TokenTransferCount,
				r.CumulativeTransactions, r.CumulativeAddresses, r.CumulativeContracts)
		}
	}
	if err != nil {
		log.Error("refill-daily-stats: stopped; rerun to resume", "error", err)
		return 1
	}
	return 0
}
