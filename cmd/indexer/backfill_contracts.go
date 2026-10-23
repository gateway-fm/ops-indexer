package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/indexer"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// runBackfillContracts is `indexer backfill-contracts --csv <file> [--rate 5]`: the one-off PRST-5426 backfill of
// contracts deployed by other contracts. It uses the indexer's own config (DATABASE_URL, RPC_URL), runs no
// migrations, and is safe to run next to the live indexer and to rerun.
func runBackfillContracts(args []string, database *db.DB, rpcURL string) int {
	fs := flag.NewFlagSet("backfill-contracts", flag.ExitOnError)
	csvPath := fs.String("csv", "", "address,creation_tx_hash,block_number rows of internal creations to trace")
	perSecond := fs.Float64("rate", 5, "node requests per second")
	_ = fs.Parse(args)

	var listed []indexer.Creation
	if *csvPath != "" {
		f, err := os.Open(*csvPath)
		if err != nil {
			log.Error("backfill-contracts: open the list", "error", err)
			return 1
		}
		listed, err = indexer.ReadCreationsCSV(f)
		f.Close()
		if err != nil {
			log.Error("backfill-contracts: read the list", "error", err)
			return 1
		}
	}
	node, err := rpc.New(rpcURL)
	if err != nil {
		log.Error("backfill-contracts: rpc client", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := indexer.BackfillContracts(ctx, database, node, listed, *perSecond)
	log.Info("backfill-contracts: done",
		"from_internal_transactions", report.FromInternalTransactions, "listed", len(listed),
		"txs_traced", report.TxsTraced, "txs_skipped_complete", report.TxsSkipped, "written", report.Written,
		"not_created_by_trace", len(report.NotCreated), "unlisted_created", len(report.Unlisted), "txs_failed", len(report.Failed))
	for _, a := range report.NotCreated {
		log.Warn("backfill-contracts: listed, but the trace of its transaction did not create it", "address", a)
	}
	if err != nil {
		log.Error("backfill-contracts: stopped; rerun to resume", "error", err)
		return 1
	}
	if len(report.Failed) > 0 {
		return 2
	}
	return 0
}
