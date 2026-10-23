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

// runBackfillContracts is `indexer backfill-contracts --csv <file | -> [--rate 5]`: the one-off PRST-5426 backfill
// of contracts deployed by other contracts. It uses the indexer's own config (DATABASE_URL, RPC_URL), runs no
// migrations, and is safe to run next to the live indexer and to rerun. "-" reads the list from stdin: the image is
// distroless, so `kubectl exec -i ... < list.csv` is how a list reaches it.
func runBackfillContracts(args []string, database *db.DB, rpcURL string) int {
	fs := flag.NewFlagSet("backfill-contracts", flag.ExitOnError)
	csvPath := fs.String("csv", "", "address,creation_tx_hash,block_number rows of internal creations to trace (- for stdin)")
	perSecond := fs.Float64("rate", 5, "node requests per second")
	noTrace := fs.Bool("no-trace", false, "build listed rows without debug_traceTransaction (receipt, current code, creator from the list or the tx's to), for a node that cannot trace old blocks")
	_ = fs.Parse(args)

	var listed []indexer.Creation
	if *csvPath != "" {
		f := os.Stdin
		if *csvPath != "-" {
			var err error
			if f, err = os.Open(*csvPath); err != nil {
				log.Error("backfill-contracts: open the list", "error", err)
				return 1
			}
			defer f.Close()
		}
		var err error
		listed, err = indexer.ReadCreationsCSV(f)
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
	report, err := indexer.BackfillContracts(ctx, database, node, listed, indexer.ContractsBackfillOptions{PerSecond: *perSecond, WithoutTraces: *noTrace})
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
