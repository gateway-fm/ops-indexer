package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

// runRepairAddressStats is `indexer repair-address-stats [--batch N] [--dry-run]` (PRST-5393): address_stats set to
// what the closed ledger plus the retained rows above it say, as additive corrections in sorted batches, next to
// the live indexer. --dry-run stages in a rolled-back transaction and prints what would change. A stopped run
// resumes: its staged corrections stay in address_stats_repair until applied.
func runRepairAddressStats(args []string, database *db.DB) int {
	fs := flag.NewFlagSet("repair-address-stats", flag.ExitOnError)
	batch := fs.Int("batch", 1000, "addresses per transaction")
	dry := fs.Bool("dry-run", false, "print how many addresses would change and the largest corrections; change nothing")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stage, err := database.StageAddressStatsRepair(ctx, *dry)
	switch {
	case errors.Is(err, db.ErrAddressStatsRepairPending):
		log.Info("repair-address-stats: resuming a staged repair")
	case err != nil:
		log.Error("repair-address-stats: staging failed", "error", err)
		return 1
	default:
		log.Info("repair-address-stats: staged", "dry_run", *dry, "ledger_closed_below", stage.LedgerClosedBelow, "head", stage.Head,
			"addresses_to_change", stage.Changes, "missing_rows", stage.Missing, "rows_unknown_to_ledger_left_as_is", stage.Untouched)
		for _, c := range stage.Largest {
			log.Info("repair-address-stats: correction", "address", c.Address, "tx_count", c.Txs, "internal_tx_count", c.Internal,
				"token_transfer_count", c.Transfers, "first_seen", c.FirstSeen, "last_seen", c.LastSeen, "missing", c.Missing)
		}
		if *dry {
			return 0
		}
	}

	var applied, created, batches int
	for {
		n, c, done, err := database.ApplyAddressStatsRepairBatch(ctx, *batch)
		if err != nil {
			log.Error("repair-address-stats: stopped; rerun to resume", "applied", applied, "error", err)
			return 1
		}
		applied, created, batches = applied+n, created+c, batches+1
		if done {
			break
		}
		if batches%100 == 0 {
			log.Info("repair-address-stats: progress", "applied", applied, "created", created)
		}
	}
	log.Info("repair-address-stats: done", "applied", applied, "created", created)
	return 0
}
