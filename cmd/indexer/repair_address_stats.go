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

// maxStaleRestages is how many times one run stages again after reorgs made its stage stale before it stops.
const maxStaleRestages = 10

// runRepairAddressStats is `indexer repair-address-stats [--batch N] [--dry-run]` (PRST-5393): address_stats set to
// what the closed ledger plus the retained rows above it say, as additive corrections in sorted batches, next to
// the live indexer. --dry-run stages in a rolled-back transaction and prints what would change. A stopped run
// resumes: its staged corrections stay in address_stats_repair until applied, and a stage that lost corrections
// before they were applied is staged again, as is one a reorg made stale.
func runRepairAddressStats(args []string, database *db.DB) int {
	fs := flag.NewFlagSet("repair-address-stats", flag.ExitOnError)
	batch := fs.Int("batch", 1000, "addresses per transaction")
	dry := fs.Bool("dry-run", false, "print how many addresses would change and the largest corrections; change nothing")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var applied, created, batches, stale int
	for restaged := 0; ; restaged++ {
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

		lost := false
		for {
			n, c, done, err := database.ApplyAddressStatsRepairBatch(ctx, *batch)
			if errors.Is(err, db.ErrAddressStatsRepairLost) && restaged-stale < 2 {
				log.Warn("repair-address-stats: staging again", "applied", applied, "error", err)
				lost = true
				break
			}
			if errors.Is(err, db.ErrAddressStatsRepairStale) {
				if stale++; stale <= maxStaleRestages {
					log.Warn("repair-address-stats: a reorg ran since the stage; staging again", "applied", applied, "error", err)
					lost = true
					break
				}
				log.Error("repair-address-stats: stopped; reorgs kept making the stage stale, rerun", "applied", applied,
					"restaged", maxStaleRestages, "error", err)
				return 1
			}
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
		if !lost {
			break
		}
	}
	log.Info("repair-address-stats: done", "applied", applied, "created", created)
	return 0
}
