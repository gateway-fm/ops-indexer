package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

// runRepairAddressStats is `indexer repair-address-stats [--batch N] [--dry-run]` (PRST-5393): every address_stats row
// set to its truth, the closed ledger plus the open stats backfill span plus the rows above the closed spans, bucket
// by bucket through the span rebase, next to the live indexer. A row is created for an address with a truth and
// removed for one without. A stopped run resumes at the bucket it stopped in. --dry-run reads and prints what each
// bucket would change, and writes nothing.
func runRepairAddressStats(args []string, database *db.DB) int {
	fs := flag.NewFlagSet("repair-address-stats", flag.ExitOnError)
	batch := fs.Int("batch", db.AddressRepairBatch, "addresses per transaction (1-1000)")
	dry := fs.Bool("dry-run", false, "print what each address bucket would change; write nothing")
	waitIndexes := fs.Duration("wait-indexes", 30*time.Minute,
		"how long to wait for the ledger's address index, which the indexer builds after an upgrade, before giving up")
	_ = fs.Parse(args)
	if *batch < 1 || *batch > 1000 {
		log.Error("repair-address-stats: --batch must be 1-1000", "batch", *batch)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if !waitForLedgerIndexes(ctx, database, *waitIndexes) {
		return 1
	}

	staged, err := database.AddressStatsRepairStageExists(ctx)
	if err != nil {
		log.Error("repair-address-stats: could not check for an earlier release's stage", "error", err)
		return 1
	}
	if *dry {
		return dryRunRepairAddressStats(ctx, database, staged)
	}
	if staged {
		if err := database.DropAddressStatsRepairStage(ctx); err != nil {
			log.Error("repair-address-stats: could not drop an earlier release's stage", "error", err)
			return 1
		}
		log.Warn("repair-address-stats: dropped an earlier release's stage; this run recounts every address instead")
	}

	from, err := database.AddressRepairNext(ctx)
	if err != nil {
		log.Error("repair-address-stats: could not read where an earlier run stopped", "error", err)
		return 1
	}
	if from > 0 {
		log.Info("repair-address-stats: resuming", "bucket", from)
	}
	var total db.RebaseResult
	for bucket := from; bucket < db.AddressBuckets; bucket++ {
		r, err := database.RepairAddressBucket(ctx, bucket, *batch)
		total.Changed, total.Created, total.Removed = total.Changed+r.Changed, total.Created+r.Created, total.Removed+r.Removed
		if err != nil {
			log.Error("repair-address-stats: stopped; rerun to resume", "bucket", bucket, "error", err)
			return 1
		}
		if err := database.SetAddressRepairNext(ctx, bucket+1); err != nil {
			log.Error("repair-address-stats: stopped; rerun to resume", "bucket", bucket, "error", err)
			return 1
		}
		if r.Changed+r.Created+r.Removed > 0 || bucket%16 == 15 {
			log.Info("repair-address-stats: bucket repaired", "bucket", bucket, "corrected", r.Changed-r.Created,
				"created", r.Created, "removed", r.Removed)
		}
	}
	if err := database.FinishAddressRepair(ctx); err != nil {
		log.Error("repair-address-stats: stopped; rerun to resume", "step", "finish", "error", err)
		return 1
	}
	log.Info("repair-address-stats: done", "corrected", total.Changed-total.Created, "created", total.Created,
		"removed", total.Removed)
	return 0
}

func dryRunRepairAddressStats(ctx context.Context, database *db.DB, staged bool) int {
	if staged {
		log.Warn("repair-address-stats: an earlier release left a repair stage; a run drops it")
	}
	var changed, missing, noTruth int
	for bucket := 0; bucket < db.AddressBuckets; bucket++ {
		drift, err := database.AddressRepairDriftIn(ctx, bucket)
		if err != nil {
			log.Error("repair-address-stats: dry run failed", "bucket", bucket, "error", err)
			return 1
		}
		changed, missing, noTruth = changed+drift.Changed, missing+drift.Missing, noTruth+drift.NoTruth
		if len(drift.Addresses) > 0 {
			log.Info("repair-address-stats: bucket would change", "bucket", bucket, "rows_to_correct", drift.Changed,
				"rows_to_create", drift.Missing, "rows_to_remove", drift.NoTruth)
		}
	}
	log.Info("repair-address-stats: dry run", "rows_to_correct", changed, "rows_to_create", missing,
		"rows_to_remove", noTruth)
	return 0
}

// waitForLedgerIndexes waits, up to wait, for the ledger's lookup indexes, which the indexer builds CONCURRENTLY after
// an upgrade: without them every bucket reads the whole ledger and its batches run out of time.
func waitForLedgerIndexes(ctx context.Context, database *db.DB, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for logged := false; ; logged = true {
		ready, err := database.LedgerIndexesReady(ctx)
		if err == nil && ready {
			return true
		}
		if !logged {
			log.Info("repair-address-stats: waiting for the ledger's address index; the indexer builds it after an upgrade",
				"wait_up_to", wait, "error", err)
		}
		if !time.Now().Before(deadline) {
			log.Error("repair-address-stats: the ledger's address index is not built; start the indexer so it builds it, then rerun",
				"waited", wait)
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(min(10*time.Second, time.Until(deadline))):
		}
	}
}
