// Package metrics is the process-wide Prometheus surface for chain-indexer.
//
// Free functions over a package-level registry, not a handle on Indexer:
// catchup and realtime rebuild a throwaway Indexer per block, so a field there
// would be nil on both production paths (PRST-4609).
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Wider than prometheus.DefBuckets, which stops at 10s, so a stage that runs long on a
// large database still lands in a bucket. (Before 0.5.0, RefreshTokenStats alone took 2.4s
// on a 10M-row database.)
var latencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

var (
	chainHeadBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_chain_head_block",
		Help: "Latest block number reported by the EVM node.",
	})

	lastIndexedBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_last_indexed_block",
		Help: "Highest block number written to the database.",
	})

	pruneWatermark = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_prune_watermark",
		Help: "Every block at or below this was pruned on purpose (0: nothing pruned).",
	})

	ledgerClosedThrough = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_ledger_closed_through",
		Help: "Every block at or below this is in a closed ledger span.",
	})

	ledgerCloseSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "indexer_ledger_close_seconds",
		Help:    "Duration of closing one ledger span from its rows.",
		Buckets: latencyBuckets,
	})

	statsBackfillThrough = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_stats_backfill_through",
		Help: "Every block at or below this is counted by the stats backfill.",
	})

	statsBackfillWaiting = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_stats_backfill_waiting_for_catchup",
		Help: "1 while the stats backfill has counted every block below START_BLOCK and waits for catch-up to store every block from it.",
	})

	pruneBlockedByBackfill = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_prune_blocked_by_stats_backfill",
		Help: "1 while retention prunes nothing because the stats backfill has not written its days.",
	})

	verifyChecked = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_verify_ranges_checked_total",
		Help: "Ledger checks run, by check (recount: one retained span; running: chain, tokens and one address bucket; node: one pruned span).",
	}, []string{"check"})

	verifyDrifted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_verify_ranges_drifted_total",
		Help: "Ledger checks that found at least one differing counter, by check.",
	}, []string{"check"})

	verifyDrift = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_verify_drift",
		Help: "Differing counters found by the last check, by check.",
	}, []string{"check"})

	verifyRepairs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_verify_repairs_total",
		Help: "Ledger checks whose drift was repaired, by check.",
	}, []string{"check"})

	verifyQuarantined = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_verify_quarantined",
		Help: "Spans and running keys that drifted again after a repair: no longer repaired.",
	})

	verifyCycleSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_verify_cycle_seconds",
		Help: "Duration of the last full cycle over every range (or address bucket), by check.",
	}, []string{"check"})

	verifyLastFullCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_verify_last_full_cycle_timestamp",
		Help: "Unix time the last full cycle finished, by check.",
	}, []string{"check"})

	retentionStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_retention_step_seconds",
		Help:    "Duration of one retention step: cut_constrain, cut_validate, cut_finish, ensure, advance, detach, archive, drop.",
		Buckets: latencyBuckets,
	}, []string{"step"})

	missingBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_missing_blocks",
		Help: "Blocks below the chain head absent from the database.",
	})

	startWaitingBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_start_waiting_blocks",
		Help: "Stored blocks the start is waiting for the node to serve before it compares them with the chain; 0 once started.",
	})

	dailyStatsLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_daily_stats_watermark_lag_blocks",
		Help: "Highest stored block minus the daily statistics watermark. It stays high while a missing block holds daily_stats for every later day.",
	})

	blocksIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_blocks_indexed_total",
		Help: "Blocks written to the database since process start.",
	})

	transactionsIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_transactions_indexed_total",
		Help: "Transactions written to the database since process start.",
	})

	gasIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_gas_indexed_total",
		Help: "Gas used across indexed blocks since process start.",
	})

	rpcRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_rpc_requests_total",
		Help: "JSON-RPC calls by method and outcome.",
	}, []string{"method", "outcome"})

	rpcDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_rpc_request_duration_seconds",
		Help:    "JSON-RPC call latency by method.",
		Buckets: latencyBuckets,
	}, []string{"method"})

	rpcStrategy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_rpc_strategy",
		Help: "Active receipt/trace fetch strategy: 1 for the path in use, 0 otherwise.",
	}, []string{"kind", "path"})

	stageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_stage_duration_seconds",
		Help:    "Duration of one stage of the block ingest path.",
		Buckets: latencyBuckets,
	}, []string{"stage"})

	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_queue_depth",
		Help: "Current depth of an internal work queue.",
	}, []string{"queue"})

	balanceQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_balance_queue_dropped_total",
		Help: "Balance refreshes dropped instead of queued: the enqueue was cancelled at shutdown, or a non-blocking enqueue found the queue full. Non-zero means lost balance updates.",
	})

	reorgs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_reorgs_total",
		Help: "Reorgs handled since process start.",
	})

	chainResets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_chain_resets_total",
		Help: "Chain resets detected at startup.",
	})
)

const (
	StageBlockTotal        = "block_total"
	StageDBCommit          = "db_commit"
	StageDBBegin           = "db_begin"
	StageDBFinish          = "db_finish"
	StageBalanceQueueWait  = "balance_queue_wait"
	StageRefreshTokenStats = "refresh_token_stats"

	QueueBalance = "balance"
	QueueCatchup = "catchup"
)

var strategyMu sync.Mutex

var (
	receiptPaths = []string{"batch", "logs", "per_tx", "none"}
	tracePaths   = []string{"batch", "per_tx"}
)

// GaugeVec series are created lazily, so without this the alternatives do not
// exist until the first fetch -- and never at all for traces when tracing is
// disabled. An alert cannot fire on a series that is absent, which is the whole
// point of exporting the zeros.
func init() {
	for _, p := range receiptPaths {
		rpcStrategy.WithLabelValues("receipts", p).Set(0)
	}
	for _, p := range tracePaths {
		rpcStrategy.WithLabelValues("traces", p).Set(0)
	}
}

func SetChainHead(block uint64) { chainHeadBlock.Set(float64(block)) }

func SetLastIndexed(block uint64) { lastIndexedBlock.Set(float64(block)) }

func SetMissingBlocks(n int64) { missingBlocks.Set(float64(n)) }

func SetStartWaitingBlocks(n uint64) { startWaitingBlocks.Set(float64(n)) }

func SetDailyStatsLag(n int64) { dailyStatsLag.Set(float64(n)) }

func SetPruneWatermark(block uint64) { pruneWatermark.Set(float64(block)) }

// VerifyChecked records one check's outcome: its differing counters and the state it left (ok, drift, repaired,
// quarantined).
func VerifyChecked(check string, drifted int, state string) {
	verifyChecked.WithLabelValues(check).Inc()
	verifyDrift.WithLabelValues(check).Set(float64(drifted))
	if drifted > 0 {
		verifyDrifted.WithLabelValues(check).Inc()
	}
	if state == "repaired" && drifted > 0 {
		verifyRepairs.WithLabelValues(check).Inc()
	}
}

// VerifyCheckedN records a sampled check: checked items, of which drifted differ.
func VerifyCheckedN(check string, checked, drifted int) {
	if checked == 0 {
		return
	}
	verifyChecked.WithLabelValues(check).Add(float64(checked))
	verifyDrift.WithLabelValues(check).Set(float64(drifted))
	verifyDrifted.WithLabelValues(check).Add(float64(drifted))
}

func SetStatsBackfillThrough(block uint64) { statsBackfillThrough.Set(float64(block)) }

func SetStatsBackfillWaiting(waiting bool) { statsBackfillWaiting.Set(boolGauge(waiting)) }

func SetPruneBlockedByBackfill(blocked bool) { pruneBlockedByBackfill.Set(boolGauge(blocked)) }

func SetVerifyQuarantined(n int64) { verifyQuarantined.Set(float64(n)) }

func VerifyCycle(check string, d time.Duration) {
	verifyCycleSeconds.WithLabelValues(check).Set(d.Seconds())
	verifyLastFullCycle.WithLabelValues(check).SetToCurrentTime()
}

func SetLedgerClosedThrough(block uint64) { ledgerClosedThrough.Set(float64(block)) }

func ObserveLedgerClose(d time.Duration) { ledgerCloseSeconds.Observe(d.Seconds()) }

func ObserveRetentionStep(step string, d time.Duration) {
	retentionStepSeconds.WithLabelValues(step).Observe(d.Seconds())
}

func BlockIndexed(txCount int, gasUsed uint64) {
	blocksIndexed.Inc()
	transactionsIndexed.Add(float64(txCount))
	gasIndexed.Add(float64(gasUsed))
}

func ObserveRPC(method, outcome string, d time.Duration) {
	rpcRequests.WithLabelValues(method, outcome).Inc()
	rpcDuration.WithLabelValues(method).Observe(d.Seconds())
}

func SetReceiptStrategy(path string) { setStrategy("receipts", path, receiptPaths) }

func SetTraceStrategy(path string) { setStrategy("traces", path, tracePaths) }

// Every alternative is written, not just the active one: an alert cannot fire on
// a series that is absent.
//
// Locked because one logical update is several independent Set calls and
// catchup fetches blocks concurrently. Interleaved loops can otherwise leave
// every path at 0, a state that never resolves.
func setStrategy(kind, path string, all []string) {
	strategyMu.Lock()
	defer strategyMu.Unlock()
	for _, p := range all {
		v := 0.0
		if p == path {
			v = 1
		}
		rpcStrategy.WithLabelValues(kind, p).Set(v)
	}
}

func ObserveStage(stage string, d time.Duration) {
	stageDuration.WithLabelValues(stage).Observe(d.Seconds())
}

// StageTimer records elapsed time for stage when the returned func is called.
func StageTimer(stage string) func() {
	start := time.Now()
	return func() { ObserveStage(stage, time.Since(start)) }
}

func SetQueueDepth(queue string, depth int) { queueDepth.WithLabelValues(queue).Set(float64(depth)) }

var balanceDeduped = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_balance_deduped_total",
	Help: "Balance work items folded into a held item for the same (address, token) before fetching.",
})

var (
	balanceFetchFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_balance_fetch_failed_total",
		Help: "Balance reads that failed and were not written.",
	})
	balanceReads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_balance_reads_total",
		Help: "Balances requested through batched reads (one eth_call reads many).",
	})
)

var ledgerIndexesReady = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "indexer_ledger_indexes_ready",
	Help: "1 once the ledger's lookup indexes are built; 0 while they are not, and the span rebase, the ledger checks and repair-address-stats wait.",
})

// SetLedgerIndexesReady sets indexer_ledger_indexes_ready.
func SetLedgerIndexesReady(ready bool) {
	if ready {
		ledgerIndexesReady.Set(1)
	} else {
		ledgerIndexesReady.Set(0)
	}
}

var nodeRecountFailures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_ledger_node_recount_failures_total",
	Help: "Pruned-span recounts from the node that failed; a span that fails twice in a row goes to the back of the queue unchecked.",
})

// NodeRecountFailed counts a failed pruned-span recount.
func NodeRecountFailed() { nodeRecountFailures.Inc() }

var nodeSpansUnchecked = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "indexer_ledger_node_spans_unchecked",
	Help: "Pruned spans the node recount left unchecked since it started, by reason: undecided (the node cannot judge a contract creation) or failing (its recount failed twice in a row).",
}, []string{"reason"})

// SetNodeSpansUnchecked sets indexer_ledger_node_spans_unchecked for one reason.
func SetNodeSpansUnchecked(reason string, n int) {
	nodeSpansUnchecked.WithLabelValues(reason).Set(float64(n))
}

var balanceRereadsAbandoned = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_balance_rereads_abandoned_total",
	Help: "Balances a reorg revert asked to read again that every attempt failed to read; each stays as the revert restored it until the holder's next transfer.",
})

var statsBackfillLenientBlocks = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_stats_backfill_lenient_blocks_total",
	Help: "Blocks the stats backfill counted without a trace or a contract's code the node failed to serve on every try.",
})

// StatsBackfillLenientBlock counts a block the stats backfill counted without what failed on every try.
func StatsBackfillLenientBlock() { statsBackfillLenientBlocks.Inc() }

// BalanceRereadsAbandoned counts reread requests given up after their last attempt.
func BalanceRereadsAbandoned(n int) {
	if n > 0 {
		balanceRereadsAbandoned.Add(float64(n))
	}
}

func BalanceFetchFailed(n int) {
	if n > 0 {
		balanceFetchFailed.Add(float64(n))
	}
}

func BalanceReads(n int) {
	if n > 0 {
		balanceReads.Add(float64(n))
	}
}

func BalanceDeduped(n int) {
	if n > 0 {
		balanceDeduped.Add(float64(n))
	}
}

func BalanceQueueDropped(n int) {
	if n > 0 {
		balanceQueueDropped.Add(float64(n))
	}
}

func ReorgDetected() { reorgs.Inc() }

func ChainResetDetected() { chainResets.Inc() }

var (
	nodeLookupCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_node_lookup_cache_total",
		Help: "Pruned-block lookups served from the node-lookup cache (hit) or fetched from the node (miss).",
	}, []string{"result"})

	nodeLookupRPC = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_node_lookup_rpc_seconds",
		Help:    "Latency of the node calls that serve pruned blocks and transactions.",
		Buckets: latencyBuckets,
	}, []string{"method", "outcome"})

	nodeLookupBusy = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_node_lookup_busy_total",
		Help: "Pruned lookups refused because every node slot stayed taken for the request timeout.",
	})

	nodeLookupCachedBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_node_lookup_cached_blocks",
		Help: "Blocks held in the node-lookup cache.",
	})

	nodeLookupCachedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_node_lookup_cached_bytes",
		Help: "Estimated size of the node-lookup cache.",
	})
)

func NodeLookupCache(hit bool) {
	if hit {
		nodeLookupCache.WithLabelValues("hit").Inc()
		return
	}
	nodeLookupCache.WithLabelValues("miss").Inc()
}

func ObserveNodeLookupRPC(method string, d time.Duration, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	nodeLookupRPC.WithLabelValues(method, outcome).Observe(d.Seconds())
}

func NodeLookupBusy() { nodeLookupBusy.Inc() }

func SetNodeLookupCache(blocks, bytes int) {
	nodeLookupCachedBlocks.Set(float64(blocks))
	nodeLookupCachedBytes.Set(float64(bytes))
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
