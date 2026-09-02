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

// Wider than prometheus.DefBuckets, which stops at 10s: RefreshTokenStats alone
// was measured at 2.4s on a 10M-row database and grows with history.
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

	missingBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_missing_blocks",
		Help: "Blocks below the chain head absent from the database.",
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
		Help: "Balance refreshes dropped because the queue was full; non-zero means lost balance updates.",
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
	StageRefreshTokenStats = "refresh_token_stats"

	QueueBalance = "balance"
	QueueCatchup = "catchup"
)

var strategyMu sync.Mutex

var (
	receiptPaths = []string{"batch", "logs", "per_tx", "none"}
	tracePaths   = []string{"batch", "per_tx"}
)

func SetChainHead(block uint64) { chainHeadBlock.Set(float64(block)) }

func SetLastIndexed(block uint64) { lastIndexedBlock.Set(float64(block)) }

func SetMissingBlocks(n int64) { missingBlocks.Set(float64(n)) }

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

func BalanceQueueDropped(n int) {
	if n > 0 {
		balanceQueueDropped.Add(float64(n))
	}
}

func ReorgDetected() { reorgs.Inc() }

func ChainResetDetected() { chainResets.Inc() }
