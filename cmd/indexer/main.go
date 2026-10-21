// Command indexer runs the chain-indexer service.
//
// The binary does two things concurrently:
//  1. An indexing worker polls the configured EVM node and writes chain data
//     to postgres (backfill + realtime).
//  2. A gRPC server exposes the indexed data to consumers via
//     chain_indexer.v1.IndexerService.
//  3. A metrics server exposes /metrics for Prometheus, plus an optional
//     pprof listener on a separate port.
//
// Both share the same database handle. On SIGINT / SIGTERM the service
// gracefully drains in-flight work and shuts down.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/gateway-fm/chain-indexer/internal/config"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/grpcserver"
	"github.com/gateway-fm/chain-indexer/internal/indexer"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", "error", err)
	}

	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		log.SetLevel(slog.LevelDebug)
	case "warn":
		log.SetLevel(slog.LevelWarn)
	case "error":
		log.SetLevel(slog.LevelError)
	}

	database, err := db.New(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("failed to connect to database", "error", err)
	}
	defer database.Close()
	database.RebuildWorkMem = cfg.RebuildWorkMem
	metrics.RegisterPoolStats(database.PoolStat)
	rpclient.Observer = metrics.ObserveRPC

	// Hidden tx types affect indexer receipt fetching and post-indexing filters.
	skipReceipts := make(map[int]bool)
	if cfg.HiddenTxTypes != "" {
		for _, s := range strings.Split(cfg.HiddenTxTypes, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			n, err := strconv.Atoi(s)
			if err != nil {
				log.Warn("invalid hidden_tx_types value, skipping", "value", s, "error", err)
				continue
			}
			skipReceipts[n] = true
			database.HiddenTxTypes = append(database.HiddenTxTypes, n)
		}
		if len(database.HiddenTxTypes) > 0 {
			log.Info("hiding transaction types from listings", "types", database.HiddenTxTypes)
		}
	}

	if err := database.Migrate(); err != nil {
		log.Fatal("failed to run migrations", "error", err)
	}
	if cfg.TxHashIndexes {
		if err := database.EnsureTxHashIndexes(context.Background()); err != nil {
			log.Fatal("failed to build the tx_hash lookup indexes", "error", err)
		}
	}

	if cfg.PruneRetainBlocks > 0 && cfg.PruneRetainBlocks < db.PruneFloorGap {
		log.Fatal("PRUNE_RETAIN_BLOCKS is inside the reorg floor", "retain_blocks", cfg.PruneRetainBlocks, "floor", db.PruneFloorGap)
	}
	prunedThrough, _, err := database.PrunedThrough(context.Background())
	if err != nil {
		log.Fatal("failed to read the prune watermark", "error", err)
	}
	// Once the history is cut a block above the newest partition cannot be stored: create them before ingest.
	if head, err := database.GetLatestBlockNumber(context.Background()); err != nil {
		log.Fatal("failed to read the stored head", "error", err)
	} else if _, err := database.EnsureHistoryPartitions(context.Background(), head+2*cfg.PartitionBlocks, cfg.PartitionBlocks); err != nil {
		log.Fatal("failed to create history partitions", "error", err)
	}

	rpcClient, err := rpc.New(cfg.RPCURL)
	if err != nil {
		log.Fatal("failed to create rpc client", "error", err)
	}
	rpcClient.LimitReceipts(cfg.RPCWorkers, cfg.RPCRateLimit)

	idxCfg := &indexer.Config{
		RPCWorkers:            cfg.RPCWorkers,
		RPCRateLimit:          cfg.RPCRateLimit,
		DBBatchSize:           cfg.DBBatchSize,
		TokenMetadataWorkers:  cfg.TokenMetadataWorkers,
		BalanceWorkers:        cfg.BalanceWorkers,
		EnableAsyncBalance:    cfg.EnableAsyncBalance,
		EnableTracing:         cfg.EnableTracing,
		TraceRateLimit:        cfg.TraceRateLimit,
		TraceWorkers:          cfg.TraceWorkers,
		CatchupEnabled:        cfg.CatchupEnabled,
		CatchupWorkers:        cfg.CatchupWorkers,
		CatchupBatchSize:      cfg.CatchupBatchSize,
		CatchupQueueSize:      cfg.CatchupQueueSize,
		SkipReceiptTxTypes:    skipReceipts,
		DailyStatsMinInterval: cfg.DailyStatsMinInterval,
		CommitPipelineDepth:   cfg.CommitPipelineDepth,
		BalanceDedupWindow:    cfg.BalanceDedupWindow,
		BalanceReaderBatch:    cfg.BalanceReaderBatch,
		BalanceSkipL1:         cfg.BalanceSkipL1,
		RetentionEnabled:      cfg.PruneRetainBlocks > 0 || prunedThrough > 0,
	}
	idx := indexer.NewWithConfig(database, rpcClient, cfg.PollInterval, cfg.StartBlock, idxCfg)

	grpcSrv, err := grpcserver.New(grpcserver.Config{
		ListenAddr:     cfg.GRPCListenAddr,
		OPStackEnabled: cfg.EnableOPDeposits,
	}, database)
	if err != nil {
		log.Fatal("failed to create grpc server", "error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("shutting down chain-indexer")
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		database.RunLedger(ctx, db.LedgerConfig{
			SpanBlocks: cfg.LedgerSpanBlocks,
			FirstBlock: cfg.StartBlock,
			Interval:   cfg.LedgerCloseInterval,
		})
	}()

	go func() {
		defer wg.Done()
		database.RunRetention(ctx, db.RetentionConfig{
			RetainBlocks:    cfg.PruneRetainBlocks,
			PartitionBlocks: cfg.PartitionBlocks,
			Interval:        cfg.PruneInterval,
			ArchiveDir:      cfg.PruneArchiveDir,
		})
	}()

	go func() {
		defer wg.Done()
		log.Info("starting indexer worker")
		if err := idx.Start(ctx); err != nil {
			log.Error("indexer worker error", "error", err)
			cancel()
		}
	}()

	go func() {
		defer wg.Done()
		if err := grpcSrv.Serve(ctx); err != nil {
			log.Error("grpc server error", "error", err)
			cancel()
		}
	}()

	if cfg.MetricsEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := metrics.ServeMetrics(ctx, cfg.MetricsListenAddr); err != nil {
				log.Error("metrics server error", "error", err)
				cancel()
			}
		}()
	} else {
		log.Warn("metrics disabled, /metrics will not be served")
	}

	if cfg.PprofEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := metrics.ServePprof(ctx, cfg.PprofListenAddr); err != nil {
				log.Error("pprof server error", "error", err)
				cancel()
			}
		}()
	}

	wg.Wait()
	log.Info("chain-indexer stopped")
}
