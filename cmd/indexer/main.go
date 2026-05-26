// Command indexer runs the chain-indexer service.
//
// The binary does two things concurrently:
//  1. An indexing worker polls the configured EVM node and writes chain data
//     to postgres (backfill + realtime).
//  2. A gRPC server exposes the indexed data to consumers via
//     chain_indexer.v1.IndexerService.
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
	"github.com/gateway-fm/chain-indexer/internal/rpc"
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

	rpcClient, err := rpc.New(cfg.RPCURL)
	if err != nil {
		log.Fatal("failed to create rpc client", "error", err)
	}

	idxCfg := &indexer.Config{
		RPCWorkers:           cfg.RPCWorkers,
		RPCRateLimit:         cfg.RPCRateLimit,
		DBBatchSize:          cfg.DBBatchSize,
		TokenMetadataWorkers: cfg.TokenMetadataWorkers,
		BalanceWorkers:       cfg.BalanceWorkers,
		EnableAsyncBalance:   cfg.EnableAsyncBalance,
		EnableTracing:        cfg.EnableTracing,
		TraceRateLimit:       cfg.TraceRateLimit,
		TraceWorkers:         cfg.TraceWorkers,
		CatchupEnabled:       cfg.CatchupEnabled,
		CatchupWorkers:       cfg.CatchupWorkers,
		CatchupBatchSize:     cfg.CatchupBatchSize,
		CatchupQueueSize:     cfg.CatchupQueueSize,
		SkipReceiptTxTypes:   skipReceipts,
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
	wg.Add(2)

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

	wg.Wait()
	log.Info("chain-indexer stopped")
}
