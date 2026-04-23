package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/gateway-fm/chain-indexer/internal/config"
	"github.com/gateway-fm/chain-indexer/internal/db"
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

	// Parse hidden transaction types from config
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
			database.HiddenTxTypes = append(database.HiddenTxTypes, n)
		}
		if len(database.HiddenTxTypes) > 0 {
			log.Info("hiding transaction types from listings", "types", database.HiddenTxTypes)
		}
	}

	if err := database.Migrate(); err != nil {
		log.Fatal("failed to run migrations", "error", err)
	}

	// Parse and apply hidden tx types to DB for query filtering
	if cfg.HiddenTxTypes != "" {
		var hidden []int
		for _, s := range strings.Split(cfg.HiddenTxTypes, ",") {
			s = strings.TrimSpace(s)
			if n, err := strconv.Atoi(s); err == nil {
				hidden = append(hidden, n)
			}
		}
		if len(hidden) > 0 {
			database.HiddenTxTypes = hidden
		}
	}

	rpcClient, err := rpc.New(cfg.RPCURL)
	if err != nil {
		log.Fatal("failed to create rpc client", "error", err)
	}

	// Parse hidden tx types into a set for skipping receipt fetches
	skipReceipts := make(map[int]bool)
	if cfg.HiddenTxTypes != "" {
		for _, s := range strings.Split(cfg.HiddenTxTypes, ",") {
			s = strings.TrimSpace(s)
			if n, err := strconv.Atoi(s); err == nil {
				skipReceipts[n] = true
			}
		}
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("shutting down indexer")
		cancel()
	}()

	log.Info("starting indexer worker")
	if err := idx.Start(ctx); err != nil {
		log.Error("indexer error", "error", err)
	}
}
