package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

type Config struct {
	RPCURL      string `mapstructure:"rpc_url"`
	DatabaseURL string `mapstructure:"database_url"`
	// GRPCListenAddr is the address the chain-indexer gRPC server binds to,
	// e.g. ":50051". Required.
	GRPCListenAddr string        `mapstructure:"grpc_listen_addr"`
	APIPort        int           `mapstructure:"api_port"`
	PollInterval   time.Duration `mapstructure:"poll_interval"`
	// Minimum interval between recomputations of the current day's daily_stats
	// row (PRST-4669). 0 = every 100 blocks (legacy).
	DailyStatsMinInterval time.Duration `mapstructure:"daily_stats_min_interval"`
	CommitPipelineDepth   int           `mapstructure:"commit_pipeline"`
	BalanceDedupWindow    time.Duration `mapstructure:"balance_dedup_window"`
	BalanceReaderBatch    int           `mapstructure:"balance_reader_batch"`
	BalanceSkipL1         bool          `mapstructure:"balance_skip_l1"`
	// TxHashIndexes keeps an index on tx_hash in logs and token_transfers for consumers that read them by
	// transaction hash (0.4.0 had one); false spares ingest its random writes.
	TxHashIndexes bool   `mapstructure:"tx_hash_indexes"`
	StartBlock    uint64 `mapstructure:"start_block"`
	// PruneRetainBlocks keeps this many blocks below the stored head and drops older history by whole partitions;
	// 0 (the default) keeps everything. PartitionBlocks 0 is PruneRetainBlocks / 10.
	PruneRetainBlocks   uint64        `mapstructure:"prune_retain_blocks"`
	PartitionBlocks     uint64        `mapstructure:"partition_blocks"`
	PruneInterval       time.Duration `mapstructure:"prune_interval"`
	PruneArchiveDir     string        `mapstructure:"prune_archive_dir"`
	LedgerSpanBlocks    uint64        `mapstructure:"ledger_span_blocks"`
	LedgerCloseInterval time.Duration `mapstructure:"ledger_close_interval"`
	LedgerWorkMem       string        `mapstructure:"ledger_work_mem"`
	// PrunedLookup serves blocks and transactions below the prune watermark from the node (PRST-5375).
	PrunedLookup            bool          `mapstructure:"pruned_lookup"`
	PrunedLookupCacheBlocks int           `mapstructure:"pruned_lookup_cache_blocks"`
	PrunedLookupCacheMB     int           `mapstructure:"pruned_lookup_cache_mb"`
	PrunedLookupTimeout     time.Duration `mapstructure:"pruned_lookup_timeout"`
	PrunedLookupConcurrency int           `mapstructure:"pruned_lookup_concurrency"`
	LedgerMaxHeadLag        uint64        `mapstructure:"ledger_max_head_lag_blocks"`

	RPCWorkers           int  `mapstructure:"rpc_workers"`
	RPCRateLimit         int  `mapstructure:"rpc_rate_limit"`
	DBBatchSize          int  `mapstructure:"db_batch_size"`
	TokenMetadataWorkers int  `mapstructure:"token_metadata_workers"`
	BalanceWorkers       int  `mapstructure:"balance_workers"`
	EnableAsyncBalance   bool `mapstructure:"enable_async_balance"`

	EnableTracing  bool `mapstructure:"enable_tracing"`
	TraceRateLimit int  `mapstructure:"trace_rate_limit"`
	TraceWorkers   int  `mapstructure:"trace_workers"`

	WSMaxConnections int           `mapstructure:"ws_max_connections"`
	WSPingInterval   time.Duration `mapstructure:"ws_ping_interval"`

	SolcPath            string `mapstructure:"solc_path"`
	UseSourcifyFallback bool   `mapstructure:"use_sourcify_fallback"`

	MetricsEnabled    bool   `mapstructure:"metrics_enabled"`
	MetricsListenAddr string `mapstructure:"metrics_listen_addr"`
	PprofEnabled      bool   `mapstructure:"pprof_enabled"`
	PprofListenAddr   string `mapstructure:"pprof_listen_addr"`

	CatchupEnabled   bool `mapstructure:"catchup_enabled"`
	CatchupWorkers   int  `mapstructure:"catchup_workers"`
	CatchupBatchSize int  `mapstructure:"catchup_batch_size"`
	CatchupQueueSize int  `mapstructure:"catchup_queue_size"`
	// RealtimeMaxBehind: realtime further behind the node than this jumps to the head and hands the skipped blocks
	// to catch-up. 0 never jumps.
	RealtimeMaxBehind uint64 `mapstructure:"realtime_max_behind_blocks"`

	// Set PRIVACY_PROXY_URL to enable proxy mode (auth + privacy enforcement).
	// Leave empty for standalone mode (direct DB/RPC access).
	PrivacyProxyURL string `mapstructure:"privacy_proxy_url"`
	// PrivacyProxyPublicURL is the browser-facing URL for OAuth redirects.
	// Defaults to PrivacyProxyURL if not set. Set this when the proxy is on a
	// Docker-internal hostname (e.g. privacy-proxy-proxy-backend-1) but the
	// browser needs to reach it via localhost or a public hostname.
	PrivacyProxyPublicURL string `mapstructure:"privacy_proxy_public_url"`
	SSOClientID           string `mapstructure:"sso_client_id"`
	SSORedirectURI        string `mapstructure:"sso_redirect_uri"`

	LogLevel string `mapstructure:"log_level"`

	// HiddenTxTypes is a comma-separated list of transaction type numbers to
	// exclude from the default transaction listings (e.g. "126" for OP deposit TXs).
	HiddenTxTypes string `mapstructure:"hidden_tx_types"`

	// RebuildWorkMem sets work_mem (via SET LOCAL) for the one-shot address_stats
	// rebuild. Env: REBUILD_WORK_MEM. Lower it on memory-tight Postgres.
	RebuildWorkMem string `mapstructure:"rebuild_work_mem"`

	EnableOPDeposits      bool          `mapstructure:"enable_op_deposits"`
	L1RPCURL              string        `mapstructure:"l1_rpc_url"`
	OptimismPortalAddress string        `mapstructure:"optimism_portal_address"`
	L1DepositPollInterval time.Duration `mapstructure:"l1_deposit_poll_interval"`
	L1DepositBatchSize    int           `mapstructure:"l1_deposit_batch_size"`
	L1DepositStartBlock   uint64        `mapstructure:"l1_deposit_start_block"`
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("rpc_url", "http://127.0.0.1:8545")
	v.SetDefault("database_url", "postgres://postgres:postgres@localhost:5432/chain_indexer?sslmode=disable")
	v.SetDefault("grpc_listen_addr", ":50051")
	v.SetDefault("api_port", 8080)
	v.SetDefault("poll_interval", "2s")
	v.SetDefault("daily_stats_min_interval", "60s")
	v.SetDefault("commit_pipeline", 1)
	v.SetDefault("balance_dedup_window", "0s")
	v.SetDefault("balance_reader_batch", 0)
	v.SetDefault("balance_skip_l1", false)
	v.SetDefault("tx_hash_indexes", true)
	v.SetDefault("start_block", 0)
	v.SetDefault("prune_retain_blocks", 0)
	v.SetDefault("partition_blocks", 0)
	v.SetDefault("prune_interval", "1m")
	v.SetDefault("prune_archive_dir", "")
	v.SetDefault("ledger_span_blocks", 100000)
	v.SetDefault("ledger_close_interval", "10s")
	v.SetDefault("ledger_work_mem", "512MB")
	v.SetDefault("pruned_lookup", false)
	v.SetDefault("pruned_lookup_cache_blocks", 2000)
	v.SetDefault("pruned_lookup_cache_mb", 256)
	v.SetDefault("pruned_lookup_timeout", "5s")
	v.SetDefault("pruned_lookup_concurrency", 4)
	v.SetDefault("ledger_max_head_lag_blocks", 0)

	v.SetDefault("rpc_workers", 50)
	v.SetDefault("rpc_rate_limit", 500)
	v.SetDefault("db_batch_size", 500)
	v.SetDefault("token_metadata_workers", 20)
	v.SetDefault("balance_workers", 30)
	v.SetDefault("enable_async_balance", true)

	v.SetDefault("enable_tracing", false)
	v.SetDefault("trace_rate_limit", 50)
	v.SetDefault("trace_workers", 10)

	v.SetDefault("ws_max_connections", 10000)
	v.SetDefault("ws_ping_interval", "30s")

	v.SetDefault("solc_path", "/opt/solc")
	v.SetDefault("use_sourcify_fallback", true)

	v.SetDefault("metrics_enabled", true)
	v.SetDefault("metrics_listen_addr", ":8080")
	v.SetDefault("pprof_enabled", false)
	v.SetDefault("pprof_listen_addr", ":6060")

	v.SetDefault("catchup_enabled", true)
	v.SetDefault("catchup_workers", 10)
	v.SetDefault("catchup_batch_size", 100)
	v.SetDefault("catchup_queue_size", 1000)
	v.SetDefault("realtime_max_behind_blocks", 0)

	v.SetDefault("privacy_proxy_url", "")
	v.SetDefault("privacy_proxy_public_url", "")
	v.SetDefault("sso_client_id", "explorer")
	v.SetDefault("sso_redirect_uri", "http://localhost:8080/api/auth/callback")

	v.SetDefault("log_level", "info")

	v.SetDefault("hidden_tx_types", "126") // OP deposit system transactions hidden by default
	v.SetDefault("rebuild_work_mem", "1GB")

	v.SetDefault("enable_op_deposits", false)
	v.SetDefault("l1_rpc_url", "")
	v.SetDefault("optimism_portal_address", "")
	v.SetDefault("l1_deposit_poll_interval", "12s")
	v.SetDefault("l1_deposit_batch_size", 1000)
	v.SetDefault("l1_deposit_start_block", 0)
}

func Load() (*Config, error) {
	v := viper.New()
	setDefaults(v)

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AddConfigPath("./backend")
	v.AddConfigPath("..")
	_ = v.ReadInConfig()

	cfg := &Config{}
	cfg.RPCURL = v.GetString("rpc_url")
	cfg.DatabaseURL = v.GetString("database_url")
	cfg.GRPCListenAddr = v.GetString("grpc_listen_addr")
	cfg.APIPort = v.GetInt("api_port")
	cfg.PollInterval = v.GetDuration("poll_interval")
	cfg.DailyStatsMinInterval = v.GetDuration("daily_stats_min_interval")
	cfg.CommitPipelineDepth = v.GetInt("commit_pipeline")
	cfg.BalanceDedupWindow = v.GetDuration("balance_dedup_window")
	cfg.BalanceReaderBatch = v.GetInt("balance_reader_batch")
	cfg.BalanceSkipL1 = v.GetBool("balance_skip_l1")
	cfg.TxHashIndexes = v.GetBool("tx_hash_indexes")
	cfg.StartBlock = v.GetUint64("start_block")
	cfg.PruneRetainBlocks = v.GetUint64("prune_retain_blocks")
	cfg.PartitionBlocks = v.GetUint64("partition_blocks")
	if cfg.PartitionBlocks == 0 {
		cfg.PartitionBlocks = cfg.PruneRetainBlocks / 10
	}
	cfg.PruneInterval = v.GetDuration("prune_interval")
	cfg.PruneArchiveDir = v.GetString("prune_archive_dir")
	cfg.LedgerSpanBlocks = v.GetUint64("ledger_span_blocks")
	cfg.LedgerCloseInterval = v.GetDuration("ledger_close_interval")
	cfg.LedgerWorkMem = v.GetString("ledger_work_mem")
	cfg.PrunedLookup = v.GetBool("pruned_lookup")
	cfg.PrunedLookupCacheBlocks = v.GetInt("pruned_lookup_cache_blocks")
	cfg.PrunedLookupCacheMB = v.GetInt("pruned_lookup_cache_mb")
	cfg.PrunedLookupTimeout = v.GetDuration("pruned_lookup_timeout")
	cfg.PrunedLookupConcurrency = v.GetInt("pruned_lookup_concurrency")
	cfg.LedgerMaxHeadLag = v.GetUint64("ledger_max_head_lag_blocks")

	cfg.RPCWorkers = v.GetInt("rpc_workers")
	cfg.RPCRateLimit = v.GetInt("rpc_rate_limit")
	cfg.DBBatchSize = v.GetInt("db_batch_size")
	cfg.TokenMetadataWorkers = v.GetInt("token_metadata_workers")
	cfg.BalanceWorkers = v.GetInt("balance_workers")
	cfg.EnableAsyncBalance = v.GetBool("enable_async_balance")

	cfg.EnableTracing = v.GetBool("enable_tracing")
	cfg.TraceRateLimit = v.GetInt("trace_rate_limit")
	cfg.TraceWorkers = v.GetInt("trace_workers")

	cfg.WSMaxConnections = v.GetInt("ws_max_connections")
	cfg.WSPingInterval = v.GetDuration("ws_ping_interval")

	cfg.SolcPath = v.GetString("solc_path")
	cfg.UseSourcifyFallback = v.GetBool("use_sourcify_fallback")

	cfg.MetricsEnabled = v.GetBool("metrics_enabled")
	cfg.MetricsListenAddr = v.GetString("metrics_listen_addr")
	cfg.PprofEnabled = v.GetBool("pprof_enabled")
	cfg.PprofListenAddr = v.GetString("pprof_listen_addr")

	cfg.CatchupEnabled = v.GetBool("catchup_enabled")
	cfg.CatchupWorkers = v.GetInt("catchup_workers")
	cfg.CatchupBatchSize = v.GetInt("catchup_batch_size")
	cfg.CatchupQueueSize = v.GetInt("catchup_queue_size")
	cfg.RealtimeMaxBehind = v.GetUint64("realtime_max_behind_blocks")

	cfg.PrivacyProxyURL = v.GetString("privacy_proxy_url")
	cfg.PrivacyProxyPublicURL = v.GetString("privacy_proxy_public_url")
	if cfg.PrivacyProxyPublicURL == "" {
		cfg.PrivacyProxyPublicURL = cfg.PrivacyProxyURL
	}
	cfg.SSOClientID = v.GetString("sso_client_id")
	cfg.SSORedirectURI = v.GetString("sso_redirect_uri")

	cfg.LogLevel = v.GetString("log_level")

	cfg.RebuildWorkMem = v.GetString("rebuild_work_mem")

	cfg.EnableOPDeposits = v.GetBool("enable_op_deposits")
	cfg.L1RPCURL = v.GetString("l1_rpc_url")
	cfg.OptimismPortalAddress = v.GetString("optimism_portal_address")
	cfg.L1DepositPollInterval = v.GetDuration("l1_deposit_poll_interval")
	cfg.L1DepositBatchSize = v.GetInt("l1_deposit_batch_size")
	cfg.L1DepositStartBlock = v.GetUint64("l1_deposit_start_block")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ReorgFloorBlocks is the minimum retention window: nothing within 100x the deepest reorg the indexer reverts is
// ever pruned (db.PruneFloorGap, prune_floor_gap()).
const ReorgFloorBlocks = 12800

// The retention and ledger minimums are derived for a chain of up to MaxChainBlocksPerSecond (100 ms blocks), with
// catchUpHeadroom to spare: the ledger and the partitions keep ahead of such a chain ten times over, so a backlog
// left by an outage shrinks at nine times the chain's rate instead of never.
const (
	MaxChainBlocksPerSecond = 10
	catchUpHeadroom         = 10
	// MinPollInterval bounds how often the retention pass and the ledger closer poll. Either finds work only once
	// the head has moved by a span or a range, at least MinLedgerSpanBlocks(MinPollInterval) blocks; polling more
	// often than once a second only reads the database for nothing.
	MinPollInterval = time.Second
	// partitionKeeperInterval is how often retention keeps partitions ahead of the head (the db package's
	// ensureInterval).
	partitionKeeperInterval = 10 * time.Second
	// MinPartitionBlocks: the two partitions kept ahead of the head cover what the chain makes between two keeper
	// checks, with the headroom: 2 x MinPartitionBlocks = 10 blocks/s x 10 x 10 s.
	MinPartitionBlocks = MaxChainBlocksPerSecond * catchUpHeadroom * uint64(partitionKeeperInterval/time.Second) / 2
)

// MinLedgerSpanBlocks is the smallest LEDGER_SPAN_BLOCKS for a close interval: the closer closes one span per
// interval, so a span must hold what the chain makes in one, with the headroom.
func MinLedgerSpanBlocks(closeInterval time.Duration) uint64 {
	return uint64(math.Ceil(MaxChainBlocksPerSecond * catchUpHeadroom * closeInterval.Seconds()))
}

func (c *Config) Validate() error {
	if c.RPCURL == "" {
		return fmt.Errorf("RPC_URL is required")
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.APIPort <= 0 || c.APIPort > 65535 {
		return fmt.Errorf("API_PORT must be between 1 and 65535")
	}
	if c.RPCWorkers <= 0 {
		return fmt.Errorf("RPC_WORKERS must be greater than 0")
	}
	if c.RPCRateLimit <= 0 {
		return fmt.Errorf("RPC_RATE_LIMIT must be greater than 0")
	}
	if c.BalanceWorkers < 0 {
		return fmt.Errorf("BALANCE_WORKERS must not be negative")
	}
	if err := c.CheckCommitPipeline(); err != nil {
		return err
	}
	if c.PruneRetainBlocks > 0 && c.PruneRetainBlocks < ReorgFloorBlocks {
		return fmt.Errorf("PRUNE_RETAIN_BLOCKS %d is inside the reorg floor (%d blocks)", c.PruneRetainBlocks, ReorgFloorBlocks)
	}
	if c.LedgerSpanBlocks == 0 {
		return fmt.Errorf("LEDGER_SPAN_BLOCKS must be greater than 0")
	}
	if c.PruneInterval < MinPollInterval {
		return fmt.Errorf("PRUNE_INTERVAL=%s would run the retention pass back to back against the database: set it to at least %s",
			c.PruneInterval, MinPollInterval)
	}
	if c.LedgerCloseInterval < MinPollInterval {
		return fmt.Errorf("LEDGER_CLOSE_INTERVAL=%s would run the ledger closer back to back against the database: set it to at least %s",
			c.LedgerCloseInterval, MinPollInterval)
	}
	if minSpan := MinLedgerSpanBlocks(c.LedgerCloseInterval); c.LedgerSpanBlocks < minSpan {
		return fmt.Errorf("LEDGER_SPAN_BLOCKS=%d with LEDGER_CLOSE_INTERVAL=%s closes at most %.0f blocks/s, too slow to keep up with "+
			"a chain of up to %d blocks/s with %dx headroom, and nothing is pruned while the ledger is behind: "+
			"set LEDGER_SPAN_BLOCKS to at least %d, or shorten LEDGER_CLOSE_INTERVAL",
			c.LedgerSpanBlocks, c.LedgerCloseInterval, float64(c.LedgerSpanBlocks)/c.LedgerCloseInterval.Seconds(),
			MaxChainBlocksPerSecond, catchUpHeadroom, minSpan)
	}
	if c.PruneRetainBlocks > 0 && c.PartitionBlocks < MinPartitionBlocks {
		return fmt.Errorf("PARTITION_BLOCKS=%d with retention on: two partitions are kept ahead of the head every %s, and partitions "+
			"this small let a chain of up to %d blocks/s outrun them; set PARTITION_BLOCKS to at least %d",
			c.PartitionBlocks, partitionKeeperInterval, MaxChainBlocksPerSecond, MinPartitionBlocks)
	}
	if c.StartBlock > 0 && c.PruneRetainBlocks > 0 {
		return fmt.Errorf("START_BLOCK %d with PRUNE_RETAIN_BLOCKS %d needs the stats backfill of blocks [0, START_BLOCK), which this build does not have: "+
			"every total would leave those blocks out. Set START_BLOCK=0, or PRUNE_RETAIN_BLOCKS=0 to keep totals from START_BLOCK", c.StartBlock, c.PruneRetainBlocks)
	}
	if c.EnableOPDeposits {
		if c.L1RPCURL == "" {
			return fmt.Errorf("L1_RPC_URL is required when ENABLE_OP_DEPOSITS is true")
		}
		if c.OptimismPortalAddress == "" {
			return fmt.Errorf("OPTIMISM_PORTAL_ADDRESS is required when ENABLE_OP_DEPOSITS is true")
		}
	}
	return nil
}

// CheckCommitPipeline refuses a COMMIT_PIPELINE deeper than the pool's maximum connections minus the ones kept for the
// rest of the indexer (ConnectionsBesidePipeline).
func (c *Config) CheckCommitPipeline() error {
	if c.CommitPipelineDepth <= 1 {
		return nil
	}
	pool, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	maxConns, beside := int(pool.MaxConns), c.ConnectionsBesidePipeline()
	if c.CommitPipelineDepth <= maxConns-beside {
		return nil
	}
	holders, lower := "", ""
	if c.EnableAsyncBalance {
		holders += fmt.Sprintf("BALANCE_WORKERS=%d balance flushes, ", c.BalanceWorkers)
		lower += "lower BALANCE_WORKERS, "
	}
	if c.CatchupEnabled {
		holders += fmt.Sprintf("CATCHUP_WORKERS=%d catch-up writes, ", c.CatchupWorkers)
		lower += "lower CATCHUP_WORKERS, "
	}
	holders += "the ledger closer, "
	if c.PruneRetainBlocks > 0 {
		holders += "retention, "
	}
	return fmt.Errorf("COMMIT_PIPELINE=%d leaves the rest of the indexer too few database connections: the pool has %d "+
		"(pool_max_conns in DATABASE_URL, default the larger of 4 and the CPU count), the open block writes can hold %d, "+
		"and %d more are kept for %sthe daily statistics advance and the progress reads, so they are not starved "+
		"while the pipeline is full. Raise pool_max_conns to %d or more, %sor set COMMIT_PIPELINE to at most %d",
		c.CommitPipelineDepth, maxConns, c.CommitPipelineDepth, beside, holders,
		c.CommitPipelineDepth+beside, lower, max(1, maxConns-beside))
}

// ConnectionsBesidePipeline is how many database connections the rest of the indexer can hold while the standard
// loop or realtime has COMMIT_PIPELINE block writes open: the balance flushes, the catch-up workers' writes (realtime
// pipelines beside them), the daily statistics advance, the progress reads, the ledger closer (one for a whole span
// close; it always runs) and, with retention on, the statements under the history-DDL gate (the gate itself holds a
// connection outside the pool).
func (c *Config) ConnectionsBesidePipeline() int {
	balanceFlushes, catchupWrites, retention := 0, 0, 0
	if c.EnableAsyncBalance {
		balanceFlushes = c.BalanceWorkers
	}
	if c.CatchupEnabled {
		catchupWrites = c.CatchupWorkers
	}
	if c.PruneRetainBlocks > 0 {
		retention = 1
	}
	const dailyStatsAdvance, progressReads, ledgerCloser = 1, 1, 1
	return balanceFlushes + catchupWrites + dailyStatsAdvance + progressReads + ledgerCloser + retention
}

func (c *Config) String() string {
	maskedDBURL := maskDatabasePassword(c.DatabaseURL)

	return fmt.Sprintf(`Config{
  Core:
    RPC_URL: %s
    DATABASE_URL: %s
    API_PORT: %d
    POLL_INTERVAL: %s
    START_BLOCK: %d
  Parallelization:
    RPC_WORKERS: %d
    RPC_RATE_LIMIT: %d
    DB_BATCH_SIZE: %d
    TOKEN_METADATA_WORKERS: %d
    BALANCE_WORKERS: %d
    ENABLE_ASYNC_BALANCE: %t
  Tracing:
    ENABLE_TRACING: %t
    TRACE_RATE_LIMIT: %d
    TRACE_WORKERS: %d
  WebSocket:
    WS_MAX_CONNECTIONS: %d
    WS_PING_INTERVAL: %s
  Verification:
    SOLC_PATH: %s
    USE_SOURCIFY_FALLBACK: %t
  Metrics:
    METRICS_ENABLED: %t
    METRICS_LISTEN_ADDR: %s
    PPROF_ENABLED: %t
    PPROF_LISTEN_ADDR: %s
  Catchup:
    CATCHUP_ENABLED: %t
    CATCHUP_WORKERS: %d
    CATCHUP_BATCH_SIZE: %d
    CATCHUP_QUEUE_SIZE: %d
  Privacy:
    PRIVACY_PROXY_URL: %s
    SSO_CLIENT_ID: %s
    SSO_REDIRECT_URI: %s
  OP Stack:
    ENABLE_OP_DEPOSITS: %t
    L1_RPC_URL: %s
    OPTIMISM_PORTAL_ADDRESS: %s
    L1_DEPOSIT_POLL_INTERVAL: %s
    L1_DEPOSIT_BATCH_SIZE: %d
    L1_DEPOSIT_START_BLOCK: %d
}`,
		c.RPCURL, maskedDBURL, c.APIPort, c.PollInterval, c.StartBlock,
		c.RPCWorkers, c.RPCRateLimit, c.DBBatchSize, c.TokenMetadataWorkers, c.BalanceWorkers, c.EnableAsyncBalance,
		c.EnableTracing, c.TraceRateLimit, c.TraceWorkers,
		c.WSMaxConnections, c.WSPingInterval,
		c.SolcPath, c.UseSourcifyFallback,
		c.MetricsEnabled, c.MetricsListenAddr, c.PprofEnabled, c.PprofListenAddr,
		c.CatchupEnabled, c.CatchupWorkers, c.CatchupBatchSize, c.CatchupQueueSize,
		c.PrivacyProxyURL, c.SSOClientID, c.SSORedirectURI,
		c.EnableOPDeposits, c.L1RPCURL, c.OptimismPortalAddress, c.L1DepositPollInterval, c.L1DepositBatchSize, c.L1DepositStartBlock,
	)
}

func maskDatabasePassword(url string) string {
	start := strings.Index(url, "://")
	if start == -1 {
		return url
	}
	atSign := strings.Index(url[start:], "@")
	if atSign == -1 {
		return url
	}

	userPassPart := url[start+3 : start+atSign]
	colonPos := strings.Index(userPassPart, ":")
	if colonPos == -1 {
		return url
	}

	return url[:start+3+colonPos+1] + "****" + url[start+atSign:]
}
