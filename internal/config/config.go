package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	RPCURL       string        `mapstructure:"rpc_url"`
	DatabaseURL  string        `mapstructure:"database_url"`
	APIPort      int           `mapstructure:"api_port"`
	PollInterval time.Duration `mapstructure:"poll_interval"`
	StartBlock   uint64        `mapstructure:"start_block"`

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

	MetricsEnabled bool `mapstructure:"metrics_enabled"`

	CatchupEnabled   bool `mapstructure:"catchup_enabled"`
	CatchupWorkers   int  `mapstructure:"catchup_workers"`
	CatchupBatchSize int  `mapstructure:"catchup_batch_size"`
	CatchupQueueSize int  `mapstructure:"catchup_queue_size"`

	// Set PRIVACY_PROXY_URL to enable proxy mode (auth + privacy enforcement).
	// Leave empty for standalone mode (direct DB/RPC access).
	PrivacyProxyURL       string `mapstructure:"privacy_proxy_url"`
	// PrivacyProxyPublicURL is the browser-facing URL for OAuth redirects.
	// Defaults to PrivacyProxyURL if not set. Set this when the proxy is on a
	// Docker-internal hostname (e.g. privacy-proxy-proxy-backend-1) but the
	// browser needs to reach it via localhost or a public hostname.
	PrivacyProxyPublicURL string `mapstructure:"privacy_proxy_public_url"`
	SSOClientID           string `mapstructure:"sso_client_id"`
	SSORedirectURI        string `mapstructure:"sso_redirect_uri"`

	LogLevel              string        `mapstructure:"log_level"`

	// HiddenTxTypes is a comma-separated list of transaction type numbers to
	// exclude from the default transaction listings (e.g. "126" for OP deposit TXs).
	HiddenTxTypes         string        `mapstructure:"hidden_tx_types"`

	EnableOPDeposits      bool          `mapstructure:"enable_op_deposits"`
	L1RPCURL              string        `mapstructure:"l1_rpc_url"`
	OptimismPortalAddress string        `mapstructure:"optimism_portal_address"`
	L1DepositPollInterval time.Duration `mapstructure:"l1_deposit_poll_interval"`
	L1DepositBatchSize    int           `mapstructure:"l1_deposit_batch_size"`
	L1DepositStartBlock   uint64        `mapstructure:"l1_deposit_start_block"`
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("rpc_url", "http://127.0.0.1:8545")
	v.SetDefault("database_url", "postgres://postgres:postgres@localhost:5432/explorer?sslmode=disable")
	v.SetDefault("api_port", 8080)
	v.SetDefault("poll_interval", "2s")
	v.SetDefault("start_block", 0)

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

	v.SetDefault("catchup_enabled", true)
	v.SetDefault("catchup_workers", 10)
	v.SetDefault("catchup_batch_size", 100)
	v.SetDefault("catchup_queue_size", 1000)

	v.SetDefault("privacy_proxy_url", "")
	v.SetDefault("privacy_proxy_public_url", "")
	v.SetDefault("sso_client_id", "explorer")
	v.SetDefault("sso_redirect_uri", "http://localhost:8080/api/auth/callback")

	v.SetDefault("log_level", "info")

	v.SetDefault("hidden_tx_types", "126") // OP deposit system transactions hidden by default

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
	cfg.APIPort = v.GetInt("api_port")
	cfg.PollInterval = v.GetDuration("poll_interval")
	cfg.StartBlock = v.GetUint64("start_block")

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

	cfg.CatchupEnabled = v.GetBool("catchup_enabled")
	cfg.CatchupWorkers = v.GetInt("catchup_workers")
	cfg.CatchupBatchSize = v.GetInt("catchup_batch_size")
	cfg.CatchupQueueSize = v.GetInt("catchup_queue_size")

	cfg.PrivacyProxyURL = v.GetString("privacy_proxy_url")
	cfg.PrivacyProxyPublicURL = v.GetString("privacy_proxy_public_url")
	if cfg.PrivacyProxyPublicURL == "" {
		cfg.PrivacyProxyPublicURL = cfg.PrivacyProxyURL
	}
	cfg.SSOClientID = v.GetString("sso_client_id")
	cfg.SSORedirectURI = v.GetString("sso_redirect_uri")

	cfg.LogLevel = v.GetString("log_level")

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
		c.MetricsEnabled,
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
