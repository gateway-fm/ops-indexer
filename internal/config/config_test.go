package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	return &Config{RPCURL: "http://node", RPCTimeout: time.Minute, DatabaseURL: "postgres://db", APIPort: 8080, RPCWorkers: 1, RPCRateLimit: 1,
		LedgerSpanBlocks: 100_000, LedgerCloseInterval: 10 * time.Second, PruneInterval: time.Minute, PartitionBlocks: 10_000}
}

func TestValidateRefusesStartBlockWithRetentionUntilTheStatsBackfill(t *testing.T) {
	c := validConfig()
	c.StartBlock, c.PruneRetainBlocks = 1_000_000, 100_000
	require.ErrorContains(t, c.Validate(), "stats backfill")
}

func TestValidateKeepsStartBlockWithoutRetention(t *testing.T) {
	c := validConfig()
	c.StartBlock = 1_000_000
	require.NoError(t, c.Validate())
	c.StartBlock, c.PruneRetainBlocks = 0, 100_000
	require.NoError(t, c.Validate())
}

func TestValidateRefusesAWindowInsideTheReorgFloor(t *testing.T) {
	c := validConfig()
	c.PruneRetainBlocks = 1_000
	require.ErrorContains(t, c.Validate(), "reorg floor")
	c.PruneRetainBlocks = ReorgFloorBlocks
	require.NoError(t, c.Validate())
}

// An interval at or near zero runs the retention pass or the ledger closer back to back against the database.
func TestValidateRefusesRetentionAndLedgerIntervalsBelowASecond(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, 0, time.Millisecond, time.Second - 1} {
		c := validConfig()
		c.PruneInterval = interval
		require.ErrorContains(t, c.Validate(), "PRUNE_INTERVAL", "PRUNE_INTERVAL=%s", interval)
		require.ErrorContains(t, c.Validate(), "at least 1s")
		c = validConfig()
		c.LedgerCloseInterval = interval
		require.ErrorContains(t, c.Validate(), "LEDGER_CLOSE_INTERVAL", "LEDGER_CLOSE_INTERVAL=%s", interval)
		require.ErrorContains(t, c.Validate(), "at least 1s")
	}
	c := validConfig()
	c.PruneInterval, c.LedgerCloseInterval = time.Second, time.Second
	c.LedgerSpanBlocks = 100
	require.NoError(t, c.Validate())
}

// The closer closes one span per interval, so LEDGER_SPAN_BLOCKS / LEDGER_CLOSE_INTERVAL is the fastest the ledger
// can follow the chain; below the chain's rate it falls behind for good and nothing is pruned.
func TestValidateRefusesALedgerSlowerThanTheChain(t *testing.T) {
	c := validConfig()
	c.LedgerCloseInterval = 10 * time.Second
	c.LedgerSpanBlocks = 999
	err := c.Validate()
	require.ErrorContains(t, err, "LEDGER_SPAN_BLOCKS=999")
	require.ErrorContains(t, err, "at least 1000")
	c.LedgerSpanBlocks = 1_000
	require.NoError(t, c.Validate())

	c.LedgerCloseInterval = 90 * time.Second
	require.ErrorContains(t, c.Validate(), "at least 9000")
}

// Partitions are kept two ahead of the head every 10 s; tiny ones are outrun by the chain between two checks.
func TestValidateRefusesTinyPartitionsWithRetention(t *testing.T) {
	for _, size := range []uint64{0, 1, 10, 499} {
		c := validConfig()
		c.PruneRetainBlocks, c.PartitionBlocks = 100_000, size
		require.ErrorContains(t, c.Validate(), "PARTITION_BLOCKS", "PARTITION_BLOCKS=%d", size)
		require.ErrorContains(t, c.Validate(), "at least 500")
	}
	c := validConfig()
	c.PruneRetainBlocks, c.PartitionBlocks = 100_000, 500
	require.NoError(t, c.Validate())
	c.PruneRetainBlocks, c.PartitionBlocks = 0, 1
	require.NoError(t, c.Validate(), "without retention PARTITION_BLOCKS is not used")
}

func TestValidateRefusesAnUnboundedRPCTimeout(t *testing.T) {
	c := validConfig()
	for _, d := range []time.Duration{0, -time.Second} {
		c.RPCTimeout = d
		require.ErrorContains(t, c.Validate(), "RPC_TIMEOUT")
	}
}

func TestRPCTimeoutDefault(t *testing.T) {
	t.Setenv("RPC_URL", "http://node")
	t.Setenv("DATABASE_URL", "postgres://db")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, time.Minute, cfg.RPCTimeout)
}
