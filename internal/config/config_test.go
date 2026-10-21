package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	return &Config{RPCURL: "http://node", DatabaseURL: "postgres://db", APIPort: 8080, RPCWorkers: 1, RPCRateLimit: 1, LedgerSpanBlocks: 100_000}
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
