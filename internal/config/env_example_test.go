package config

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// An operator who copies .env.example as .env gets the built-in defaults for these settings, not older values.
func TestEnvExampleCarriesTheDefaults(t *testing.T) {
	raw, err := os.ReadFile("../../.env.example")
	require.NoError(t, err)
	example := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(k, "#") {
			example[k] = v
		}
	}
	v := viper.New()
	setDefaults(v)
	for _, key := range []string{"REALTIME_MAX_BEHIND_BLOCKS", "LEDGER_COMPACT_SPANS", "VERIFY_RECOUNT_INTERVAL",
		"VERIFY_REPAIR", "VERIFY_NODE_RECOUNT_RPS", "VERIFY_CHAIN_RPS", "VERIFY_CHAIN_SAMPLE", "VERIFY_CHAIN_INTERVAL",
		"STATS_BACKFILL_RPS", "STATS_BACKFILL_WORKERS", "STATS_BACKFILL_BATCH_BLOCKS"} {
		got, ok := example[key]
		require.True(t, ok, "%s is in .env.example", key)
		require.Equal(t, fmt.Sprint(v.Get(strings.ToLower(key))), got, key)
	}
	require.Contains(t, string(raw), "repair-address-stats [--batch 100]")
	require.NotContains(t, string(raw), "cannot be combined with retention")
}
