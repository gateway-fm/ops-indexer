package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/config"
)

func TestConfigReorgFloorIsTheDatabases(t *testing.T) {
	require.Equal(t, uint64(PruneFloorGap), uint64(config.ReorgFloorBlocks))
}
