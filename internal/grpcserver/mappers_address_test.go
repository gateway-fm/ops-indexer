package grpcserver

import (
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAddressTxCountOutIsTheSentCount(t *testing.T) {
	a := mapAddressStats(&types.AddressStats{Address: "0xa1", TxCount: 5, SentCount: 3})
	require.Equal(t, uint64(5), a.TxCountIn)
	require.Equal(t, uint64(3), a.TxCountOut)
}
