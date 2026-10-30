package grpcserver

import (
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAddressTxCountOutIsTheSentCount(t *testing.T) {
	a := mapAddressStats(&types.AddressStats{Address: "0xa1", TxCount: 5, SentCount: 3})
	require.Equal(t, uint64(2), a.TxCountIn, "the transactions it did not send")
	require.Equal(t, uint64(3), a.TxCountOut)
}

// tx_count_in + tx_count_out is the address's transaction count; a row whose sent count drifted above it serves 0 in,
// not a wrapped value.
func TestAddressInAndOutSplitTheTransactionCount(t *testing.T) {
	a := mapAddressStats(&types.AddressStats{Address: "0xa", TxCount: 7, SentCount: 3})
	if a.GetTxCountIn() != 4 || a.GetTxCountOut() != 3 {
		t.Fatalf("in %d out %d, want 4 and 3", a.GetTxCountIn(), a.GetTxCountOut())
	}
	drifted := mapAddressStats(&types.AddressStats{Address: "0xb", TxCount: 2, SentCount: 5})
	if drifted.GetTxCountIn() != 0 || drifted.GetTxCountOut() != 5 {
		t.Fatalf("drifted row: in %d out %d, want 0 and 5", drifted.GetTxCountIn(), drifted.GetTxCountOut())
	}
}
