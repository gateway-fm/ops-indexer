package grpcserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
)

func TestBlockReceiptsUnavailableIsAnswered(t *testing.T) {
	err := nodeLookupErr(fmt.Errorf("%w: block 7, 1 of 2 transactions without a receipt", nodelookup.ErrReceiptsUnavailable), "block by number")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "receipts")
}
