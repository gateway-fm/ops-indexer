// Package indexerv2 implements the fixed chain_indexer.v2 read contract. It
// owns no consumer policy, authentication or external database connection.
package indexerv2

import (
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
)

type Service struct {
	pb.UnimplementedIndexerServiceServer
	db     *db.DB
	lookup *nodelookup.Lookup
	ready  func() bool
}

func New(database *db.DB, lookup *nodelookup.Lookup, ready func() bool) *Service {
	return &Service{db: database, lookup: lookup, ready: ready}
}

var _ pb.IndexerServiceServer = (*Service)(nil)
