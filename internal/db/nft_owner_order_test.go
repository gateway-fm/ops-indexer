package db

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// Catch-up commits blocks concurrently: two blocks moving the same NFTs in opposite orders must not deadlock.
func TestNFTOwnerUpsertsDoNotDeadlockAcrossConcurrentWriters(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := make([]string, 30)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4*25)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				nfts := make([]*types.NFTToken, len(ids))
				for k := range ids {
					id := ids[k]
					if w%2 == 1 {
						id = ids[len(ids)-1-k]
					}
					nfts[k] = &types.NFTToken{TokenAddress: "0xnft", TokenID: id, Owner: fmt.Sprintf("0x%d", w), BlockNumber: uint64(i)}
				}
				errs <- d.InsertBlockDataBatch(ctx, &BlockData{NFTTokens: nfts})
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
