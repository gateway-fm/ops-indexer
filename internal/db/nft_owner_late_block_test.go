package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func nftMoveBlock(number uint64, owner string, uri *string) *BlockData {
	return &BlockData{
		Block: &types.Block{Number: number, Hash: fmt.Sprintf("nft-%d", number), ParentHash: fmt.Sprintf("nft-%d", number-1), Timestamp: 1_700_000_000 + number},
		NFTTokens: []*types.NFTToken{{TokenAddress: "0x4f7", TokenID: "1", Owner: owner, TokenURI: uri,
			BlockNumber: number}},
	}
}

// A block written after a later one (catch-up filling a gap out of order, or the standard loop retrying a recorded
// block) does not overwrite the later block's owner of an NFT; its token URI still fills one never recorded.
func TestOlderBlockDoesNotOverwriteANewerNFTOwner(t *testing.T) {
	uri := "ipfs://minted"
	for _, write := range []struct {
		name string
		fn   func(ctx context.Context, d *DB, b *BlockData) error
	}{
		{"batch", func(ctx context.Context, d *DB, b *BlockData) error { return d.InsertBlockDataBatch(ctx, b) }},
		{"pipelined", func(ctx context.Context, d *DB, b *BlockData) error {
			w, err := d.BeginBlockData(ctx, b)
			if err != nil {
				return err
			}
			return w.Finish(ctx)
		}},
	} {
		t.Run(write.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			owner := func() (string, int64, *string) {
				var o string
				var b int64
				var u *string
				require.NoError(t, d.pool.QueryRow(ctx,
					`SELECT owner, block_number, token_uri FROM nft_tokens WHERE token_address = '0x4f7' AND token_id = '1'`).Scan(&o, &b, &u))
				return o, b, u
			}

			require.NoError(t, write.fn(ctx, d, nftMoveBlock(90, "0xb", nil)))
			require.NoError(t, write.fn(ctx, d, nftMoveBlock(83, "0xa", &uri)))
			o, b, u := owner()
			require.Equal(t, "0xb", o, "block 90's owner stays")
			require.EqualValues(t, 90, b)
			require.NotNil(t, u)
			require.Equal(t, uri, *u, "the mint's token URI, never recorded before, is kept")

			require.NoError(t, write.fn(ctx, d, nftMoveBlock(95, "0xc", nil)))
			o, b, _ = owner()
			require.Equal(t, "0xc", o, "a later block still moves it")
			require.EqualValues(t, 95, b)
		})
	}
}
