package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

type nftMove struct {
	id, from, to string
	uri          *string
}

// nftForkBlock is block number of fork, with one transaction moving each NFT of the 0x4f7 collection in moves.
func nftForkBlock(fork string, number uint64, moves ...nftMove) *BlockData {
	hash := func(n uint64) string {
		if n < 103 {
			return fmt.Sprintf("nft-%d", n)
		}
		return fmt.Sprintf("nft-%s-%d", fork, n)
	}
	txHash := "tx-" + hash(number)
	to := "0x4f7"
	b := &BlockData{
		Block:            &types.Block{Number: number, Hash: hash(number), ParentHash: hash(number - 1), Timestamp: 1_700_000_000 + number},
		Transactions:     []*types.Transaction{{Hash: txHash, BlockNumber: number, From: "0xa", To: &to, Value: "0", Status: 1}},
		SkipAddressStats: true,
	}
	for k, m := range moves {
		transferType := types.TransferTypeTransfer
		switch {
		case m.from == zeroAddress:
			transferType = types.TransferTypeMint
		case m.to == zeroAddress:
			transferType = types.TransferTypeBurn
		}
		id := m.id
		b.Transfers = append(b.Transfers, &types.TokenTransfer{TxHash: txHash, LogIndex: k, TokenAddress: "0x4f7", From: m.from, To: m.to,
			Value: "1", BlockNumber: number, TransferType: transferType, TokenType: types.TokenTypeERC721, TokenID: &id})
		b.NFTTokens = append(b.NFTTokens, &types.NFTToken{TokenAddress: "0x4f7", TokenID: m.id, Owner: m.to, TokenURI: m.uri, BlockNumber: number})
	}
	return b
}

type nftRow struct {
	owner string
	block int64
	uri   *string
}

func nftRowOf(t *testing.T, d *DB, id string) *nftRow {
	t.Helper()
	var r nftRow
	err := d.pool.QueryRow(context.Background(),
		`SELECT owner, block_number, token_uri FROM nft_tokens WHERE token_address = '0x4f7' AND token_id = $1`, id).Scan(&r.owner, &r.block, &r.uri)
	if err != nil {
		require.ErrorContains(t, err, "no rows")
		return nil
	}
	return &r
}

// A reorg reverts 103..105 and the new chain is written: every NFT ends as the new chain leaves it, whatever the
// reverted fork did to it.
func TestReorgRevertsNFTOwners(t *testing.T) {
	oldURI, newURI := "ipfs://old", "ipfs://new"
	for _, tc := range []struct {
		name        string
		old, new    map[uint64][]nftMove
		pruneBefore bool
		reverted    *nftRow
		want        *nftRow
	}{{
		name:     "moved on the reverted fork, moved elsewhere on the new chain",
		old:      map[uint64][]nftMove{105: {{id: "1", from: "0xo", to: "0xa"}}},
		new:      map[uint64][]nftMove{104: {{id: "1", from: "0xo", to: "0xb"}}},
		reverted: &nftRow{owner: "0xo", block: 101},
		want:     &nftRow{owner: "0xb", block: 104},
	}, {
		name:     "minted on the reverted fork, minted again on the new chain",
		old:      map[uint64][]nftMove{104: {{id: "2", from: zeroAddress, to: "0xa", uri: &oldURI}}},
		new:      map[uint64][]nftMove{104: {{id: "2", from: zeroAddress, to: "0xb", uri: &newURI}}},
		reverted: nil,
		want:     &nftRow{owner: "0xb", block: 104, uri: &newURI},
	}, {
		name:     "moved on the reverted fork, burned on the new chain",
		old:      map[uint64][]nftMove{105: {{id: "1", from: "0xo", to: "0xa"}}},
		new:      map[uint64][]nftMove{104: {{id: "1", from: "0xo", to: zeroAddress}}},
		reverted: &nftRow{owner: "0xo", block: 101},
		want:     &nftRow{owner: zeroAddress, block: 104},
	}, {
		name:     "moved twice on the reverted fork, not on the new chain",
		old:      map[uint64][]nftMove{103: {{id: "1", from: "0xo", to: "0xa"}}, 105: {{id: "1", from: "0xa", to: "0xc"}}},
		new:      map[uint64][]nftMove{},
		reverted: &nftRow{owner: "0xo", block: 101},
		want:     &nftRow{owner: "0xo", block: 101},
	}, {
		name:        "its earlier transfers pruned, moved on the reverted fork, moved elsewhere on the new chain",
		old:         map[uint64][]nftMove{105: {{id: "1", from: "0xo", to: "0xa"}}},
		new:         map[uint64][]nftMove{104: {{id: "1", from: "0xo", to: "0xb"}}},
		pruneBefore: true,
		reverted:    &nftRow{owner: "0xo", block: 102},
		want:        &nftRow{owner: "0xb", block: 104},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			mintURI := "ipfs://minted"
			for n := uint64(100); n <= 102; n++ {
				var moves []nftMove
				if n == 101 {
					moves = []nftMove{{id: "1", from: zeroAddress, to: "0xo", uri: &mintURI}}
				}
				require.NoError(t, d.InsertBlockDataBatch(ctx, nftForkBlock("", n, moves...)))
			}
			if tc.pruneBefore {
				_, err := d.pool.Exec(ctx, `DELETE FROM token_transfers WHERE block_number < 103`)
				require.NoError(t, err)
			}
			for n := uint64(103); n <= 105; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, nftForkBlock("old", n, tc.old[n]...)))
			}

			for n := uint64(105); n >= 103; n-- {
				require.NoError(t, d.DeleteBlock(ctx, n))
				require.NoError(t, d.DeleteBlock(ctx, n), "deleting the block again, after a lost commit acknowledgement, changes nothing")
			}
			id := "1"
			if tc.reverted == nil {
				id = "2"
			}
			for _, m := range tc.new[104] {
				id = m.id
			}
			got := nftRowOf(t, d, id)
			if tc.reverted == nil {
				require.Nil(t, got, "the NFT minted on the reverted fork is gone")
			} else {
				require.NotNil(t, got)
				require.Equal(t, tc.reverted.owner, got.owner, "the owner before the reverted fork")
				require.Equal(t, tc.reverted.block, got.block)
			}

			for n := uint64(103); n <= 105; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, nftForkBlock("new", n, tc.new[n]...)))
			}
			got = nftRowOf(t, d, id)
			require.NotNil(t, got)
			require.Equal(t, tc.want.owner, got.owner, "the new chain's owner")
			require.Equal(t, tc.want.block, got.block)
			if id == "1" {
				require.NotNil(t, got.uri)
				require.Equal(t, mintURI, *got.uri, "the mint's token URI is kept")
			} else {
				require.NotNil(t, got.uri)
				require.Equal(t, *tc.want.uri, *got.uri, "the new chain's mint's token URI, not the reverted one's")
			}
		})
	}
}
