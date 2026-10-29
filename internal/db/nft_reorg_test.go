package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// The indexer writes addresses in checksum case, as these are.
var (
	nftCollection = common.HexToAddress("0x4f7abcdef00000000000000000000000000c0de1").Hex()
	nftOwnerO     = common.HexToAddress("0xabcdef000000000000000000000000000000000a").Hex()
	nftOwnerA     = common.HexToAddress("0xbeef00000000000000000000000000000000abcd").Hex()
	nftOwnerB     = common.HexToAddress("0xcafe0000000000000000000000000000000fade1").Hex()
	nftOwnerC     = common.HexToAddress("0xdead0000000000000000000000000000000bea70").Hex()
)

type nftMove struct {
	id, from, to string
	uri          *string
}

// nftForkBlock is block number of fork, with one transaction moving each NFT of nftCollection in moves.
func nftForkBlock(fork string, number uint64, moves ...nftMove) *BlockData {
	hash := func(n uint64) string {
		if n < 103 {
			return fmt.Sprintf("nft-%d", n)
		}
		return fmt.Sprintf("nft-%s-%d", fork, n)
	}
	txHash := "tx-" + hash(number)
	b := &BlockData{
		Block:            &types.Block{Number: number, Hash: hash(number), ParentHash: hash(number - 1), Timestamp: 1_700_000_000 + number},
		Transactions:     []*types.Transaction{{Hash: txHash, BlockNumber: number, From: nftOwnerO, To: &nftCollection, Value: "0", Status: 1}},
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
		b.Transfers = append(b.Transfers, &types.TokenTransfer{TxHash: txHash, LogIndex: k, TokenAddress: nftCollection, From: m.from, To: m.to,
			Value: "1", BlockNumber: number, TransferType: transferType, TokenType: types.TokenTypeERC721, TokenID: &id})
		b.NFTTokens = append(b.NFTTokens, &types.NFTToken{TokenAddress: nftCollection, TokenID: m.id, Owner: m.to, TokenURI: m.uri, BlockNumber: number})
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
		`SELECT owner, block_number, token_uri FROM nft_tokens WHERE LOWER(token_address) = LOWER($1) AND token_id = $2::NUMERIC`,
		nftCollection, id).Scan(&r.owner, &r.block, &r.uri)
	if err != nil {
		require.ErrorContains(t, err, "no rows")
		return nil
	}
	return &r
}

// A reorg reverts 103..105 and the new chain is written: every NFT ends as the new chain leaves it, whatever the
// reverted fork did to it, with its owner in the case the indexer writes.
func TestReorgRevertsNFTOwners(t *testing.T) {
	require.NotEqual(t, strings.ToLower(nftCollection), nftCollection, "a collection address in mixed case")
	oldURI, newURI := "ipfs://old", "ipfs://new"
	for _, tc := range []struct {
		name        string
		old, new    map[uint64][]nftMove
		pruneBefore bool
		burnBefore  bool
		reverted    *nftRow
		want        *nftRow
	}{{
		name:     "moved on the reverted fork, moved elsewhere on the new chain",
		old:      map[uint64][]nftMove{105: {{id: "1", from: nftOwnerO, to: nftOwnerA}}},
		new:      map[uint64][]nftMove{104: {{id: "1", from: nftOwnerO, to: nftOwnerB}}},
		reverted: &nftRow{owner: nftOwnerO, block: 101},
		want:     &nftRow{owner: nftOwnerB, block: 104},
	}, {
		name:     "moved on the reverted fork only",
		old:      map[uint64][]nftMove{105: {{id: "1", from: nftOwnerO, to: nftOwnerA}}},
		new:      map[uint64][]nftMove{},
		reverted: &nftRow{owner: nftOwnerO, block: 101},
		want:     &nftRow{owner: nftOwnerO, block: 101},
	}, {
		name:     "minted on the reverted fork, minted again on the new chain",
		old:      map[uint64][]nftMove{104: {{id: "2", from: zeroAddress, to: nftOwnerA, uri: &oldURI}}},
		new:      map[uint64][]nftMove{104: {{id: "2", from: zeroAddress, to: nftOwnerB, uri: &newURI}}},
		reverted: nil,
		want:     &nftRow{owner: nftOwnerB, block: 104, uri: &newURI},
	}, {
		name:     "minted on the reverted fork only",
		old:      map[uint64][]nftMove{104: {{id: "2", from: zeroAddress, to: nftOwnerA, uri: &oldURI}}},
		new:      map[uint64][]nftMove{},
		reverted: nil,
		want:     nil,
	}, {
		name:     "burned on the reverted fork only",
		old:      map[uint64][]nftMove{105: {{id: "1", from: nftOwnerO, to: zeroAddress}}},
		new:      map[uint64][]nftMove{},
		reverted: &nftRow{owner: nftOwnerO, block: 101},
		want:     &nftRow{owner: nftOwnerO, block: 101},
	}, {
		name:     "moved on the reverted fork, burned on the new chain",
		old:      map[uint64][]nftMove{105: {{id: "1", from: nftOwnerO, to: nftOwnerA}}},
		new:      map[uint64][]nftMove{104: {{id: "1", from: nftOwnerO, to: zeroAddress}}},
		reverted: &nftRow{owner: nftOwnerO, block: 101},
		want:     &nftRow{owner: zeroAddress, block: 104},
	}, {
		name:     "moved twice on the reverted fork, not on the new chain",
		old:      map[uint64][]nftMove{103: {{id: "1", from: nftOwnerO, to: nftOwnerA}}, 105: {{id: "1", from: nftOwnerA, to: nftOwnerC}}},
		new:      map[uint64][]nftMove{},
		reverted: &nftRow{owner: nftOwnerO, block: 101},
		want:     &nftRow{owner: nftOwnerO, block: 101},
	}, {
		name:       "burned before the fork, minted again on the reverted fork only",
		old:        map[uint64][]nftMove{104: {{id: "1", from: zeroAddress, to: nftOwnerA}}},
		new:        map[uint64][]nftMove{},
		burnBefore: true,
		reverted:   &nftRow{owner: zeroAddress, block: 102},
		want:       &nftRow{owner: zeroAddress, block: 102},
	}, {
		name:        "its earlier transfers pruned, moved on the reverted fork, moved elsewhere on the new chain",
		old:         map[uint64][]nftMove{105: {{id: "1", from: nftOwnerO, to: nftOwnerA}}},
		new:         map[uint64][]nftMove{104: {{id: "1", from: nftOwnerO, to: nftOwnerB}}},
		pruneBefore: true,
		reverted:    &nftRow{owner: nftOwnerO, block: 102},
		want:        &nftRow{owner: nftOwnerB, block: 104},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			mintURI := "ipfs://minted"
			for n := uint64(100); n <= 102; n++ {
				var moves []nftMove
				if n == 101 {
					moves = []nftMove{{id: "1", from: zeroAddress, to: nftOwnerO, uri: &mintURI}}
				}
				if n == 102 && tc.burnBefore {
					moves = []nftMove{{id: "1", from: nftOwnerO, to: zeroAddress}}
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
			for _, moves := range tc.old {
				for _, m := range moves {
					id = m.id
				}
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
			if tc.want == nil {
				require.Nil(t, got, "the new chain never minted it")
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.want.owner, got.owner, "the new chain's owner")
			require.Equal(t, tc.want.block, got.block)
			require.NotNil(t, got.uri)
			if id == "1" {
				require.Equal(t, mintURI, *got.uri, "the mint's token URI is kept")
			} else {
				require.Equal(t, *tc.want.uri, *got.uri, "the new chain's mint's token URI, not the reverted one's")
			}

			items, total, err := d.GetTokenInventory(ctx, nftCollection, id, 10, 0)
			require.NoError(t, err)
			if tc.want.owner == zeroAddress {
				require.Zero(t, total, "a burned NFT is not listed")
				return
			}
			require.EqualValues(t, 1, total)
			require.Equal(t, tc.want.owner, items[0].Owner, "the inventory serves the owner as the indexer writes it")
		})
	}
}
