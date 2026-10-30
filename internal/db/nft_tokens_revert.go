package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// revertNFTTokens takes a reverted block off nft_tokens, before its transfers are deleted. The writer keeps the owner
// of the highest block written, so a reverted fork's owner would outlive the reorg: every row at or above the block
// goes back to the token's latest transfer in any other block. With none remaining, a token minted in the block loses
// its row; one whose earlier transfers are pruned goes back to the sender of its first transfer in the block, and one
// the block did not move keeps its owner. token_transfers holds addresses in lower case and nft_tokens in the case the
// indexer writes them, checksum case, so the transfers are looked up by the lowered collection address and a restored
// owner is written back in checksum case. Rows are locked in the order the writer locks them, the address and then the
// decimal id compared byte by byte as Go compares strings ("10" before "9"), whatever the database's collation.
func revertNFTTokens(ctx context.Context, tx pgx.Tx, number uint64) error {
	rows, err := tx.Query(ctx, `
		SELECT token_address, token_id::TEXT FROM nft_tokens WHERE block_number >= $1
		ORDER BY token_address COLLATE "C", token_id::TEXT COLLATE "C" FOR UPDATE`, int64(number))
	if err != nil {
		return err
	}
	type nft struct{ token, id string }
	moved, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (nft, error) {
		var n nft
		return n, r.Scan(&n.token, &n.id)
	})
	if err != nil {
		return err
	}

	below := int64(number) - 1
	for _, n := range moved {
		collection := strings.ToLower(n.token)
		var owner string
		var block int64
		err := tx.QueryRow(ctx, `
			SELECT to_address, block_number FROM token_transfers
			WHERE token_address = $1 AND token_id = $2::NUMERIC AND token_type = 'ERC721' AND block_number <> $3
			ORDER BY block_number DESC, log_index DESC LIMIT 1`, collection, n.id, int64(number)).Scan(&owner, &block)
		if err == pgx.ErrNoRows {
			var from string
			err = tx.QueryRow(ctx, `
				SELECT from_address FROM token_transfers
				WHERE block_number = $3 AND token_address = $1 AND token_id = $2::NUMERIC AND token_type = 'ERC721'
				ORDER BY log_index LIMIT 1`, collection, n.id, int64(number)).Scan(&from)
			switch {
			case err == pgx.ErrNoRows:
				_, err = tx.Exec(ctx, `
					UPDATE nft_tokens SET block_number = $3, updated_at = NOW()
					WHERE token_address = $1 AND token_id = $2::NUMERIC`, n.token, n.id, below)
				if err != nil {
					return err
				}
				continue
			case err != nil:
				return err
			case from == zeroAddress:
				if _, err := tx.Exec(ctx, `DELETE FROM nft_tokens WHERE token_address = $1 AND token_id = $2::NUMERIC`, n.token, n.id); err != nil {
					return err
				}
				continue
			}
			owner, block = from, below
		} else if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE nft_tokens SET owner = $3, block_number = $4, updated_at = NOW()
			WHERE token_address = $1 AND token_id = $2::NUMERIC`, n.token, n.id, common.HexToAddress(owner).Hex(), block); err != nil {
			return err
		}
	}
	return nil
}
