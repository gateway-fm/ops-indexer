package db

import "github.com/gateway-fm/chain-indexer/internal/types"

// Transaction category bit flags. Matches migration 003 and docs/API.md.
const (
	CategoryCoinTransfer      = 1 << 0 // native-value transfer with no calldata
	CategoryContractCreation  = 1 << 1 // to=null and the deploy succeeded
	CategoryContractCall      = 1 << 2 // to!=null and input has a method selector
	CategoryTokenTransfer     = 1 << 3 // emitted an ERC20/721/1155 Transfer log
)

// computeTxCategories derives the materialized bitfield for a single
// transaction. hasTokenTransfer tells the function whether any token_transfers
// row was recorded for this tx in the same write batch.
//
// Rules intentionally match the UPDATE in migration 003 so backfilled rows
// and forward-written rows agree.
func computeTxCategories(tx *types.Transaction, hasTokenTransfer bool) int16 {
	if tx == nil {
		return 0
	}
	var c int16

	// coin_transfer: value > 0 and no calldata.
	if tx.Value != "" && tx.Value != "0" && isEmptyCalldata(tx.InputData) {
		c |= CategoryCoinTransfer
	}

	// contract_creation: to is nil and status is success (1).
	if tx.To == nil && tx.Status == 1 {
		c |= CategoryContractCreation
	}

	// contract_call: to is set and input has at least a 4-byte selector.
	if tx.To != nil && hasMethodSelector(tx.InputData) {
		c |= CategoryContractCall
	}

	if hasTokenTransfer {
		c |= CategoryTokenTransfer
	}

	return c
}

func isEmptyCalldata(input string) bool {
	return input == "" || input == "0x" || input == "0X"
}

func hasMethodSelector(input string) bool {
	// "0x" + 8 hex chars = 4 bytes.
	return len(input) >= 10
}

// buildTokenTransferTxSet returns the set of tx hashes that have at least
// one token transfer in the provided slice. Used by the batch insert path to
// set the token_transfer bit atomically with the transaction row.
func buildTokenTransferTxSet(transfers []*types.TokenTransfer) map[string]struct{} {
	if len(transfers) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(transfers))
	for _, t := range transfers {
		if t == nil {
			continue
		}
		m[t.TxHash] = struct{}{}
	}
	return m
}
