package indexer

import (
	"context"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// internalCreations are the contracts a transaction deployed from inside a call (a factory's CREATE / CREATE2),
// which no receipt reports: the CREATE frames below the top level that succeeded and whose every enclosing frame
// succeeded too, since a reverted call undoes the creations under it. The top-level CREATE is the receipt's.
func internalCreations(frames []*types.InternalTransaction) []*types.InternalTransaction {
	failed := map[string]bool{}
	for _, f := range frames {
		if f.Error != nil {
			failed[f.TxHash+"/"+f.TraceAddress] = true
		}
	}
	var out []*types.InternalTransaction
	for _, f := range frames {
		if (f.CallType != "create" && f.CallType != "create2") || f.TraceAddress == "" || f.To == nil || f.Error != nil {
			continue
		}
		if !failed[f.TxHash+"/"] && !anyEnclosingFailed(failed, f.TxHash, f.TraceAddress) {
			out = append(out, f)
		}
	}
	return out
}

func anyEnclosingFailed(failed map[string]bool, tx, traceAddress string) bool {
	for i := len(traceAddress) - 1; i > 0; i-- {
		if traceAddress[i] == ',' && failed[tx+"/"+traceAddress[:i]] {
			return true
		}
	}
	return false
}

func strOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return strings.TrimSpace(*s)
}

// contractFromCreation is the contracts row for an internal creation frame, as ingest and the backfill write it:
// the deployed code from the frame's output, else from the node; nil when neither has code.
func contractFromCreation(ctx context.Context, getCode func(context.Context, common.Address) ([]byte, error),
	it *types.InternalTransaction, blockNumber uint64) *types.Contract {
	code := strings.TrimPrefix(strOr(it.Output, ""), "0x")
	if code == "" {
		raw, err := getCode(ctx, common.HexToAddress(*it.To))
		if err != nil || len(raw) == 0 {
			return nil
		}
		code = common.Bytes2Hex(raw)
	}
	bytecodeHash := common.BytesToHash(common.FromHex(code)).Hex()
	return &types.Contract{
		Address:      strings.ToLower(*it.To),
		Bytecode:     code,
		BytecodeHash: &bytecodeHash,
		Creator:      strings.ToLower(it.From),
		CreationTx:   it.TxHash,
		BlockNumber:  blockNumber,
	}
}
