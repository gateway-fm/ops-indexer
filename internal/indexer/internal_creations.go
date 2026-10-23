package indexer

import (
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/types"
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
