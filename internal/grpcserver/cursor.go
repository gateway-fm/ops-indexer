package grpcserver

import (
	"encoding/base64"
	"encoding/json"
)

// Cursors are opaque strings the server encodes. Clients pass them back
// verbatim. The current encoding is base64(JSON(struct)). Implementation
// details are private — any encoding change is backwards-compatible as long
// as the decoder tolerates unknown fields and legacy shapes.

// txFeedCursor paginates the main transaction feed, ordered by
// (block_number DESC, transaction_index DESC).
type txFeedCursor struct {
	BlockNumber      uint64 `json:"b"`
	TransactionIndex uint32 `json:"t"`
}

// blockFeedCursor paginates the blocks feed ordered by (number DESC).
type blockFeedCursor struct {
	BlockNumber uint64 `json:"b"`
}

// logFeedCursor paginates logs ordered by (block_number DESC, log_index DESC).
type logFeedCursor struct {
	BlockNumber uint64 `json:"b"`
	LogIndex    uint32 `json:"l"`
}

// transferFeedCursor paginates token transfers ordered by
// (block_number DESC, log_index DESC).
type transferFeedCursor struct {
	BlockNumber uint64 `json:"b"`
	LogIndex    uint32 `json:"l"`
}

// internalTxFeedCursor paginates internal txs ordered by
// (block_number DESC, trace_address).
type internalTxFeedCursor struct {
	BlockNumber  uint64 `json:"b"`
	TraceAddress string `json:"t"`
}

func encodeCursor(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeCursor(s string, out any) error {
	if s == "" {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return invalidArgument("cursor malformed")
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return invalidArgument("cursor malformed")
	}
	return nil
}
