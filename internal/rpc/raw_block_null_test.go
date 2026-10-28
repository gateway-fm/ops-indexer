package rpc

import (
	"context"
	"encoding/json"
	"testing"
)

// A node that does not have a block yet answers null. That is "not available", never a block with the zero hash,
// which the reorg check would take for a different chain.
func TestBlockReadsRefuseANullOrHashlessBlock(t *testing.T) {
	for name, result := range map[string]any{
		"null":      nil,
		"no hash":   map[string]any{"number": "0x2a", "parentHash": "0x" + "11" + "00000000000000000000000000000000000000000000000000000000000000", "transactions": []any{}},
		"zero hash": map[string]any{"number": "0x2a", "hash": "0x0000000000000000000000000000000000000000000000000000000000000000", "transactions": []any{}},
	} {
		t.Run(name, func(t *testing.T) {
			c := fakeNode(t, func(string, []json.RawMessage) rpcReply { return rpcReply{result: result} })
			if h, err := c.RawBlockHash(context.Background(), 42); err == nil {
				t.Errorf("RawBlockHash returned %s and no error", h)
			}
			if b, err := c.RawBlockByNumber(context.Background(), 42); err == nil {
				t.Errorf("RawBlockByNumber returned block %v with hash %s and no error", b.Number, b.Hash.Hex())
			}
		})
	}
}
