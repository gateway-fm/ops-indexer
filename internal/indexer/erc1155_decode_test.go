package indexer

import (
	"math/big"
	"testing"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// abiBatch builds the data payload of a TransferBatch(uint256[],uint256[]) log
// for the given ids/values, mirroring how a node ABI-encodes the two arrays:
// two head offset words, then each array as a length word followed by its
// elements.
func abiBatch(ids, values []int64) []byte {
	word := func(v uint64) []byte {
		b := make([]byte, 32)
		new(big.Int).SetUint64(v).FillBytes(b)
		return b
	}
	encArray := func(xs []int64) []byte {
		out := word(uint64(len(xs)))
		for _, x := range xs {
			out = append(out, word(uint64(x))...)
		}
		return out
	}
	idsEnc := encArray(ids)
	valsEnc := encArray(values)
	// head: offset to ids (always 0x40), offset to values (0x40 + len(idsEnc))
	head := append(word(0x40), word(uint64(0x40+len(idsEnc)))...)
	return append(append(head, idsEnc...), valsEnc...)
}

func TestDecodeERC1155Batch(t *testing.T) {
	tests := []struct {
		name   string
		ids    []int64
		values []int64
	}{
		{"single pair", []int64{7}, []int64{100}},
		{"multiple pairs", []int64{1, 2, 3}, []int64{10, 20, 30}},
		{"empty arrays", []int64{}, []int64{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIDs, gotVals := decodeERC1155Batch(abiBatch(tt.ids, tt.values))
			if len(gotIDs) != len(tt.ids) || len(gotVals) != len(tt.values) {
				t.Fatalf("len = (%d,%d), want (%d,%d)", len(gotIDs), len(gotVals), len(tt.ids), len(tt.values))
			}
			for i := range tt.ids {
				if gotIDs[i].Int64() != tt.ids[i] {
					t.Errorf("id[%d] = %d, want %d", i, gotIDs[i].Int64(), tt.ids[i])
				}
				if gotVals[i].Int64() != tt.values[i] {
					t.Errorf("value[%d] = %d, want %d", i, gotVals[i].Int64(), tt.values[i])
				}
			}
		})
	}
}

func TestDecodeERC1155Batch_Truncated(t *testing.T) {
	// A payload shorter than the two head words must yield nil, not panic.
	if ids, vals := decodeERC1155Batch([]byte{0x01, 0x02}); ids != nil || vals != nil {
		t.Errorf("expected nil slices for truncated data, got ids=%v vals=%v", ids, vals)
	}
}

// TestERC1155TopicHashes pins the event-signature hashes the log loop matches
// against, so a typo can't silently disable ERC-1155 detection.
func TestERC1155TopicHashes(t *testing.T) {
	wantSingle := "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
	wantBatch := "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
	if got := transferSingleTopic.Hex(); got != wantSingle {
		t.Errorf("TransferSingle topic = %s, want %s", got, wantSingle)
	}
	if got := transferBatchTopic.Hex(); got != wantBatch {
		t.Errorf("TransferBatch topic = %s, want %s", got, wantBatch)
	}
	_ = common.Hash{} // keep the common import for parity with the package
}
