package indexerv2

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Text key collation and byte keys agree only for the canonical lowercase
// representation written by ingest. Returning a mixed-case SQL key would make
// a lowercased typed continuation skip or repeat rows.
func TestFactKeyRequiresCanonicalStorageRepresentation(t *testing.T) {
	for _, family := range []string{"transactions", "internals", "contracts", "tokens", "addresses", "balances", "holders"} {
		t.Run(family, func(t *testing.T) {
			v := map[string]any{"_block": int64(5), "_index": int32(0), "_hash": "0x" + strings.Repeat("ab", 32), "_trace": "[0]", "_address": "0x" + strings.Repeat("ab", 20), "_token": "0x" + strings.Repeat("cd", 20), "_balance": "9"}
			if _, e := makeKey(family, v); e != nil {
				t.Fatal(e)
			}
			if family == "transactions" || family == "internals" {
				v["_hash"] = "0x" + strings.Repeat("AB", 32)
			} else {
				v["_address"] = "0x" + strings.Repeat("AB", 20)
			}
			if _, e := makeKey(family, v); status.Code(e) != codes.DataLoss {
				t.Fatalf("inconsistent text/byte key accepted: %v", e)
			}
		})
	}
}

func TestHolderBalancesRejectMalformedFactAndPosition(t *testing.T) {
	overflow := new(big.Int).Lsh(big.NewInt(1), 256)
	maximum := new(big.Int).Sub(new(big.Int).Set(overflow), big.NewInt(1)).String()
	for _, amount := range []string{maximum, overflow.String(), "-1", "00", "+1", "1e2", ""} {
		t.Run(amount, func(t *testing.T) {
			v := map[string]any{"_address": "0x" + strings.Repeat("ab", 20), "_balance": amount}
			key, e := makeKey("holders", v)
			valid := amount == maximum
			if valid {
				if e != nil {
					t.Fatal(e)
				}
				if _, e = keyValues("holders", key); e != nil {
					t.Fatal(e)
				}
			} else {
				if status.Code(e) != codes.DataLoss {
					t.Fatalf("badholderfact: %v", e)
				}
				key = &pb.Key{Value: &pb.Key_Holder{Holder: &pb.HolderKey{Address: bytes.Repeat([]byte{0xab}, 20), Balance: amount}}}
				if _, e = keyValues("holders", key); status.Code(e) != codes.InvalidArgument {
					t.Fatalf("badholderposition: %v", e)
				}
			}
		})
	}
}
