package indexerv2

import (
	"bytes"
	"testing"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
)

func TestCurrentCountSelectorsAreClosedFiniteAndEmptyMeansNone(t *testing.T) {
	a, b := bytes.Repeat([]byte{1}, 20), bytes.Repeat([]byte{2}, 20)
	valid := func(set *pb.AddressSet) *pb.CurrentCountSelector {
		return &pb.CurrentCountSelector{Value: &pb.CurrentCountSelector_Addresses{Addresses: &pb.CurrentAddresses{Addresses: set}}}
	}
	for _, x := range []*pb.CurrentCountSelector{valid(&pb.AddressSet{}), valid(&pb.AddressSet{Addresses: [][]byte{a, b}})} {
		if _, e := currentCountAddresses(x); e != nil {
			t.Fatal(e)
		}
	}
	for _, x := range []*pb.CurrentCountSelector{nil, {}, valid(nil), valid(&pb.AddressSet{Addresses: [][]byte{a, a}}), valid(&pb.AddressSet{Addresses: [][]byte{b, a}}), valid(&pb.AddressSet{Addresses: [][]byte{{1}}}), {Value: &pb.CurrentCountSelector_Tokens{Tokens: &pb.CurrentTokens{Addresses: &pb.AddressSet{}, TokenType: "ignored"}}}, {Value: &pb.CurrentCountSelector_Holders{Holders: &pb.CurrentHolders{Addresses: &pb.AddressSet{}}}}} {
		if _, e := currentCountAddresses(x); e == nil {
			t.Fatalf("invalid source selector accepted: %v", x)
		}
	}
}
