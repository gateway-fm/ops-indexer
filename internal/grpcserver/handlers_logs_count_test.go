package grpcserver

import (
	"context"
	"strings"
	"testing"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// TestListLogs_TotalCount_FilterAccuracy pins that ListLogs.total_count is the
// exact filter-wide count — not the page length, and not the whole table —
// across the topic-only, address-only and multi-filter (CountLogs) paths.
// The dataset is salted with decoys that violate each filter dimension so a
// count that matched the wrong predicate (the failure mode in PR #18's
// by-address rewrite) would show up as a wrong number.
func TestListLogs_TotalCount_FilterAccuracy(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()

	const (
		X = "0xc0ffee0000000000000000000000000000000001"
		Y = "0xdddd000000000000000000000000000000000002"
	)
	T := "0x" + strings.Repeat("a", 64)
	U := "0x" + strings.Repeat("b", 64)

	// (block, logIndex, address, topic0)
	seed := []struct {
		block    uint64
		logIndex int
		addr     string
		topic    string
	}{
		{1, 0, X, T}, // matches T; X
		{1, 1, X, T}, // matches T; X   (2 logs in block 1's tx)
		{2, 0, X, T}, // matches T; X
		{3, 0, X, U}, // X but topic U (decoy for topic, matches address-X)
		{4, 0, Y, T}, // topic T but address Y (decoy for address)
		{5, 0, Y, U}, // neither
	}
	for _, s := range seed {
		tp := s.topic
		if err := h.db.InsertLog(ctx, &types.Log{
			TxHash: txHash(s.block), LogIndex: s.logIndex, Address: s.addr,
			Topic0: &tp, Data: "0x", BlockNumber: s.block,
		}); err != nil {
			t.Fatalf("InsertLog %+v: %v", s, err)
		}
	}

	cases := []struct {
		name string
		req  *indexerv1.ListLogsRequest
		want int64
	}{
		{
			// topic-only → every log with topic T, any address: L1,L2,L3(b2),L?  -> blocks 1,1,2,4 = 4
			name: "topic only",
			req:  &indexerv1.ListLogsRequest{Topic0: T, Page: &indexerv1.PageRequest{PageSize: 2}},
			want: 4,
		},
		{
			// address-only → every log at X, any topic: blocks 1,1,2 (T) + 3 (U) = 4
			name: "address only",
			req:  &indexerv1.ListLogsRequest{ByAddress: X, Page: &indexerv1.PageRequest{PageSize: 2}},
			want: 4,
		},
		{
			// multi-filter (CountLogs): X AND T, any block: 1,1,2 = 3
			name: "address + topic",
			req:  &indexerv1.ListLogsRequest{ByAddress: X, Topic0: T, Page: &indexerv1.PageRequest{PageSize: 2}},
			want: 3,
		},
		{
			// multi-filter + block range [2,5]: of the X+T logs, only block 2 = 1
			name: "address + topic + range",
			req: &indexerv1.ListLogsRequest{
				ByAddress: X, Topic0: T,
				BlockRange: &indexerv1.BlockRange{FromBlock: 2, ToBlock: 5},
				Page:       &indexerv1.PageRequest{PageSize: 2},
			},
			want: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := h.client.ListLogs(ctx, c.req)
			if err != nil {
				t.Fatalf("ListLogs: %v", err)
			}
			if got := resp.GetTotalCount(); got != c.want {
				t.Errorf("total_count = %d, want %d (page returned %d logs)", got, c.want, len(resp.GetLogs()))
			}
		})
	}
}
