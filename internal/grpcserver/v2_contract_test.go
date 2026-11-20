package grpcserver

// These tests use the production server registration and a real migrated
// PostgreSQL fixture. There is deliberately no fake V2 paging implementation.
import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/nodelookup"

	indexerv2 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func v2Hash(n uint64) []byte             { b, _ := hex.DecodeString(fmt.Sprintf("%064x", n)); return b }
func v2Address(hexAddress string) []byte { b, _ := hex.DecodeString(hexAddress); return b }

func v2Resume(p *indexerv2.PageInfo) *indexerv2.ReadContext {
	f := p.GetRunFloorBlock()
	return &indexerv2.ReadContext{ReadAt: p.GetReadAt(), ExpectDatasetEpoch: p.GetDatasetEpoch(), ExpectedRunFloor: &f}
}

func v2RequireReason(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("code=%s, want %s (%s): %v", status.Code(err), code, reason, err)
	}
	for _, d := range status.Convert(err).Details() {
		if e, ok := d.(*errdetails.ErrorInfo); ok && e.GetDomain() == "indexer.ops" && e.GetReason() == reason {
			return
		}
	}
	t.Fatalf("missing indexer.ops ErrorInfo reason %s: %v", reason, err)
}

// One database and real TCP server keep the red-first contract suite finite.
// Reset restores only this test's isolated container; no developer data is used.
func TestV2Contract(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	var ready atomic.Bool
	ready.Store(true)
	srv, err := New(Config{ListenAddr: addr, DatasetReady: ready.Load, NodeLookup: nodelookup.New(archiveNode{}, nodelookup.Config{})}, h.db)
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(serverCtx) }()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.WaitForReady(true)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := indexerv2.NewIndexerServiceClient(conn)
	reset := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		_, e := h.pool.Exec(ctx, `TRUNCATE blocks, transactions, logs, token_transfers, internal_transactions, balances, contracts, tokens, address_stats, token_balances_current, missing_block_ranges;
 UPDATE prune_state SET pruned_through=0;
 UPDATE indexer_dataset_metadata SET chain_id='31337',genesis_hash=decode(repeat('00',32),'hex') WHERE id=1`)
		if e != nil {
			t.Fatal(e)
		}
		seedChain(ctx, t, h.pool)
		if _, e = h.pool.Exec(ctx, `INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count) VALUES(0,'0x'||repeat('00',32),'0x'||repeat('00',32),1700000000,0,30000000,0)`); e != nil {
			t.Fatal(e)
		}
		h.db.HiddenTxTypes = nil
		ready.Store(true)
		return ctx
	}
	t.Run("safe_head_and_durable_identity", func(t *testing.T) {
		ctx := reset(t)
		a, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if e != nil {
			t.Fatal(e)
		}
		b, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if a.GetPage().GetDatasetEpoch() == "" || a.GetPage().GetDatasetEpoch() != b.GetPage().GetDatasetEpoch() || a.GetChainId() != "31337" || len(a.GetGenesisHash()) != 32 || a.GetSafeHead().GetBlockNumber() != 5 {
			t.Fatalf("invalid identity/head: %v", a)
		}
	})
	t.Run("same_block_unique_key_pages_1_7_100", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `INSERT INTO transactions (hash,block_number,block_timestamp,tx_index,from_address,to_address,value,gas_used,gas_price,status) SELECT '0x'||lpad(to_hex(8192+n),64,'0'),5,1700000005,1,'0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',1,21000,1,1 FROM generate_series(1,9) n`)
		if e != nil {
			t.Fatal(e)
		}
		var expected []string
		for _, size := range []int32{100, 1, 7} {
			var got []string
			req := &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: size}}
			for n := 0; n < 50; n++ {
				p, e := client.ListTransactions(ctx, req)
				if e != nil {
					t.Fatal(e)
				}
				for _, row := range p.GetTransactions() {
					got = append(got, hex.EncodeToString(row.GetKey().GetHash()))
				}
				if p.GetPage().GetNextStartAfter() == nil {
					break
				}
				req.Read = v2Resume(p.GetPage())
				req.Page.StartAfter = p.GetPage().GetNextStartAfter()
			}
			if len(got) != 14 {
				t.Fatalf("size %d: got %d rows, want14", size, len(got))
			}
			if expected == nil {
				expected = got
			} else if !reflect.DeepEqual(got, expected) {
				t.Fatalf("size%d: skipped/repeated rows", size)
			}
		}
	})
	t.Run("snapshot_excludes_later_indexed_rows_and_resume_used_key", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 3}})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.GetTransactions()) != 3 {
			t.Fatalf("want3 rows: %v", p)
		}
		_, e = h.pool.Exec(ctx, `INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count) VALUES(6,'0x'||lpad('6',64,'0'),'0x'||lpad('5',64,'0'),1700000006,1,1,1); INSERT INTO transactions (hash,block_number,block_timestamp,tx_index,from_address,value,gas_used,gas_price,status) VALUES ('0x'||lpad('9999',64,'0'),6,1700000006,0,'0xaaa0000000000000000000000000000000000001',1,1,1,1); UPDATE sync_status SET last_indexed_block=6`)
		if e != nil {
			t.Fatal(e)
		}
		used := p.Transactions[0].GetKey()
		q, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{PageSize: 100, StartAfter: &indexerv2.Key{Value: &indexerv2.Key_Transaction{Transaction: used}}}})
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Transactions) != 4 || q.Transactions[0].Key.BlockNumber != 4 {
			t.Fatalf("used-row continuation skipped/unpinned: %v", q)
		}
	})
	t.Run("physical_gap_even_when_metadata_missing", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`)
		if e != nil {
			t.Fatal(e)
		}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 100}})
		if e != nil {
			t.Fatal(e)
		}
		if p.GetPage().GetRunFloorBlock() != 4 || p.GetPage().GetEndedAt() != indexerv2.EndedAt_ENDED_AT_INDEX_FLOOR || len(p.Transactions) != 2 {
			t.Fatalf("physical gap leaked older rows: %v", p)
		}
	})
	t.Run("reorg_anchor_rejected", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.pool.Exec(ctx, `UPDATE blocks SET hash='0x'||repeat('f',64) WHERE number=5`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: p.Page.NextStartAfter}})
		v2RequireReason(t, e, codes.Aborted, "CHAIN_REORGED")
	})
	t.Run("epoch_change_rejected", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.pool.Exec(ctx, `UPDATE indexer_dataset_metadata SET dataset_epoch=gen_random_uuid()`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: p.Page.NextStartAfter}})
		v2RequireReason(t, e, codes.Aborted, "DATASET_EPOCH_CHANGED")
	})
	t.Run("prune_precedence_and_rows_not_detached", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3; DELETE FROM blocks WHERE number<4`)
		if e != nil {
			t.Fatal(e)
		}
		q, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{PageSize: 100, StartAfter: p.Page.NextStartAfter}})
		if e != nil {
			t.Fatal(e)
		}
		if q.Page.GetRunFloorBlock() != p.Page.GetRunFloorBlock() || len(q.Transactions) != 1 || q.Transactions[0].Key.BlockNumber != 4 || q.Page.GetEndedAt() != indexerv2.EndedAt_ENDED_AT_RETENTION_FLOOR {
			t.Fatalf("prune falsely changed run or returned detached rows: %v", q)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Read: v2Resume(p.Page), Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1)), ToBlock: ptrV2(uint64(5))}}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
	})
	t.Run("empty_set_fails_closed_absent_is_unfiltered", func(t *testing.T) {
		ctx := reset(t)
		a, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: &indexerv2.TransactionFilter{From: &indexerv2.AddressSet{}}})
		if e != nil {
			t.Fatal(e)
		}
		b, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if len(a.Transactions) != 0 || len(b.Transactions) != 5 {
			t.Fatalf("empty/absent semantics violated: %v %v", a, b)
		}
	})
	t.Run("implicit_hidden_types_never_filter_v2", func(t *testing.T) {
		ctx := reset(t)
		h.db.HiddenTxTypes = []int{0}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 5 {
			t.Fatalf("configuration changed evidence: %v", p)
		}
	})
	t.Run("exact_transaction_precision_and_exact_counts", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `INSERT INTO transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status) SELECT '0x'||lpad(to_hex(9000+n),64,'0'),2,n,'0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',1,1,1,1 FROM generate_series(1,2) n`)
		if e != nil {
			t.Fatal(e)
		}
		f := &indexerv2.TransactionFilter{Destinations: &indexerv2.DestinationPredicates{Intervals: []*indexerv2.DestinationInterval{{ToAddress: v2Address("bbb0000000000000000000000000000000000002"), From: &indexerv2.TransactionPosition{BlockNumber: 2, TransactionIndex: 1}, To: &indexerv2.TransactionPosition{BlockNumber: 2, TransactionIndex: 2}}}}}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		c, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 1 || p.Transactions[0].Key.BlockNumber != 2 || p.Transactions[0].Key.TransactionIndex != 1 || c.Count != 1 {
			t.Fatalf("interval/count mismatch: %v %v", p, c)
		}
	})
	t.Run("ordered_batch_and_null_parent_facts", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.BatchGetTransactions(ctx, &indexerv2.BatchGetTransactionsRequest{Hashes: [][]byte{v2Hash(0x1002), v2Hash(0x1001)}})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 2 || !bytes.Equal(p.Transactions[0].Transaction.Key.Hash, v2Hash(0x1002)) || p.Transactions[0].Transaction.Nonce == nil || p.Transactions[0].Transaction.MaxFeePerGas != nil {
			t.Fatalf("order/null facts changed: %v", p)
		}
	})
	t.Run("batch_child_overflow_parent_point_still_available", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `INSERT INTO logs(tx_hash,log_index,address,data,block_number) SELECT '0x'||lpad('1005',64,'0'),n,'0xbbb0000000000000000000000000000000000002','0x',5 FROM generate_series(0,20000) n`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.BatchGetTransactions(ctx, &indexerv2.BatchGetTransactionsRequest{Hashes: [][]byte{v2Hash(0x1005)}})
		v2RequireReason(t, e, codes.ResourceExhausted, "CHILD_LIST_TOO_LARGE")
		p, e := client.GetTransaction(ctx, &indexerv2.GetTransactionRequest{Hash: v2Hash(0x1005)})
		if e != nil {
			t.Fatal(e)
		}
		if p.GetTransaction() == nil || p.GetSource() != indexerv2.Source_SOURCE_INDEXER {
			t.Fatalf("point cap escape unavailable: %v", p)
		}
	})
	t.Run("new_gap_in_pinned_retained_run_rejected", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: p.Page.NextStartAfter}})
		v2RequireReason(t, e, codes.Aborted, "INDEX_RUN_CHANGED")
	})
	t.Run("zero_match_scan_budget_advances", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `TRUNCATE transactions;
INSERT INTO transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status)
SELECT '0x'||lpad(to_hex(100000+n),64,'0'),5,n,'0xaaa0000000000000000000000000000000000001',CASE WHEN n=0 THEN '0xccc0000000000000000000000000000000000003' ELSE '0xbbb0000000000000000000000000000000000002' END,1,1,1,1 FROM generate_series(0,20000) n`)
		if e != nil {
			t.Fatal(e)
		}
		f := &indexerv2.TransactionFilter{To: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("ccc0000000000000000000000000000000000003")}}}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 0 || p.GetPage().GetNextStartAfter() == nil {
			t.Fatalf("empty budget page must advance: %v", p)
		}
		q, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: p.Page.NextStartAfter}, Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Transactions) != 1 || q.Transactions[0].Key.TransactionIndex != 0 || q.Page.GetNextStartAfter() != nil {
			t.Fatalf("scan continuation rescanned or skipped: %v", q)
		}
	})
	t.Run("counterparties_only_top_level_creation_and_block_match", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE transactions SET to_address=NULL WHERE block_number=4;
INSERT INTO contracts(address,bytecode,creator,creation_tx,block_number) VALUES
('0xddd0000000000000000000000000000000000004','0x','0xaaa0000000000000000000000000000000000001','0x'||lpad('1004',64,'0'),4),
('0xeee0000000000000000000000000000000000005','0x','0xaaa0000000000000000000000000000000000001','0x'||lpad('1005',64,'0'),5),
('0xfff0000000000000000000000000000000000006','0x','0xaaa0000000000000000000000000000000000001','0x'||lpad('1004',64,'0'),3)`)
		if e != nil {
			t.Fatal(e)
		}
		p, e := client.ListCounterparties(ctx, &indexerv2.ListCounterpartiesRequest{Targets: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("aaa0000000000000000000000000000000000001")}}})
		if e != nil {
			t.Fatal(e)
		}
		var got []string
		for _, r := range p.Counterparties {
			got = append(got, hex.EncodeToString(r.GetKey().GetAddress()))
		}
		want := []string{"bbb0000000000000000000000000000000000002", "ddd0000000000000000000000000000000000004"}
		if !reflect.DeepEqual(got, want) || p.Page.Consistency != indexerv2.Consistency_CONSISTENCY_CURRENT {
			t.Fatalf("factory/foreign block widened destinations: %v", p)
		}
	})
	t.Run("first_activity_exact_and_input_order", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.BatchFirstActivity(ctx, &indexerv2.BatchFirstActivityRequest{Targets: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("bbb0000000000000000000000000000000000002"), v2Address("ccc0000000000000000000000000000000000003")}}, Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(2)), ToBlock: ptrV2(uint64(4))}}})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Activities) != 2 || p.Activities[0].GetTransaction().GetKey().GetBlockNumber() != 2 || p.Activities[1].GetTransaction() != nil {
			t.Fatalf("first activity widened scope/order: %v", p)
		}
	})

	checkPage := func(t *testing.T, p *indexerv2.PageInfo, c indexerv2.Consistency) {
		t.Helper()
		if p == nil || p.GetReadAt() == nil || len(p.ReadAt.BlockHash) != 32 || p.DatasetEpoch == "" || p.Consistency != c {
			t.Fatalf("missing typed read metadata: %v", p)
		}
	}
	seedFacts := func(t *testing.T, ctx context.Context) {
		t.Helper()
		_, e := h.pool.Exec(ctx, `INSERT INTO logs(tx_hash,log_index,address,topic0,data,block_number,timestamp) VALUES ('0x'||lpad('1005',64,'0'),0,'0xddd0000000000000000000000000000000000004','0x'||repeat('ab',32),'0x01',5,1700000005);
INSERT INTO token_transfers(tx_hash,log_index,token_address,from_address,to_address,value,block_number,timestamp) VALUES ('0x'||lpad('1005',64,'0'),0,'0xddd0000000000000000000000000000000000004','0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',123456789012345678901234567890,5,1700000005);
INSERT INTO internal_transactions(tx_hash,block_number,trace_address,from_address,to_address,value,call_type) VALUES ('0x'||lpad('1005',64,'0'),5,'[0]','0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',9,'call');
INSERT INTO contracts(address,bytecode,creator,creation_tx,block_number) VALUES ('0xddd0000000000000000000000000000000000004','0x01','0xaaa0000000000000000000000000000000000001','0x'||lpad('1005',64,'0'),5);
INSERT INTO tokens(address,symbol,block_number,total_supply,usd_price) VALUES ('0xddd0000000000000000000000000000000000004','TEST',5,123456789012345678901234567890,1.23456789);
INSERT INTO address_stats(address,tx_count,is_contract) VALUES ('0xaaa0000000000000000000000000000000000001',5,false);
INSERT INTO balances(address,token_address,block_number,balance) VALUES ('0xaaa0000000000000000000000000000000000001','0xddd0000000000000000000000000000000000004',5,9999999999999999999999999);
INSERT INTO token_balances_current(token_address,address,block_number,balance) VALUES ('0xddd0000000000000000000000000000000000004','0xaaa0000000000000000000000000000000000001',5,9999999999999999999999999)`)
		if e != nil {
			t.Fatal(e)
		}
	}
	t.Run("get_block_point_source", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.GetBlock(ctx, &indexerv2.GetBlockRequest{Selector: &indexerv2.GetBlockRequest_Number{Number: 5}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if p.GetBlock().GetKey().GetNumber() != 5 || p.Source != indexerv2.Source_SOURCE_INDEXER {
			t.Fatalf("point/source: %v", p)
		}
	})
	t.Run("blocks_range_order_keys", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListBlocks(ctx, &indexerv2.ListBlocksRequest{Page: &indexerv2.PageRequest{Order: indexerv2.Order_ORDER_ASCENDING}, BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(2)), ToBlock: ptrV2(uint64(3))}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if len(p.Blocks) != 2 || p.Blocks[0].GetKey().GetNumber() != 2 || p.Blocks[1].GetKey().GetNumber() != 3 {
			t.Fatalf("inclusive range/order: %v", p)
		}
	})
	t.Run("logs_topics_exact_block_and_key", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListLogs(ctx, &indexerv2.ListLogsRequest{Filter: &indexerv2.LogFilter{Topic0: &indexerv2.HashSet{Hashes: [][]byte{bytes.Repeat([]byte{0xab}, 32)}}, TransactionHash: v2Hash(0x1005), BlockNumber: ptrV2(uint64(5))}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if len(p.Logs) != 1 || p.Logs[0].GetKey().GetBlockNumber() != 5 || p.Logs[0].GetKey().GetLogIndex() != 0 || p.Logs[0].Topic1 != nil {
			t.Fatalf("typed log/filter/null: %v", p)
		}
	})
	t.Run("transfer_token_kind_source_pages_and_count", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `INSERT INTO token_transfers(tx_hash,log_index,token_address,from_address,to_address,value,block_number,token_type,token_id)
 SELECT '0x'||lpad('1005',64,'0'),n,'0xddd0000000000000000000000000000000000004','0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',1,5,CASE WHEN n%3=0 THEN 'ERC20' WHEN n%3=1 THEN 'ERC721' ELSE 'ERC1155' END,CASE WHEN n%3=0 THEN NULL ELSE n END FROM generate_series(0,11)n`)
		if e != nil {
			t.Fatal(e)
		}
		for _, kind := range []indexerv2.TokenKind{indexerv2.TokenKind_TOKEN_KIND_ERC20, indexerv2.TokenKind_TOKEN_KIND_ERC721, indexerv2.TokenKind_TOKEN_KIND_ERC1155} {
			filter := &indexerv2.TransferFilter{TokenKind: kind.Enum(), TransactionHash: v2Hash(0x1005)}
			want := map[indexerv2.TokenKind]string{indexerv2.TokenKind_TOKEN_KIND_ERC20: "ERC20", indexerv2.TokenKind_TOKEN_KIND_ERC721: "ERC721", indexerv2.TokenKind_TOKEN_KIND_ERC1155: "ERC1155"}[kind]
			for _, size := range []int32{1, 3, 100} {
				req := &indexerv2.ListTokenTransfersRequest{Filter: filter, Page: &indexerv2.PageRequest{PageSize: size}}
				seen := map[uint32]bool{}
				for n := 0; n < 10; n++ {
					p, err := client.ListTokenTransfers(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range p.Transfers {
						if row.TokenType != want || seen[row.Key.LogIndex] {
							t.Fatalf("wrong type/repeated row: %v", row)
						}
						seen[row.Key.LogIndex] = true
					}
					if p.Page.NextStartAfter == nil {
						break
					}
					req.Read = v2Resume(p.Page)
					req.Page.StartAfter = p.Page.NextStartAfter
				}
				if len(seen) != 4 {
					t.Fatalf("%s page%d missing matches beyond first page: %v", want, size, seen)
				}
			}
			count, err := client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{Filter: filter})
			if err != nil || count.Count != 4 {
				t.Fatalf("typed count: %v %v", count, err)
			}
			filter.TransactionHash = v2Hash(0x1004)
			count, err = client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{Filter: filter})
			if err != nil || count.Count != 0 {
				t.Fatalf("kind must AND transaction hash: %v %v", count, err)
			}
		}
		all, e := client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{})
		if e != nil || all.Count != 12 {
			t.Fatalf("unfiltered compatibility: %v %v", all, e)
		}
		for _, kind := range []indexerv2.TokenKind{indexerv2.TokenKind_TOKEN_KIND_UNSPECIFIED, 99, -1} {
			_, e = client.ListTokenTransfers(ctx, &indexerv2.ListTokenTransfersRequest{Filter: &indexerv2.TransferFilter{TokenKind: kind.Enum()}})
			if status.Code(e) != codes.InvalidArgument {
				t.Fatalf("invalid present kind accepted by list: %v", e)
			}
			_, e = client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{Filter: &indexerv2.TransferFilter{TokenKind: kind.Enum()}})
			if status.Code(e) != codes.InvalidArgument {
				t.Fatalf("invalid present kind accepted by count: %v", e)
			}
		}
	})
	t.Run("transfers_typed_filters_lossless_value", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListTokenTransfers(ctx, &indexerv2.ListTokenTransfersRequest{Filter: &indexerv2.TransferFilter{Tokens: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("ddd0000000000000000000000000000000000004")}}, TransactionHash: v2Hash(0x1005), BlockNumber: ptrV2(uint64(5))}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if len(p.Transfers) != 1 || p.Transfers[0].GetKey().GetBlockNumber() != 5 || p.Transfers[0].Value != "123456789012345678901234567890" || p.Transfers[0].TokenId != nil {
			t.Fatalf("typed transfer/value: %v", p)
		}
	})
	t.Run("internals_indexed_frame_keys", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListInternalTransactions(ctx, &indexerv2.ListInternalTransactionsRequest{Filter: &indexerv2.InternalFilter{TransactionHash: v2Hash(0x1005), BlockNumber: ptrV2(uint64(5))}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if len(p.InternalTransactions) != 1 || p.InternalTransactions[0].GetKey().GetTraceAddress() != "[0]" || p.InternalTransactions[0].Gas != nil {
			t.Fatalf("typed frame key/null: %v", p)
		}
	})
	t.Run("contracts_current_typed_key", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListContracts(ctx, &indexerv2.ListContractsRequest{Addresses: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("ddd0000000000000000000000000000000000004")}}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Contracts) != 1 || p.Contracts[0].GetKey().GetBlockNumber() != 5 || p.Contracts[0].Name != nil {
			t.Fatalf("typed contract: %v", p)
		}
	})
	t.Run("tokens_current_nullable_decimal", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListTokens(ctx, &indexerv2.ListTokensRequest{Search: "TEST", TokenType: "ERC20"})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Tokens) != 1 || p.Tokens[0].GetKey().GetBlockNumber() != 5 || p.Tokens[0].GetUsdPrice() != "1.23456789" || p.Tokens[0].Name != nil {
			t.Fatalf("typed token: %v", p)
		}
	})
	t.Run("addresses_current_membership_filter", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListAddresses(ctx, &indexerv2.ListAddressesRequest{Addresses: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("aaa0000000000000000000000000000000000001")}}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Addresses) != 1 || !bytes.Equal(p.Addresses[0].GetKey().GetAddress(), v2Address("aaa0000000000000000000000000000000000001")) {
			t.Fatalf("typed address: %v", p)
		}
	})
	t.Run("balances_current_pair_key", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.ListBalances(ctx, &indexerv2.ListBalancesRequest{Addresses: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("aaa0000000000000000000000000000000000001")}}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Balances) != 1 || p.Balances[0].GetKey().GetTokenAddress() == nil || p.Balances[0].Balance != "9999999999999999999999999" {
			t.Fatalf("typed balance: %v", p)
		}
	})
	t.Run("holders_current_numeric_order_tie", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		_, e := h.pool.Exec(ctx, `INSERT INTO token_balances_current(token_address,address,block_number,balance) VALUES('0xddd0000000000000000000000000000000000004','0xbbb0000000000000000000000000000000000002',5,9999999999999999999999999)`)
		if e != nil {
			t.Fatal(e)
		}
		req := &indexerv2.ListHoldersRequest{TokenAddress: v2Address("ddd0000000000000000000000000000000000004"), Page: &indexerv2.PageRequest{PageSize: 1}}
		p, e := client.ListHolders(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Holders) != 1 || p.Page.NextStartAfter == nil || p.Holders[0].GetKey().GetBalance() != "9999999999999999999999999" {
			t.Fatalf("holder key/tie: %v", p)
		}
		req.Read = v2Resume(p.Page)
		req.Page.StartAfter = p.Page.NextStartAfter
		q, e := client.ListHolders(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Holders) != 1 || bytes.Equal(q.Holders[0].Key.Address, p.Holders[0].Key.Address) {
			t.Fatalf("holder tie repeated/skipped: %v", q)
		}
	})
	t.Run("count_logs_same_exact_filter", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.CountLogs(ctx, &indexerv2.CountLogsRequest{Filter: &indexerv2.LogFilter{TransactionHash: v2Hash(0x1005)}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if p.Count != 1 {
			t.Fatalf("exact log count: %v", p)
		}
	})
	t.Run("count_transfers_empty_set_none", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		p, e := client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{Filter: &indexerv2.TransferFilter{Tokens: &indexerv2.AddressSet{}}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_SNAPSHOT)
		if p.Count != 0 {
			t.Fatalf("empty transfer set widened exact count: %v", p)
		}
	})
	t.Run("search_kind_typed_key_and_context", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.Search(ctx, &indexerv2.SearchRequest{Query: fmt.Sprintf("0x%064x", 0x1005), Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{indexerv2.SearchKind_SEARCH_KIND_TRANSACTION}}})
		if e != nil {
			t.Fatal(e)
		}
		checkPage(t, p.Page, indexerv2.Consistency_CONSISTENCY_CURRENT)
		if len(p.Results) != 1 || p.Results[0].GetKey().GetKind() != indexerv2.SearchKind_SEARCH_KIND_TRANSACTION || p.Results[0].GetTransaction().GetKey().GetBlockNumber() != 5 {
			t.Fatalf("search typed fact/key: %v", p)
		}
	})
	t.Run("watermark_only_keeps_physical_rows_but_excludes_them", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`)
		if e != nil {
			t.Fatal(e)
		}
		var physicallyAttached int
		if e = h.pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE block_number<=3`).Scan(&physicallyAttached); e != nil || physicallyAttached != 3 {
			t.Fatalf("fixture must keep three old rows: %d %v", physicallyAttached, e)
		}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 100}})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 2 || p.Page.HistoryFromBlock != 4 || p.Page.EndedAt != indexerv2.EndedAt_ENDED_AT_RETENTION_FLOOR {
			t.Fatalf("attached pruned rows leaked: %v", p)
		}
		for _, r := range p.Transactions {
			if r.Key.BlockNumber < 4 {
				t.Fatalf("row below watermark: %v", r)
			}
		}
	})
	t.Run("pinned_floor_survives_lower_backfill", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`)
		if e != nil {
			t.Fatal(e)
		}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		if p.Page.RunFloorBlock != 4 {
			t.Fatalf("fixture floor: %v", p)
		}
		_, e = h.pool.Exec(ctx, `INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count) VALUES(3,'0x'||lpad('3',64,'0'),'0x'||lpad('2',64,'0'),1700000003,1,1,1)`)
		if e != nil {
			t.Fatal(e)
		}
		q, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{PageSize: 100, StartAfter: p.Page.NextStartAfter}})
		if e != nil {
			t.Fatal(e)
		}
		if q.Page.RunFloorBlock != 4 || len(q.Transactions) != 1 || q.Transactions[0].Key.BlockNumber != 4 || q.Page.EndedAt != indexerv2.EndedAt_ENDED_AT_INDEX_FLOOR {
			t.Fatalf("backfill widened pinned run: %v", q)
		}
	})
	t.Run("continuation_requires_complete_context", func(t *testing.T) {
		ctx := reset(t)
		_, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{StartAfter: &indexerv2.Key{Value: &indexerv2.Key_Transaction{Transaction: &indexerv2.TransactionKey{BlockNumber: 4, Hash: v2Hash(0x1004)}}}}})
		v2RequireReason(t, e, codes.InvalidArgument, "INVALID_READ_CONTEXT")
	})
	t.Run("wrong_family_key_rejected", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: &indexerv2.Key{Value: &indexerv2.Key_Block{Block: &indexerv2.BlockKey{Number: 4}}}}})
		v2RequireReason(t, e, codes.InvalidArgument, "INVALID_KEY")
	})
	t.Run("oversized_set_rejected", func(t *testing.T) {
		ctx := reset(t)
		many := make([][]byte, 4097)
		for i := range many {
			many[i] = make([]byte, 20)
			many[i][18] = byte(i >> 8)
			many[i][19] = byte(i)
		}
		_, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: &indexerv2.TransactionFilter{From: &indexerv2.AddressSet{Addresses: many}}})
		v2RequireReason(t, e, codes.InvalidArgument, "FILTER_SET_TOO_LARGE")
	})
	t.Run("aggregate_scope_below_run_is_not_clipped", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1)), ToBlock: ptrV2(uint64(5))}}})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
	})
	t.Run("creation_requires_exact_hash_block_index", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `UPDATE transactions SET to_address=NULL WHERE block_number=4`); e != nil {
			t.Fatal(e)
		}
		f := &indexerv2.TransactionFilter{TransactionHash: v2Hash(0x1004), Destinations: &indexerv2.DestinationPredicates{Creations: []*indexerv2.ExactCreation{{Transaction: &indexerv2.TransactionKey{Hash: v2Hash(0x1004), BlockNumber: 4, TransactionIndex: 1}}}}}
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 0 {
			t.Fatalf("wrong exact creation index widened scope: %v", p)
		}
		f.Destinations.Creations[0].Transaction.TransactionIndex = 0
		q, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: f})
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Transactions) != 1 {
			t.Fatalf("exact creation + hash did not match: %v", q)
		}
	})
	t.Run("count_over_budget_is_error", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `INSERT INTO transactions(hash,block_number,tx_index,from_address,value,gas_used,gas_price,status) SELECT '0x'||lpad(to_hex(100000+n),64,'0'),5,n,'0xaaa0000000000000000000000000000000000001',1,1,1,1 FROM generate_series(1,20001) n`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{})
		v2RequireReason(t, e, codes.ResourceExhausted, "COUNT_BUDGET_EXCEEDED")
	})
	t.Run("batch_missing_parent_is_atomic_and_duplicates_preserved", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.BatchGetTransactions(ctx, &indexerv2.BatchGetTransactionsRequest{Hashes: [][]byte{v2Hash(0x1001), v2Hash(0xdead)}})
		v2RequireReason(t, e, codes.NotFound, "OBJECT_NOT_FOUND")
		if p != nil {
			t.Fatal("batch returned partial result")
		}
		q, e := client.BatchGetTransactions(ctx, &indexerv2.BatchGetTransactionsRequest{Hashes: [][]byte{v2Hash(0x1001), v2Hash(0x1001)}})
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Transactions) != 2 || !bytes.Equal(q.Transactions[0].Transaction.Key.Hash, q.Transactions[1].Transaction.Key.Hash) {
			t.Fatalf("duplicate order lost: %v", q)
		}
	})
	t.Run("wipe_marker_blocks_all_reads", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `CREATE TABLE wipe_in_progress(marker BOOLEAN)`)
		if e != nil {
			t.Fatal(e)
		}
		defer h.pool.Exec(context.Background(), `DROP TABLE wipe_in_progress`)
		_, e = client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		v2RequireReason(t, e, codes.Unavailable, "DATASET_UNAVAILABLE")
	})
	t.Run("history_exclusive_gate_is_bounded_and_resumes", func(t *testing.T) {
		ctx := reset(t)
		gate, e := h.pool.Acquire(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer gate.Release()
		if _, e = gate.Exec(ctx, `SELECT pg_advisory_lock(4673,1)`); e != nil {
			t.Fatal(e)
		}
		defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(4673,1)`)
		started := time.Now()
		_, e = client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if status.Code(e) != codes.Unavailable && status.Code(e) != codes.DeadlineExceeded {
			t.Fatalf("held gate not unavailable: %v", e)
		}
		if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
			t.Fatalf("gate exceeded1s lock bound: %s", elapsed)
		}
		if _, e = gate.Exec(ctx, `SELECT pg_advisory_unlock(4673,1)`); e != nil {
			t.Fatal(e)
		}
		if _, e = client.GetChainView(ctx, &indexerv2.GetChainViewRequest{}); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("shorter_caller_deadline_bounds_history_gate", func(t *testing.T) {
		ctx := reset(t)
		gate, e := h.pool.Acquire(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer gate.Release()
		if _, e = gate.Exec(ctx, `SELECT pg_advisory_lock(4673,1)`); e != nil {
			t.Fatal(e)
		}
		defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(4673,1)`)
		short, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, e = client.GetChainView(short, &indexerv2.GetChainViewRequest{})
		if status.Code(e) != codes.DeadlineExceeded || time.Since(started) > 500*time.Millisecond {
			t.Fatalf("short caller budget reset: %v", e)
		}
	})
	t.Run("process_startup_identity_gate", func(t *testing.T) {
		ctx := reset(t)
		ready.Store(false)
		_, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		v2RequireReason(t, e, codes.Unavailable, "DATASET_UNAVAILABLE")
		ready.Store(true)
		if _, e = client.GetChainView(ctx, &indexerv2.GetChainViewRequest{}); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("missing_trusted_identity_is_unavailable", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE indexer_dataset_metadata SET chain_id=NULL,genesis_hash=NULL`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
	})

	t.Run("pruned_points_node_source_ranges_stay_indexed", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`)
		if e != nil {
			t.Fatal(e)
		}
		b, e := client.GetBlock(ctx, &indexerv2.GetBlockRequest{Selector: &indexerv2.GetBlockRequest_Number{Number: 2}})
		if e != nil {
			t.Fatal(e)
		}
		if b.Source != indexerv2.Source_SOURCE_NODE || !bytes.Equal(b.Block.Hash, nodeBlockHash(2).Bytes()) {
			t.Fatalf("point did not use node: %v", b)
		}
		tx, e := client.GetTransaction(ctx, &indexerv2.GetTransactionRequest{Hash: nodeTxHash(2).Bytes()})
		if e != nil {
			t.Fatal(e)
		}
		if tx.Source != indexerv2.Source_SOURCE_NODE || tx.Transaction.Key.BlockNumber != 2 || tx.Transaction.GasUsed != 21000 {
			t.Fatalf("parent/receipt node mapping: %v", tx)
		}
		rows, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if len(rows.Transactions) != 2 || rows.Page.EndedAt != indexerv2.EndedAt_ENDED_AT_RETENTION_FLOOR {
			t.Fatalf("range fell back to node: %v", rows)
		}
	})
	t.Run("unknown_request_field_is_rejected", func(t *testing.T) {
		ctx := reset(t)
		r := &indexerv2.ListTransactionsRequest{}
		r.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
		_, e := client.ListTransactions(ctx, r)
		v2RequireReason(t, e, codes.InvalidArgument, "INVALID_FILTER")
	})
	t.Run("oversized_fact_is_not_transferred_as_partial_success", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE transactions SET input_data='0x'||repeat('ab',2097153) WHERE block_number=5`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.GetTransaction(ctx, &indexerv2.GetTransactionRequest{Hash: v2Hash(0x1005)})
		v2RequireReason(t, e, codes.ResourceExhausted, "READ_BUDGET_EXCEEDED")
	})
	t.Run("multibyte_text_field_bytes_are_bounded", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		_, e := h.pool.Exec(ctx, `UPDATE contracts SET source_code=repeat('€',700000)`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListContracts(ctx, &indexerv2.ListContractsRequest{})
		v2RequireReason(t, e, codes.ResourceExhausted, "READ_BUDGET_EXCEEDED")
	})
	t.Run("malformed_indexed_fact_is_data_loss", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE transactions SET input_data='0xzz' WHERE block_number=5`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.GetTransaction(ctx, &indexerv2.GetTransactionRequest{Hash: v2Hash(0x1005)})
		v2RequireReason(t, e, codes.DataLoss, "INCONSISTENT_DATA")
	})
	t.Run("unbounded_aggregate_after_prune_is_not_clipped", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
	})
	t.Run("unbounded_aggregate_after_gap_is_not_clipped", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
	})

	t.Run("creation_predicate_never_matches_ordinary_call", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: &indexerv2.TransactionFilter{Destinations: &indexerv2.DestinationPredicates{Creations: []*indexerv2.ExactCreation{{Transaction: &indexerv2.TransactionKey{Hash: v2Hash(0x1004), BlockNumber: 4}}}}}})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Transactions) != 0 {
			t.Fatalf("creation widened normal call: %v", p)
		}
	})
	t.Run("counterparty_bounds_include_unbackfilled_creation", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE transactions SET to_address=NULL WHERE block_number IN(1,5)`)
		if e != nil {
			t.Fatal(e)
		}
		p, e := client.ListCounterparties(ctx, &indexerv2.ListCounterpartiesRequest{Targets: &indexerv2.AddressSet{Addresses: [][]byte{v2Address("aaa0000000000000000000000000000000000001")}}})
		if e != nil {
			t.Fatal(e)
		}
		if p.FirstBlock == nil || *p.FirstBlock != 1 || p.LastBlock == nil || *p.LastBlock != 5 || len(p.Counterparties) != 1 {
			t.Fatalf("creation bounds silently clipped: %v", p)
		}
	})
	t.Run("fully_pruned_range_is_explicit_error", func(t *testing.T) {
		ctx := reset(t)
		_, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`)
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1)), ToBlock: ptrV2(uint64(2))}}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
	})

	t.Run("review_missing_genesis_is_incomplete", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=0`); e != nil {
			t.Fatal(e)
		}
		p, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if p.BackfillComplete {
			t.Error("floor1 must not claim complete history")
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
		_, e = client.CountLogs(ctx, &indexerv2.CountLogsRequest{})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
		_, e = client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
		_, e = client.BatchFirstActivity(ctx, &indexerv2.BatchFirstActivityRequest{Targets: &indexerv2.AddressSet{}})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
		_, e = client.ListCounterparties(ctx, &indexerv2.ListCounterpartiesRequest{Targets: &indexerv2.AddressSet{}})
		v2RequireReason(t, e, codes.Unavailable, "INDEXER_BEHIND")
		c, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1))}}})
		if e != nil || c.GetCount() != 5 {
			t.Fatalf("boundedfrom1: %v %v", c, e)
		}
	})
	t.Run("review_pruned_resume_precedes_retained_gap", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=2;DELETE FROM blocks WHERE number=4`); e != nil {
			t.Fatal(e)
		}
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: &indexerv2.Key{Value: &indexerv2.Key_Transaction{Transaction: &indexerv2.TransactionKey{BlockNumber: 2, Hash: v2Hash(0x1002)}}}}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
	})
	t.Run("review_pruned_scope_precedes_retained_gap", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Page: &indexerv2.PageRequest{PageSize: 1}})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=2;DELETE FROM blocks WHERE number=4`); e != nil {
			t.Fatal(e)
		}
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Read: v2Resume(p.Page), Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1)), ToBlock: ptrV2(uint64(5))}}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Filter: &indexerv2.TransactionFilter{BlockRange: &indexerv2.BlockRange{ToBlock: ptrV2(uint64(2))}}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
		_, e = client.ListTransactions(ctx, &indexerv2.ListTransactionsRequest{Read: v2Resume(p.Page), Page: &indexerv2.PageRequest{StartAfter: p.Page.NextStartAfter}})
		v2RequireReason(t, e, codes.Aborted, "INDEX_RUN_CHANGED")
	})
	t.Run("review_search_large_catalog_progresses", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `INSERT INTO address_stats(address) SELECT '0x'||lpad(to_hex(100000+n),40,'0') FROM generate_series(1,20001)n`); e != nil {
			t.Fatal(e)
		}
		req := &indexerv2.SearchRequest{Query: "0x", Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{indexerv2.SearchKind_SEARCH_KIND_ADDRESS}}, Page: &indexerv2.PageRequest{PageSize: 1, Order: indexerv2.Order_ORDER_ASCENDING}}
		p, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Results) != 1 || p.Page.NextStartAfter == nil {
			t.Fatalf("firstpage: %v", p)
		}
		req.Read = v2Resume(p.Page)
		req.Page.StartAfter = p.Page.NextStartAfter
		q, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Results) != 1 || bytes.Equal(p.Results[0].Key.Identity, q.Results[0].Key.Identity) {
			t.Fatalf("search repeat: %v", q)
		}
		req.Page.StartAfter = &indexerv2.Key{Value: &indexerv2.Key_Search{Search: &indexerv2.SearchKey{Kind: indexerv2.SearchKind_SEARCH_KIND_ADDRESS, Identity: v2Address(fmt.Sprintf("%040x", 120000))}}}
		z, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(z.Results) != 1 || z.Page.NextStartAfter != nil || !bytes.Equal(z.Results[0].Key.Identity, v2Address(fmt.Sprintf("%040x", 120001))) {
			t.Fatalf("tailafter20000: %v", z)
		}
	})
	for _, family := range []string{"logs", "transfers"} {
		for _, bad := range []string{"0x01", "0x" + strings.Repeat("ab", 33)} {
			t.Run("review_bad_child_hash_"+family+fmt.Sprint(len(bad)), func(t *testing.T) {
				ctx := reset(t)
				var e error
				if family == "logs" {
					_, e = h.pool.Exec(ctx, `INSERT INTO logs(tx_hash,log_index,address,data,block_number) VALUES($1,0,'0xbbb0000000000000000000000000000000000002','0x',5)`, bad)
				} else {
					_, e = h.pool.Exec(ctx, `INSERT INTO token_transfers(tx_hash,log_index,token_address,from_address,to_address,value,block_number) VALUES($1,0,'0xddd0000000000000000000000000000000000004','0xaaa0000000000000000000000000000000000001','0xbbb0000000000000000000000000000000000002',1,5)`, bad)
				}
				if e != nil {
					t.Fatal(e)
				}
				if family == "logs" {
					_, e = client.ListLogs(ctx, &indexerv2.ListLogsRequest{})
				} else {
					_, e = client.ListTokenTransfers(ctx, &indexerv2.ListTokenTransfersRequest{})
				}
				v2RequireReason(t, e, codes.DataLoss, "INCONSISTENT_DATA")
			})
		}
	}
	t.Run("review_holder_uint256_fact_and_key", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		overflow := new(big.Int).Lsh(big.NewInt(1), 256).String()
		if _, e := h.pool.Exec(ctx, `UPDATE token_balances_current SET balance=$1::numeric`, overflow); e != nil {
			t.Fatal(e)
		}
		_, e := client.ListHolders(ctx, &indexerv2.ListHoldersRequest{TokenAddress: v2Address("ddd0000000000000000000000000000000000004")})
		v2RequireReason(t, e, codes.DataLoss, "INCONSISTENT_DATA")
		p, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if e != nil {
			t.Fatal(e)
		}
		_, e = client.ListHolders(ctx, &indexerv2.ListHoldersRequest{Read: v2Resume(p.Page), TokenAddress: v2Address("ddd0000000000000000000000000000000000004"), Page: &indexerv2.PageRequest{StartAfter: &indexerv2.Key{Value: &indexerv2.Key_Holder{Holder: &indexerv2.HolderKey{Address: v2Address("aaa0000000000000000000000000000000000001"), Balance: overflow}}}}})
		v2RequireReason(t, e, codes.InvalidArgument, "INVALID_KEY")
	})

	t.Run("review_actual_genesis_allows_complete_aggregate", func(t *testing.T) {
		ctx := reset(t)
		p, e := client.GetChainView(ctx, &indexerv2.GetChainViewRequest{})
		if e != nil {
			t.Fatal(e)
		}
		if p.Page.RunFloorBlock != 0 || !p.BackfillComplete {
			t.Fatalf("actualgenesis not complete: %v", p)
		}
		c, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{})
		if e != nil || c.GetCount() != 5 {
			t.Fatalf("complete count: %v %v", c, e)
		}
	})
	t.Run("review_search_mixed_kind_key_order", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		for _, order := range []indexerv2.Order{indexerv2.Order_ORDER_DESCENDING, indexerv2.Order_ORDER_ASCENDING} {
			req := &indexerv2.SearchRequest{Query: "0x", Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{3, 4, 5}}, Page: &indexerv2.PageRequest{PageSize: 1, Order: order}}
			var got []int32
			for i := 0; i < 8; i++ {
				p, e := client.Search(ctx, req)
				if e != nil {
					t.Fatal(e)
				}
				for _, r := range p.Results {
					got = append(got, int32(r.Key.Kind))
				}
				if p.Page.NextStartAfter == nil {
					break
				}
				req.Read = v2Resume(p.Page)
				req.Page.StartAfter = p.Page.NextStartAfter
			}
			want := []int32{5, 4, 3}
			if order == indexerv2.Order_ORDER_ASCENDING {
				want = []int32{3, 4, 5}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("mixedorder got%v want%v", got, want)
			}
		}
	})
	t.Run("review_search_transaction_order_matches_search_key", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `INSERT INTO transactions(hash,block_number,tx_index,from_address,value,gas_used,gas_price,status) VALUES('0x'||lpad('2001',64,'0'),5,20,'0xaaa0000000000000000000000000000000000001',0,1,1,1),('0x'||lpad('2002',64,'0'),5,1,'0xaaa0000000000000000000000000000000000001',0,1,1,1)`); e != nil {
			t.Fatal(e)
		}
		req := &indexerv2.SearchRequest{Query: "0x", Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{2}}, Page: &indexerv2.PageRequest{PageSize: 1, Order: indexerv2.Order_ORDER_ASCENDING}}
		var got []string
		for i := 0; i < 12; i++ {
			p, e := client.Search(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			for _, r := range p.Results {
				got = append(got, hex.EncodeToString(r.Key.Identity))
			}
			if p.Page.NextStartAfter == nil {
				break
			}
			req.Read = v2Resume(p.Page)
			req.Page.StartAfter = p.Page.NextStartAfter
		}
		var want []string
		for _, n := range []uint64{0x1001, 0x1002, 0x1003, 0x1004, 0x1005, 0x2001, 0x2002} {
			want = append(want, hex.EncodeToString(v2Hash(n)))
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("searchhashorder got%v want%v", got, want)
		}
	})
	t.Run("review_search_budget_empty_page_advances", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `INSERT INTO address_stats(address) SELECT '0x'||lpad(to_hex(100000+n),40,'0') FROM generate_series(1,20001)n`); e != nil {
			t.Fatal(e)
		}
		req := &indexerv2.SearchRequest{Query: "no-matching-address", Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{5}}, Page: &indexerv2.PageRequest{PageSize: 1}}
		p, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Results) != 0 || p.Page.NextStartAfter == nil {
			t.Fatalf("zero budget continuation: %v", p)
		}
		req.Read = v2Resume(p.Page)
		req.Page.StartAfter = p.Page.NextStartAfter
		q, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if len(q.Results) != 0 || q.Page.NextStartAfter != nil {
			t.Fatalf("empty continuation did not finish: %v", q)
		}
	})
	t.Run("review_search_terminal_reports_floor", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number=3`); e != nil {
			t.Fatal(e)
		}
		req := &indexerv2.SearchRequest{Query: "0x", Kinds: &indexerv2.SearchKindSet{Kinds: []indexerv2.SearchKind{2}}}
		p, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if p.Page.EndedAt != indexerv2.EndedAt_ENDED_AT_INDEX_FLOOR || len(p.Results) != 2 {
			t.Fatalf("searchindexfloor: %v", p)
		}
		if _, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`); e != nil {
			t.Fatal(e)
		}
		q, e := client.Search(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		if q.Page.EndedAt != indexerv2.EndedAt_ENDED_AT_RETENTION_FLOOR || len(q.Results) != 2 {
			t.Fatalf("searchretentionfloor: %v", q)
		}
	})
	t.Run("review_holder_max_uint256_is_valid", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)).String()
		if _, e := h.pool.Exec(ctx, `UPDATE token_balances_current SET balance=$1::numeric`, maximum); e != nil {
			t.Fatal(e)
		}
		p, e := client.ListHolders(ctx, &indexerv2.ListHoldersRequest{TokenAddress: v2Address("ddd0000000000000000000000000000000000004")})
		if e != nil {
			t.Fatal(e)
		}
		if len(p.Holders) != 1 || p.Holders[0].Key.Balance != maximum {
			t.Fatalf("validmaxholder: %v", p)
		}
	})

	t.Run("review_v2_seek_indexes", func(t *testing.T) {
		ctx := reset(t)
		names := []string{"idx_v2_transactions_seek", "idx_v2_transactions_search", "idx_v2_internal_seek", "idx_v2_contracts_seek", "idx_v2_tokens_seek", "idx_v2_addresses_seek", "idx_v2_balances_seek", "idx_v2_holders_seek", "idx_v2_contract_creation"}
		var n int
		if e := h.pool.QueryRow(ctx, `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=ANY($1::text[]) AND i.indisvalid`, names).Scan(&n); e != nil || n != len(names) {
			t.Fatalf("validseekindexes %d/%d: %v", n, len(names), e)
		}
		var inherited bool
		if e := h.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_inherits h JOIN pg_class c ON c.oid=h.inhparent WHERE c.relname='idx_v2_transactions_seek')`).Scan(&inherited); e != nil || !inherited {
			t.Fatalf("partitionseeknotinherited: %v", e)
		}
	})

	t.Run("review_retained_exact_block_count_after_prune", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		if _, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`); e != nil {
			t.Fatal(e)
		}
		logs, e := client.CountLogs(ctx, &indexerv2.CountLogsRequest{Filter: &indexerv2.LogFilter{BlockNumber: ptrV2(uint64(5))}})
		if e != nil || logs.GetCount() != 1 {
			t.Fatalf("retained exactblock logs: %v %v", logs, e)
		}
		transfers, e := client.CountTokenTransfers(ctx, &indexerv2.CountTokenTransfersRequest{Filter: &indexerv2.TransferFilter{BlockNumber: ptrV2(uint64(5))}})
		if e != nil || transfers.GetCount() != 1 {
			t.Fatalf("retained exactblock transfers: %v %v", transfers, e)
		}
	})
	t.Run("review_retained_exact_hash_count_after_prune", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		if _, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`); e != nil {
			t.Fatal(e)
		}
		tx, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{TransactionHash: v2Hash(0x1005)}})
		if e != nil || tx.GetCount() != 1 {
			t.Fatalf("retained exacthash tx: %v %v", tx, e)
		}
		logs, e := client.CountLogs(ctx, &indexerv2.CountLogsRequest{Filter: &indexerv2.LogFilter{TransactionHash: v2Hash(0x1005)}})
		if e != nil || logs.GetCount() != 1 {
			t.Fatalf("retained exacthash logs: %v %v", logs, e)
		}
	})

	t.Run("review_exact_scope_intersection_never_widens", func(t *testing.T) {
		ctx := reset(t)
		seedFacts(t, ctx)
		if _, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`); e != nil {
			t.Fatal(e)
		}
		old := &indexerv2.BlockRange{FromBlock: ptrV2(uint64(1)), ToBlock: ptrV2(uint64(2))}
		tx, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{TransactionHash: v2Hash(0x1005), BlockRange: old}})
		if e != nil || tx.GetCount() != 0 {
			t.Fatalf("hash/range widened: %v %v", tx, e)
		}
		logs, e := client.CountLogs(ctx, &indexerv2.CountLogsRequest{Filter: &indexerv2.LogFilter{BlockNumber: ptrV2(uint64(5)), BlockRange: old}})
		if e != nil || logs.GetCount() != 0 {
			t.Fatalf("exactblock/range widened: %v %v", logs, e)
		}
	})
	t.Run("review_unknown_hash_never_invents_coverage", func(t *testing.T) {
		ctx := reset(t)
		if _, e := h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3`); e != nil {
			t.Fatal(e)
		}
		_, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{TransactionHash: v2Hash(0xdead)}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
		_, e = client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{TransactionHash: v2Hash(0x1002)}})
		v2RequireReason(t, e, codes.OutOfRange, "HISTORY_PRUNED")
		p, e := client.CountTransactions(ctx, &indexerv2.CountTransactionsRequest{Filter: &indexerv2.TransactionFilter{TransactionHash: v2Hash(0xdead), BlockRange: &indexerv2.BlockRange{FromBlock: ptrV2(uint64(4)), ToBlock: ptrV2(uint64(5))}}})
		if e != nil || p.GetCount() != 0 {
			t.Fatalf("bounded unknownhash: %v %v", p, e)
		}
	})
	stop()
	select {
	case err = <-done:
		if err != nil {
			t.Errorf("server stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("server failed to stop")
	}
}

func ptrV2[T any](v T) *T { return &v }
