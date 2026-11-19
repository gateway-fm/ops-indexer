package grpcserver

import (
	"context"
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"testing"
	"time"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A real gRPC/Postgres regression: the page boundary falls inside one block.
func TestV1TransactionFeedSameBlockAndFilterBinding(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	for i := 1; i <= 17; i++ {
		_, err := h.pool.Exec(ctx, `INSERT INTO transactions(hash,block_number,block_timestamp,tx_index,from_address,to_address,value,gas_used,gas_price,tx_type,input_data,status) VALUES($1,5,1700000005,$2,$3,$4,0,21000,1,0,'0x',1)`, fmt.Sprintf("0x%064x", 0x9000+i), i, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int32{1, 7, 100} {
		seen := map[string]bool{}
		cursor := ""
		for page := 0; page < 100; page++ {
			resp, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: size, Cursor: cursor}})
			if err != nil {
				t.Fatal(err)
			}
			for _, tx := range resp.Transactions {
				if seen[tx.Hash] {
					t.Fatalf("size %d duplicate %s", size, tx.Hash)
				}
				seen[tx.Hash] = true
			}
			cursor = resp.GetPage().GetNextCursor()
			if cursor == "" {
				break
			}
		}
		if len(seen) != 22 {
			t.Fatalf("size %d got %d transactions, want22 (same-block rows must not be skipped)", size, len(seen))
		}
	}
	first, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 1, Cursor: first.Page.NextCursor}, Status: indexerv1.TransactionStatus_TRANSACTION_STATUS_FAILED})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed status cursor: %v", err)
	}
}

func TestV1ListKeysetsAllShippedResources(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	const acct = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const topic = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	// More than one maximum-size page and all primary sort values tied.
	_, err := h.pool.Exec(ctx, `INSERT INTO tokens(address,symbol,name,decimals,token_type,holder_count,block_number) VALUES($1,'TEST','Token',18,'ERC20',107,5)`, token)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 107; i++ {
		addr := fmt.Sprintf("0x%040x", 0x20000+i)
		tok := fmt.Sprintf("0x%040x", 0x30000+i)
		_, err = h.pool.Exec(ctx, `INSERT INTO logs(tx_hash,log_index,address,topic0,data,block_number) VALUES($1,$2,$3,$4,'0x',5)`, txHash(5), i, acct, topic)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO token_transfers(tx_hash,log_index,token_address,from_address,to_address,value,block_number,transfer_type,token_type) VALUES($1,$2,$3,$4,$5,100,5,'transfer','ERC20')`, txHash(5), i, token, acct, addr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO internal_transactions(tx_hash,block_number,trace_address,from_address,to_address,value,call_type) VALUES($1,5,$2,$3,$4,0,'call')`, txHash(5), fmt.Sprintf("0/%03d", i), acct, addr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO tokens(address,symbol,name,decimals,token_type,holder_count,block_number) VALUES($1,'SAME','Same',18,'ERC20',7,5)`, tok)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO token_balances_current(address,token_address,block_number,balance) VALUES($1,$2,5,100),($3,$4,5,99)`, addr, token, acct, tok)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO address_stats(address,tx_count,first_seen,last_seen,is_contract) VALUES($1,7,1,5,false)`, addr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO nft_tokens(token_address,token_id,owner,block_number) VALUES($1,$2,$3,5)`, token, fmt.Sprint(i+1), addr)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO op_deposits(l2_tx_hash,l1_block_number,l1_tx_hash,l1_tx_origin) VALUES($1,100,$2,$3)`, fmt.Sprintf("0x%064x", 0x40000+i), txHash(1), acct)
		if err != nil {
			t.Fatal(err)
		}
	}
	h.srv.cfg.OPStackEnabled = true
	type fetch func(int32, string, int32) ([]string, string, int64, error)
	checks := []struct {
		name   string
		want   int
		offset bool
		fetch  fetch
	}{
		{"logs-address", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: acct, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Logs {
				ids = append(ids, fmt.Sprint(v.LogIndex))
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"logs-transaction", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByTxHash: txHash(5), Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Logs {
				ids = append(ids, fmt.Sprint(v.LogIndex))
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"transfers-token", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByToken: token, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Transfers {
				ids = append(ids, fmt.Sprint(v.LogIndex))
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"transfers-address", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByAddress: acct, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Transfers {
				ids = append(ids, fmt.Sprint(v.LogIndex))
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"internals-tx", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{ByTxHash: txHash(5), Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.InternalTransactions {
				ids = append(ids, v.TraceAddress)
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"internals-address", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{ByAddress: acct, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.InternalTransactions {
				ids = append(ids, v.TraceAddress)
			}
			return ids, r.Page.NextCursor, r.TotalCount, nil
		}},
		{"tokens", 108, true, func(n int32, c string, page int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokens(ctx, &indexerv1.ListTokensRequest{Page: &indexerv1.OffsetPageRequest{PageSize: n, Cursor: c, Page: page}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Tokens {
				ids = append(ids, v.Address)
			}
			return ids, r.Page.NextCursor, r.Page.TotalItems, nil
		}},
		{"holders", 107, true, func(n int32, c string, page int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokenHolders(ctx, &indexerv1.ListTokenHoldersRequest{TokenAddress: token, Page: &indexerv1.OffsetPageRequest{PageSize: n, Cursor: c, Page: page}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Holders {
				ids = append(ids, v.Address)
			}
			return ids, r.Page.NextCursor, r.Page.TotalItems, nil
		}},
		{"inventory", 107, true, func(n int32, c string, page int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokenInventory(ctx, &indexerv1.ListTokenInventoryRequest{TokenAddress: token, Page: &indexerv1.OffsetPageRequest{PageSize: n, Cursor: c, Page: page}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Items {
				ids = append(ids, v.TokenId.Value)
			}
			return ids, r.Page.NextCursor, r.Page.TotalItems, nil
		}},
		{"addresses", 107, true, func(n int32, c string, page int32) ([]string, string, int64, error) {
			r, e := h.client.ListAddresses(ctx, &indexerv1.ListAddressesRequest{Page: &indexerv1.OffsetPageRequest{PageSize: n, Cursor: c, Page: page}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Addresses {
				ids = append(ids, v.Address)
			}
			return ids, r.Page.NextCursor, r.Page.TotalItems, nil
		}},
		{"global-transfers", 107, true, func(n int32, c string, page int32) ([]string, string, int64, error) {
			r, e := h.client.ListAllTokenTransfers(ctx, &indexerv1.ListAllTokenTransfersRequest{Page: &indexerv1.OffsetPageRequest{PageSize: n, Cursor: c, Page: page}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Transfers {
				ids = append(ids, fmt.Sprint(v.LogIndex))
			}
			return ids, r.Page.NextCursor, r.Page.TotalItems, nil
		}},
		{"balances", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListTokenBalances(ctx, &indexerv1.ListTokenBalancesRequest{Address: acct, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Balances {
				ids = append(ids, v.TokenAddress)
			}
			return ids, r.Page.NextCursor, 107, nil
		}},
		{"op-deposits", 107, false, func(n int32, c string, _ int32) ([]string, string, int64, error) {
			r, e := h.client.ListOPDeposits(ctx, &indexerv1.ListOPDepositsRequest{ByFrom: acct, Page: &indexerv1.PageRequest{PageSize: n, Cursor: c}})
			if e != nil {
				return nil, "", 0, e
			}
			ids := []string{}
			for _, v := range r.Deposits {
				ids = append(ids, v.L2TransactionHash)
			}
			return ids, r.Page.NextCursor, 107, nil
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			var reference []string
			for _, size := range []int32{1, 7, 100} {
				cursor := ""
				seen := map[string]bool{}
				all := []string{}
				for i := 0; i < check.want+1; i++ {
					ids, next, _, e := check.fetch(size, cursor, 0)
					if e != nil {
						t.Fatal(e)
					}
					if len(ids) > int(size) {
						t.Fatal("page_size ignored")
					}
					for _, id := range ids {
						if seen[id] {
							t.Fatalf("duplicate %s", id)
						}
						seen[id] = true
						all = append(all, id)
					}
					cursor = next
					if cursor == "" {
						break
					}
				}
				if len(all) != check.want {
					t.Fatalf("size%d got%d want%d", size, len(all), check.want)
				}
				if reference == nil {
					reference = all
				} else if !reflect.DeepEqual(reference, all) {
					t.Fatalf("size%d changes order", size)
				}
			}
			if check.offset {
				first, c, _, e := check.fetch(7, "", 1)
				if e != nil {
					t.Fatal(e)
				}
				page, _, _, e := check.fetch(7, "", 2)
				if e != nil {
					t.Fatal(e)
				}
				continued, _, _, e := check.fetch(7, c, 0)
				if e != nil {
					t.Fatal(e)
				}
				if len(first) != 7 || !reflect.DeepEqual(page, continued) {
					t.Fatalf("legacypage2 differs from cursor: %v vs%v", page, continued)
				}
				_, _, _, e = check.fetch(7, "", 101)
				if status.Code(e) != codes.ResourceExhausted {
					t.Fatalf("deep page:%v", e)
				}
			}
		})
	}
}

func TestV1FiltersRetentionAndSource(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	const from = "0xaaa0000000000000000000000000000000000001"
	const to = "0xbbb0000000000000000000000000000000000002"
	_, err := h.pool.Exec(ctx, `UPDATE transactions SET categories=CASE WHEN block_number=3 THEN 8 ELSE 1 END,status=CASE WHEN block_number=4 THEN 0 ELSE 1 END`)
	if err != nil {
		t.Fatal(err)
	}
	requests := []struct {
		name string
		req  *indexerv1.ListTransactionsRequest
		want []uint64
	}{
		{"half-open-block", &indexerv1.ListTransactionsRequest{BlockRange: &indexerv1.BlockRange{FromBlock: 2, ToBlock: 4}}, []uint64{3, 2}},
		{"failed", &indexerv1.ListTransactionsRequest{Status: indexerv1.TransactionStatus_TRANSACTION_STATUS_FAILED}, []uint64{4}},
		{"category", &indexerv1.ListTransactionsRequest{CategoryIn: []indexerv1.TransactionCategory{indexerv1.TransactionCategory_TRANSACTION_CATEGORY_TOKEN_TRANSFER}}, []uint64{3}},
		{"time", &indexerv1.ListTransactionsRequest{TimeRange: &indexerv1.TimeRange{Start: timestamppb.New(time.Unix(1700000002, 0)), End: timestamppb.New(time.Unix(1700000004, 0))}}, []uint64{3, 2}},
		{"fractional-time", &indexerv1.ListTransactionsRequest{TimeRange: &indexerv1.TimeRange{Start: timestamppb.New(time.Unix(1700000002, 1)), End: timestamppb.New(time.Unix(1700000004, 1))}}, []uint64{4, 3}},
		{"from-does-not-include-recipient", &indexerv1.ListTransactionsRequest{Filter: &indexerv1.ListTransactionsRequest_ByAddress{ByAddress: &indexerv1.ListTransactionsRequest_AddressFilter{Address: to, Direction: indexerv1.AddressDirection_ADDRESS_DIRECTION_FROM}}}, nil},
		{"to-does-not-include-sender", &indexerv1.ListTransactionsRequest{Filter: &indexerv1.ListTransactionsRequest_ByAddress{ByAddress: &indexerv1.ListTransactionsRequest_AddressFilter{Address: from, Direction: indexerv1.AddressDirection_ADDRESS_DIRECTION_TO}}}, nil},
		{"and", &indexerv1.ListTransactionsRequest{BlockRange: &indexerv1.BlockRange{FromBlock: 2, ToBlock: 4}, Status: indexerv1.TransactionStatus_TRANSACTION_STATUS_FAILED}, nil},
	}
	for _, c := range requests {
		t.Run(c.name, func(t *testing.T) {
			r, e := h.client.ListTransactions(ctx, c.req)
			if e != nil {
				t.Fatal(e)
			}
			var got []uint64
			for _, v := range r.Transactions {
				got = append(got, v.BlockNumber)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got%v want%v", got, c.want)
			}
		})
	}
	// Reject unsupported values, do not silently turn them into unfiltered reads.
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Status: 99})
	requireCode(t, err, codes.InvalidArgument)
	_, err = h.client.ListAddresses(ctx, &indexerv1.ListAddressesRequest{SortBy: indexerv1.ListAddressesRequest_SORT_BY_NATIVE_BALANCE})
	requireCode(t, err, codes.InvalidArgument)
	_, err = h.client.ListTokens(ctx, &indexerv1.ListTokensRequest{TokenType: 99})
	requireCode(t, err, codes.InvalidArgument)
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{Cursor: "eyJiIjo0LCJ0IjowfQ"}})
	requireCode(t, err, codes.InvalidArgument)
	first, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.pool.Exec(ctx, `UPDATE indexer_dataset_metadata SET dataset_epoch=gen_random_uuid() WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 1, Cursor: first.Page.NextCursor}})
	requireCode(t, err, codes.InvalidArgument)
	// Publish a new floor while the old partitions still physically contain rows.
	// A cached W=0 must not leak those rows into any subsequent list response.
	if _, err = h.client.GetChainStats(ctx, &indexerv1.Empty{}); err != nil {
		t.Fatal(err)
	}
	_, err = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=2,pruned_through_ts=1700000002 WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Transactions) != 3 || r.Transactions[2].BlockNumber != 3 || r.Page.NextCursor == "" {
		t.Fatalf("retention leaked rows or falsely ended:%v", r)
	}
	_, err = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=3,pruned_through_ts=1700000003 WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 100, Cursor: r.Page.NextCursor}})
	requirePrunedFloor(t, err, "4")
	_, err = h.client.ListTransactionsPaginated(ctx, &indexerv1.ListTransactionsPaginatedRequest{Page: &indexerv1.OffsetPageRequest{PageSize: 2, Page: 2}})
	requirePrunedFloor(t, err, "4")
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{BlockRange: &indexerv1.BlockRange{ToBlock: 4}})
	requirePrunedFloor(t, err, "4")
	// A fully retained bounded interval has a normal end, even with retention on.
	r, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{BlockRange: &indexerv1.BlockRange{FromBlock: 4, ToBlock: 6}})
	if err != nil || len(r.Transactions) != 2 || r.Page.NextCursor != "" {
		t.Fatalf("retained range:%v %v", r, err)
	}
	// A current catalog is unrelated to the historical floor.
	tokens, err := h.client.ListTokens(ctx, &indexerv1.ListTokensRequest{})
	if err != nil || tokens.Page.NextCursor != "" {
		t.Fatalf("catalog incorrectly pruned:%v %v", tokens, err)
	}
}
func requirePrunedFloor(t *testing.T, err error, floor string) {
	t.Helper()
	requireCode(t, err, codes.OutOfRange)
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "HISTORY_PRUNED" && info.Domain == "indexer.ops" && info.Metadata["history_from_block"] == floor {
			return
		}
	}
	t.Fatalf("missing HISTORY_PRUNED floor=%s:%v", floor, err)
}

func TestV1ChildFiltersAndPrunedContinuations(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	const address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const other = "0xdddddddddddddddddddddddddddddddddddddddd"
	const token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const topic = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	for block := 1; block <= 5; block++ {
		_, err := h.pool.Exec(ctx, `INSERT INTO logs(tx_hash,log_index,address,topic0,data,block_number) VALUES($1,0,$2,$3,'0x',$4)`, txHash(uint64(block)), address, topic, block)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO token_transfers(tx_hash,log_index,token_address,from_address,to_address,value,block_number) VALUES($1,0,$2,$3,$4,1,$5)`, txHash(uint64(block)), token, address, other, block)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO internal_transactions(tx_hash,block_number,trace_address,from_address,to_address,value,call_type) VALUES($1,$2,'0',$3,$4,0,'call')`, txHash(uint64(block)), block, address, other)
		if err != nil {
			t.Fatal(err)
		}
	}
	logs, err := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: address, Topic0: topic, BlockRange: &indexerv1.BlockRange{FromBlock: 2, ToBlock: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Logs) != 2 || logs.Logs[0].BlockNumber != 3 || logs.Logs[1].BlockNumber != 2 || logs.TotalCount != 2 {
		t.Fatalf("combined log filters:%v", logs)
	}
	logs, err = h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByTxHash: txHash(3), ByAddress: other})
	if err != nil || len(logs.Logs) != 0 {
		t.Fatalf("by-tx ignored address:%v %v", logs, err)
	}
	logs, err = h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByTxHash: txHash(3), BlockRange: &indexerv1.BlockRange{ToBlock: 3}})
	if err != nil || len(logs.Logs) != 0 {
		t.Fatalf("by-tx ignored half-open upper:%v %v", logs, err)
	}
	transfers, err := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByAddress: address, ByToken: token, BlockRange: &indexerv1.BlockRange{FromBlock: 2, ToBlock: 4}})
	if err != nil || len(transfers.Transfers) != 2 || transfers.Transfers[0].BlockNumber != 3 || transfers.TotalCount != 2 {
		t.Fatalf("combined transfer filters:%v %v", transfers, err)
	}
	transfers, err = h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByTxHash: txHash(3), ByToken: other})
	if err != nil || len(transfers.Transfers) != 0 {
		t.Fatalf("by-tx ignored token:%v %v", transfers, err)
	}
	internals, err := h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{ByTxHash: txHash(3), ByAddress: token})
	if err != nil || len(internals.InternalTransactions) != 0 {
		t.Fatalf("by-tx ignored internal address:%v %v", internals, err)
	}
	firstLogs, err := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: address, Page: &indexerv1.PageRequest{PageSize: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: address, Topic0: topic, Page: &indexerv1.PageRequest{PageSize: 1, Cursor: firstLogs.Page.NextCursor}})
	requireCode(t, err, codes.InvalidArgument)
	firstTransfers, err := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByToken: token, Page: &indexerv1.PageRequest{PageSize: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByToken: token, BlockRange: &indexerv1.BlockRange{FromBlock: 2}, Page: &indexerv1.PageRequest{PageSize: 1, Cursor: firstTransfers.Page.NextCursor}})
	requireCode(t, err, codes.InvalidArgument)
	_, err = h.pool.Exec(ctx, `UPDATE prune_state SET pruned_through=2,pruned_through_ts=1700000002 WHERE id=1`)
	if err != nil {
		t.Fatal(err)
	}
	logs, err = h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: address})
	if err != nil || len(logs.Logs) != 3 || logs.Logs[2].BlockNumber != 3 || logs.Page.NextCursor == "" {
		t.Fatalf("log floor leak/false end:%v %v", logs, err)
	}
	_, err = h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByAddress: address, Page: &indexerv1.PageRequest{Cursor: logs.Page.NextCursor}})
	requirePrunedFloor(t, err, "3")
	transfers, err = h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByToken: token})
	if err != nil || len(transfers.Transfers) != 3 || transfers.Transfers[2].BlockNumber != 3 || transfers.Page.NextCursor == "" {
		t.Fatalf("transfer floor leak/false end:%v %v", transfers, err)
	}
	_, err = h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{ByToken: token, Page: &indexerv1.PageRequest{Cursor: transfers.Page.NextCursor}})
	requirePrunedFloor(t, err, "3")
	internals, err = h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{ByAddress: address})
	if err != nil || len(internals.InternalTransactions) != 3 || internals.InternalTransactions[2].BlockNumber != 3 || internals.Page.NextCursor == "" {
		t.Fatalf("internal floor leak/false end:%v %v", internals, err)
	}
	_, err = h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{ByAddress: address, Page: &indexerv1.PageRequest{Cursor: internals.Page.NextCursor}})
	requirePrunedFloor(t, err, "3")
	blocks, err := h.client.ListBlocks(ctx, &indexerv1.ListBlocksRequest{})
	if err != nil || len(blocks.Blocks) != 3 || blocks.Blocks[2].Number != 3 || blocks.Page.NextCursor == "" {
		t.Fatalf("block floor leak/false end:%v %v", blocks, err)
	}
	_, err = h.client.ListBlocks(ctx, &indexerv1.ListBlocksRequest{Page: &indexerv1.PageRequest{Cursor: blocks.Page.NextCursor}})
	requirePrunedFloor(t, err, "3")
	// An interrupted destructive operation cannot leak a partial retained set.
	_, err = h.pool.Exec(ctx, `CREATE TABLE wipe_in_progress(started_at timestamptz)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{})
	requireCode(t, err, codes.Unavailable)
}
