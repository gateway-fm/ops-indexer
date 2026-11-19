package grpcserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestV1BlockHashCursorAndReorgBinding(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	oldHash := fmt.Sprintf("0x%064x", 5)
	newHash := fmt.Sprintf("0x%064x", 50005)
	for i := 1; i <= 2; i++ {
		_, err := h.pool.Exec(ctx, `INSERT INTO transactions(hash,block_number,block_timestamp,tx_index,from_address,to_address,value,gas_used,gas_price,tx_type,input_data,status) VALUES($1,5,1700000005,$2,'0xaaa','0xbbb',0,21000,1,0,'0x',1)`, fmt.Sprintf("0x%064x", 90000+i), i)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.pool.Exec(ctx, `INSERT INTO internal_transactions(tx_hash,block_number,trace_address,from_address,to_address,value,call_type) VALUES($1,5,$2,'0xaaa','0xbbb',0,'call')`, txHash(5), fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	txRequest := func(hash, cursor string) *indexerv1.ListTransactionsRequest {
		return &indexerv1.ListTransactionsRequest{Filter: &indexerv1.ListTransactionsRequest_ByBlock{ByBlock: &indexerv1.ListTransactionsRequest_BlockFilter{Selector: &indexerv1.ListTransactionsRequest_BlockFilter_Hash{Hash: hash}}}, Page: &indexerv1.PageRequest{PageSize: 1, Cursor: cursor}}
	}
	internalRequest := func(hash, cursor string) *indexerv1.ListInternalTransactionsRequest {
		return &indexerv1.ListInternalTransactionsRequest{ByBlock: &indexerv1.ListInternalTransactionsRequest_BlockFilter{Selector: &indexerv1.ListInternalTransactionsRequest_BlockFilter_Hash{Hash: hash}}, Page: &indexerv1.PageRequest{PageSize: 1, Cursor: cursor}}
	}
	first, err := h.client.ListTransactions(ctx, txRequest(oldHash, ""))
	if err != nil {
		t.Fatal(err)
	}
	if first.Page.NextCursor == "" {
		t.Fatal("fixture requires second transaction page")
	}
	firstInternal, err := h.client.ListInternalTransactions(ctx, internalRequest(oldHash, ""))
	if err != nil {
		t.Fatal(err)
	}
	if firstInternal.Page.NextCursor == "" {
		t.Fatal("fixture requires second internal page")
	}
	_, err = h.pool.Exec(ctx, `UPDATE blocks SET hash=$1 WHERE number=5`, newHash)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.client.ListTransactions(ctx, txRequest(newHash, first.Page.NextCursor))
	requireCode(t, err, codes.InvalidArgument)
	_, err = h.client.ListInternalTransactions(ctx, internalRequest(newHash, firstInternal.Page.NextCursor))
	requireCode(t, err, codes.InvalidArgument)
	// Queue each list behind the history gate. The old implementation already
	// resolved the hash outside that gate; then a replacement at the same height
	// could be returned for the hash that was removed while the read waited.
	for _, kind := range []string{"transactions", "internals"} {
		t.Run(kind, func(t *testing.T) {
			_, err := h.pool.Exec(ctx, `UPDATE blocks SET hash=$1 WHERE number=5`, oldHash)
			if err != nil {
				t.Fatal(err)
			}
			blocker, err := h.pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Release()
			_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock(4673,1)`)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				if kind == "transactions" {
					r, e := h.client.ListTransactions(ctx, txRequest(oldHash, ""))
					if e == nil && len(r.Transactions) > 0 {
						e = fmt.Errorf("returned replacement block transactions for removed hash")
					}
					result <- e
				} else {
					r, e := h.client.ListInternalTransactions(ctx, internalRequest(oldHash, ""))
					if e == nil && len(r.InternalTransactions) > 0 {
						e = fmt.Errorf("returned replacement block internals for removed hash")
					}
					result <- e
				}
			}()
			deadline := time.Now().Add(time.Second)
			waiting := false
			for time.Now().Before(deadline) {
				if err = blocker.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=4673 AND objid=1 AND NOT granted)`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !waiting {
				t.Fatal("read did not queue behind history gate")
			}
			_, err = blocker.Exec(ctx, `UPDATE blocks SET hash=$1 WHERE number=5`, newHash)
			if err != nil {
				t.Fatal(err)
			}
			_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock(4673,1)`)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case e := <-result:
				if status.Code(e) != codes.NotFound {
					t.Fatalf("removed original hash must be NotFound, got%v", e)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("list did not finish")
			}
		})
	}
}
