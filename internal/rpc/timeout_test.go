package rpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func silentNode(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	return srv.URL
}

// Every call ingest, the node lookup and the balance reader make through the client fails with a timeout error on
// a node that never answers, never with an empty or not-found result.
func TestNodeCallsFailWithATimeoutOnANodeThatNeverAnswers(t *testing.T) {
	c, err := NewWithTimeout(silentNode(t), 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	calls := map[string]func() error{
		"BlockNumber": func() error { _, err := c.BlockNumber(ctx); return err },
		"RawBlockByNumber": func() error {
			b, err := c.RawBlockByNumber(ctx, 7)
			if err == nil && b == nil {
				return errors.New("nil block and no error")
			}
			return err
		},
		"RawBlockHash": func() error { _, err := c.RawBlockHash(ctx, 7); return err },
		"FetchBlockReceipts": func() error {
			r, err := c.FetchBlockReceipts(ctx, 7)
			if err == nil && r == nil {
				return errors.New("nil receipts and no error")
			}
			return err
		},
		"TraceBlock": func() error { _, err := c.TraceBlock(ctx, 7); return err },
	}
	for name, call := range calls {
		start := time.Now()
		err := call()
		var netErr net.Error
		if err == nil || !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("%s: want a timeout error, got %v", name, err)
		}
		if waited := time.Since(start); waited > 5*time.Second {
			t.Fatalf("%s: gave up after %s", name, waited)
		}
	}
}
