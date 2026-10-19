package rpc

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// The default transport keeps 2 idle connections per host; with the indexer's workers against one node nearly
// every request opened a new TCP connection and the pod ran out of ephemeral ports under load.
func TestClientReusesConnectionsToTheNode(t *testing.T) {
	var opened atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x2a"}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	c, err := New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	for range 50 {
		if _, err := c.BlockNumber(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := opened.Load(); n != 1 {
		t.Fatalf("50 sequential calls opened %d connections, want 1", n)
	}

	const workers, calls = 20, 25
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range calls {
				if _, err := c.BlockNumber(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if n := opened.Load(); n > 2*workers {
		t.Fatalf("%d workers x %d calls opened %d connections, want about one per worker", workers, calls, n)
	}
}
