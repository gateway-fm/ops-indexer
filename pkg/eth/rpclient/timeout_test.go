package rpclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// silentNode accepts every request and never answers until the test ends.
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

// A node that accepts a request and never answers fails the call at the timeout, single or batched, instead of
// holding it for as long as the connection stays open.
func TestCallsGiveUpAtTheTimeoutOnANodeThatNeverAnswers(t *testing.T) {
	c := DialTimeout(silentNode(t), 200*time.Millisecond)
	calls := map[string]func() error{
		"single": func() error {
			var h string
			return c.CallContext(context.Background(), &h, "eth_blockNumber")
		},
		"batch": func() error {
			return c.BatchCallContext(context.Background(), "eth_getBlockByNumber", [][]any{{"0x1", false}}, []any{new(map[string]any)})
		},
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

func TestClientsWithoutAnExplicitTimeoutGetTheDefault(t *testing.T) {
	for name, c := range map[string]*Client{"New(url, nil)": New("http://node", nil), "Dial": Dial("http://node")} {
		if c.httpClient.Timeout != DefaultTimeout {
			t.Fatalf("%s: timeout %s, want %s", name, c.httpClient.Timeout, DefaultTimeout)
		}
	}
}
