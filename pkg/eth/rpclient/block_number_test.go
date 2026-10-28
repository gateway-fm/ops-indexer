package rpclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// eth_blockNumber answering null or an empty number is an error, not head 0; "0x0" (a chain at genesis) and a plain
// JSON number are heads.
func TestBlockNumberRefusesANullOrEmptyHead(t *testing.T) {
	for _, c := range []struct {
		result string
		head   uint64
		ok     bool
	}{
		{`null`, 0, false},
		{`""`, 0, false},
		{`"0x"`, 0, false},
		{`"0x0"`, 0, true},
		{`"0x5a"`, 90, true},
		{`90`, 90, true},
	} {
		t.Run(c.result, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%s}`, c.result)
			}))
			defer srv.Close()
			head, err := New(srv.URL, nil).BlockNumber(context.Background())
			if c.ok && (err != nil || head != c.head) {
				t.Fatalf("head %d, error %v; want %d", head, err, c.head)
			}
			if !c.ok && err == nil {
				t.Fatalf("head %d with no error; want an error", head)
			}
		})
	}
}
