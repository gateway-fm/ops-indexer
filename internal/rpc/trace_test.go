package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// traceProbeServer answers eth_blockNumber with head and debug_traceBlockByNumber
// with traceErr (empty means success), recording the block the probe asked for.
func traceProbeServer(t *testing.T, head string, traceErr string, probed *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_blockNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%q}`, req.ID, head)
		case "debug_traceBlockByNumber":
			if len(req.Params) > 0 {
				if s, ok := req.Params[0].(string); ok {
					*probed = s
				}
			}
			if traceErr != "" {
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":%q}}`, req.ID, traceErr)
				return
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[]}`, req.ID)
		default:
			t.Errorf("unexpected method %q", req.Method)
		}
	}))
}

func TestCheckTracingSupport(t *testing.T) {
	tests := []struct {
		name        string
		head        string
		traceErr    string
		wantProbed  string
		wantSupport bool
		wantErr     bool
	}{
		{
			name:        "supported: probes a recent block, not genesis",
			head:        "0x64",
			wantProbed:  "0x61",
			wantSupport: true,
		},
		{
			name:        "supported: head below the offset probes genesis",
			head:        "0x2",
			wantProbed:  "0x2",
			wantSupport: true,
		},
		{
			name:        "unsupported: debug namespace absent",
			head:        "0x64",
			traceErr:    "the method debug_traceBlockByNumber does not exist/is not available",
			wantProbed:  "0x61",
			wantSupport: false,
		},
		{
			name:        "unsupported: signal matched case-insensitively",
			head:        "0x64",
			traceErr:    "Method Not Found",
			wantProbed:  "0x61",
			wantSupport: false,
		},
		{
			// Previously returned (true, nil): the indexer then enabled tracing
			// against a node that had not answered the probe.
			name:       "transient error is reported, not assumed to be support",
			head:       "0x64",
			traceErr:   "rate limit exceeded",
			wantProbed: "0x61",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var probed string
			srv := traceProbeServer(t, tt.head, tt.traceErr, &probed)
			defer srv.Close()

			client, err := New(srv.URL)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			supported, err := client.CheckTracingSupport(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatal("want an error, got nil")
				}
				if supported {
					t.Error("want supported=false alongside an error")
				}
			} else if err != nil {
				t.Fatalf("CheckTracingSupport: %v", err)
			}
			if supported != tt.wantSupport {
				t.Errorf("supported = %v, want %v", supported, tt.wantSupport)
			}
			if probed != tt.wantProbed {
				t.Errorf("probed block = %q, want %q", probed, tt.wantProbed)
			}
		})
	}
}

func TestCheckTracingSupportBlockNumberFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer srv.Close()

	client, err := New(srv.URL)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	supported, err := client.CheckTracingSupport(context.Background())
	if err == nil {
		t.Fatal("want an error when the head is unreadable")
	}
	if supported {
		t.Error("want supported=false when the head is unreadable")
	}
	if !strings.Contains(err.Error(), "block number") {
		t.Errorf("error should name the failing step, got %q", err)
	}
}
