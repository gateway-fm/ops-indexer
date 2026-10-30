package rpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Replies may come in any order: each result goes to the call with its id. A null result leaves its slot as it is,
// and one element's error fails the batch.
func TestBatchCallContextMatchesRepliesById(t *testing.T) {
	failOn := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []jsonrpcRequest
		if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		for i := len(reqs) - 1; i >= 0; i-- {
			arg := reqs[i].Params[0].(string)
			reply := map[string]any{"jsonrpc": "2.0", "id": reqs[i].ID}
			switch arg {
			case "null":
				reply["result"] = nil
			case failOn:
				reply["error"] = map[string]any{"code": -32000, "message": "boom"}
			default:
				reply["result"] = "r-" + arg
			}
			out = append(out, reply)
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := Dial(srv.URL)

	got := make([]string, 3)
	got[1] = "untouched"
	results := []any{&got[0], &got[1], &got[2]}
	if err := c.BatchCallContext(context.Background(), "m", [][]any{{"a"}, {"null"}, {"c"}}, results); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[r-a untouched r-c]" {
		t.Fatalf("got %v", got)
	}

	failOn = "c"
	if err := c.BatchCallContext(context.Background(), "m", [][]any{{"a"}, {"c"}}, []any{new(string), new(string)}); err == nil {
		t.Fatal("an element's error must fail the batch")
	}
}

func TestBatchCallContextReportsTheHTTPStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	err := Dial(srv.URL).BatchCallContext(context.Background(), "m", [][]any{{"a"}}, []any{new(string)})
	var status *HTTPStatusError
	if !errors.As(err, &status) || status.Code != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want HTTP 429", err)
	}
}
