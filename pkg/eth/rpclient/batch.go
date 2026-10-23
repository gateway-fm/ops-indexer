package rpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// BatchCallContext sends one JSON-RPC batch: method once per args[i], each result decoded into results[i]. A null
// result leaves results[i] as it is; any element's error fails the batch.
func (c *Client) BatchCallContext(ctx context.Context, method string, args [][]any, results []any) error {
	if len(args) != len(results) {
		return fmt.Errorf("rpc: batch of %d calls with %d results", len(args), len(results))
	}
	if len(args) == 0 {
		return nil
	}
	start := time.Now()
	outcome := OutcomeTransportError
	defer func() { observe(method, outcome, start) }()

	reqs := make([]jsonrpcRequest, len(args))
	index := make(map[uint64]int, len(args))
	for i, a := range args {
		if a == nil {
			a = []any{}
		}
		id := c.nextID.Add(1)
		reqs[i] = jsonrpcRequest{JSONRPC: "2.0", Method: method, Params: a, ID: id}
		index[id] = i
	}
	body, err := json.Marshal(reqs)
	if err != nil {
		return fmt.Errorf("rpc: marshal batch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("rpc: read batch response: %w", err)
	}
	var replies []jsonrpcResponse
	if err := json.Unmarshal(respBody, &replies); err != nil {
		outcome = OutcomeDecodeError
		return fmt.Errorf("rpc: unmarshal batch response: %w", err)
	}
	if len(replies) != len(args) {
		outcome = OutcomeDecodeError
		return fmt.Errorf("rpc: batch of %d calls got %d replies", len(args), len(replies))
	}
	for _, r := range replies {
		i, ok := index[r.ID]
		if !ok {
			outcome = OutcomeDecodeError
			return fmt.Errorf("rpc: batch reply with unknown id %d", r.ID)
		}
		if r.Error != nil {
			outcome = OutcomeRPCError
			return fmt.Errorf("rpc: batch element %d: %w", i, r.Error)
		}
		if len(r.Result) == 0 || string(r.Result) == "null" {
			continue
		}
		if err := json.Unmarshal(r.Result, results[i]); err != nil {
			outcome = OutcomeDecodeError
			return fmt.Errorf("rpc: batch element %d: %w", i, err)
		}
	}
	outcome = OutcomeOK
	return nil
}
