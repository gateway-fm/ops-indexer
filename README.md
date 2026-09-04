# ops-indexer

A standalone EVM chain indexer with a gRPC read API. Polls an EVM node, writes to a private postgres, serves chain data to consumers (Open Privacy Suite, ops-explorer) via gRPC.

**Scope.** Pure chain data: blocks, transactions, logs, addresses, tokens, transfers, contracts, internal txs, gas stats, daily aggregates, OP-Stack deposits. No RBAC, no privacy logic, no redaction. Consumers handle access control at their own layer.

## Layout

```
proto/chain_indexer/v1/       gRPC API definition (source of truth)
cmd/indexer/                  service entrypoint
internal/                     service implementation (postgres, indexing, gRPC handlers)
docs/API.md                   API conventions and design rationale
scripts/                      local dev helpers
```

## Observability

| Port | Env | Default | Purpose |
| --- | --- | --- | --- |
| 50051 | `GRPC_LISTEN_ADDR` | `:50051` | gRPC read API |
| 8080 | `METRICS_LISTEN_ADDR` | `:8080` | Prometheus `/metrics` |
| 6060 | `PPROF_LISTEN_ADDR` | `:6060` | `net/http/pprof`, off by default |

`PPROF_ENABLED` (default `false`) must stay off outside active profiling, and the port
must never be reachable through an ingress: heap dumps can contain secret material and
the profile endpoints are a cheap denial of service.

Metric names and meanings are self-describing in the `/metrics` output; see
`internal/metrics` for the definitions. One thing that is not obvious from the names:
chain head and last indexed block are exported as separate gauges rather than a
pre-computed lag, so that when lag looks wrong you can see which of the two moved.

## Generated code

Generated Go stubs are checked in under `gen/` so consumers don't need `buf` or `protoc` locally. Regenerate with `make proto-gen`.

## Origins

Extracted from `gateway-fm/ops-explorer` (formerly `block-explorer`; commit TBD) as part of RD-855 (privacy mode must physically prevent ops-explorer from bypassing Open Privacy Suite). The indexer previously lived inside ops-explorer's trust boundary; moving it to its own repo + service makes the trust boundary structural.

## Non-goals

- Authentication or authorization. Indexer runs on a trusted network and is reachable only by trusted consumers. See `docs/API.md`.
- Contract source verification / ABI management. That's a consumer concern.
- Privacy filtering, redaction, or per-viewer access control. Consumers handle it.
- WebSocket / SSE streaming. Subscriptions are deferred until a product requirement is confirmed.
