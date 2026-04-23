# chain-indexer

A standalone EVM chain indexer with a gRPC read API. Polls an EVM node, writes to a private postgres, serves chain data to consumers (privacy-proxy, block-explorer) via gRPC.

**Scope.** Pure chain data: blocks, transactions, logs, addresses, tokens, transfers, contracts, internal txs, gas stats, daily aggregates, OP-Stack deposits. No RBAC, no privacy logic, no redaction. Consumers handle access control at their own layer.

## Layout

```
proto/chain_indexer/v1/       gRPC API definition (source of truth)
cmd/indexer/                  service entrypoint
internal/                     service implementation (postgres, indexing, gRPC handlers)
docs/API.md                   API conventions and design rationale
scripts/                      local dev helpers
```

## Generated code

Generated Go stubs are checked in under `gen/` so consumers don't need `buf` or `protoc` locally. Regenerate with `make proto-gen`.

## Origins

Extracted from `gateway-fm/block-explorer` (commit TBD) as part of RD-855 (privacy mode must physically prevent block-explorer from bypassing privacy-proxy). The indexer previously lived inside block-explorer's trust boundary; moving it to its own repo + service makes the trust boundary structural.

## Non-goals

- Authentication or authorization. Indexer runs on a trusted network and is reachable only by trusted consumers. See `docs/API.md`.
- Contract source verification / ABI management. That's a consumer concern.
- Privacy filtering, redaction, or per-viewer access control. Consumers handle it.
- WebSocket / SSE streaming. Subscriptions are deferred until a product requirement is confirmed.
