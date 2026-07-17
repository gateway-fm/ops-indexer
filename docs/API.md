# ops-indexer API design

Conventions for `chain_indexer.v1.IndexerService`. This document is the reference for consumer implementers and for future API changes. Breaking changes require a new package version (`v2`), never a mutation of `v1`.

## Trust model

The indexer is **not a security layer.** It returns whatever is requested. Authentication, authorization, privacy filtering, and redaction are consumer concerns.

In the Open Privacy Suite deployment:
- The indexer listens on a trusted Docker / k8s network only.
- Open Privacy Suite is the sole authorized consumer and applies per-viewer redaction before serving data to clients.

In the standalone ops-explorer deployment:
- The indexer serves the ops-explorer api directly.
- Data is public by intent; no redaction happens.

The indexer does not do tenant-aware access control, rate limiting, or audit logging on its own. Consumers apply those concerns.

## Versioning

- Package `chain_indexer.v1` is the stable contract. Once shipped, message fields may only be added, never removed or renamed in place.
- Field numbers are immutable. Deleted fields are reserved, not reused.
- Deprecated fields are marked `[deprecated = true]` but remain on the wire indefinitely within a major version.
- Breaking changes go to `chain_indexer.v2`. The indexer may serve both versions in parallel for a deprecation window; see the release notes of the first v2 release for the cut-off.
- CI enforces non-breaking changes via `buf breaking --against '.git#branch=main'`.

## Pagination

Two modes.

### Cursor pagination (default)

Use for:
- Feeds ordered by block number / timestamp descending (blocks, transactions, logs, transfers).
- Index lookups by address (tx lists per address, transfer lists per address).

```proto
message PageRequest {
  int32 page_size = 1;  // 0 = server default
  string cursor = 2;    // empty = first page
}
message PageResponse {
  string next_cursor = 1;  // empty = last page
}
```

- `cursor` is **opaque**. Server encodes; clients pass back verbatim.
- Current implementation encodes position as `(block_number, tx_index)` or similar and base64's it — but the encoding is internal. Clients must not parse or mutate cursors.
- `page_size` is clamped at server-configured caps (typical cap: 100).
- Clients traversing backwards from `next_cursor` must re-issue the original request with the new cursor — there is no reverse mode.

### Offset pagination

Use only for bounded browse endpoints where total is meaningful to show and users typically jump to a page:
- `ListAddresses`, `ListTokens`, `ListTokenHolders`, `ListTransactionsPaginated`, `ListVerifiedContracts` (not in v1).

`ListTransactionsPaginated` is the offset-paginated variant of the global
transaction feed: same rows as `ListTransactions` (no filter), but with a true
chain-wide `total_items` so browse-style UIs can render "page X of Y". Use the
cursor-based `ListTransactions` for by-address / by-block feeds.

```proto
message OffsetPageRequest {
  int32 page_size = 1;
  int32 page = 2;  // 1-based
}
message OffsetPageResponse {
  int32 page = 1;
  int32 page_size = 2;
  int64 total_items = 3;
}
```

- `total_items` is an exact count. On large tables the server may cap the scan and return `-1` for "too many to count" — consumers render "1000+" in that case.
- `page_size` cap matches cursor pagination (100 by default).

### Filter-wide counts on cursor list endpoints

`ListLogs`, `ListTokenTransfers`, and `ListInternalTransactions` stay
cursor-paginated, but each response also carries a `total_count` (int64) — the
exact number of rows matching the request filter, ignoring pagination — so
consumers can render a true "showing N of M". It is the count for the active
filter (e.g. all logs for `by_address`, all transfers of a `by_token`, every
log of a `by_tx_hash`), independent of `page_size`.

### Sort order

Sort order is **fixed per RPC** and documented in the proto comments. Clients cannot change it. If a different ordering is needed, that's a new RPC or a new request field, not a freeform `order_by` option.

## Scalar conventions

### Addresses and hashes

- Lowercase hex with `0x` prefix.
- Addresses: 42 chars including `0x`. Case is always lowercase in responses; requests are normalized server-side (clients may send EIP-55 checksum case or uppercase without issue).
- Transaction / block hashes: 66 chars including `0x`.
- Topic values: 66 chars.
- Indexer never emits a mixed-case address; consumers rendering EIP-55 checksums do the conversion themselves.

### Big integers

- Values that can exceed `uint64` (wei, total supply, big token balances) use `BigInt { string value = 1 }` with a decimal-string representation.
- Empty `BigInt.value` means "unknown" or "not populated." Consumers MUST distinguish empty from `"0"` — they are different.
- Clients should parse into their language's arbitrary-precision integer type.

### Timestamps

- All timestamps are UTC `google.protobuf.Timestamp`.
- Block timestamp is the header timestamp (seconds since epoch at 1 s resolution as emitted by the chain). The indexer does not round or re-timestamp.

### Nullable / optional fields

proto3 semantics by default — unset primitive fields default to zero / empty. Where this is ambiguous (e.g., distinguishing "not populated" from "zero"), the proto uses an explicit sentinel documented in the field comment. Notable cases:

- `Transaction.to` — empty string means "contract creation".
- `Transaction.contract_address` — populated only on contract-creation txs.
- `Transaction.effective_gas_price` — empty `BigInt` on failed / pre-1559 txs.
- `Block.base_fee_per_gas` — empty on pre-EIP-1559 blocks.
- `Contract.proxy_implementation` — empty when `is_proxy == false`.
- `Address.native_balance` — empty when the indexer is not configured to track native balances.

## Filter semantics

- Filters on a request are ANDed. There is no OR builder and no free-form `where` string.
- When the proto uses `oneof` for mutually exclusive filters (e.g., `ListTransactionsRequest.filter`), exactly one option is allowed.
- Filters that require at least one criterion (e.g., `ListLogsRequest`) return `InvalidArgument` if none is provided. This prevents accidental full-table scans.

## Error model

Standard gRPC status codes:

| Code | When |
|---|---|
| `OK` | Success. |
| `NotFound` | Entity does not exist in the index (e.g., `GetBlock` on an unseen number). |
| `InvalidArgument` | Malformed address / hash / cursor, missing required filter, page size out of range. |
| `Unavailable` | Feature gated off for this indexer's chain (e.g., OP-Stack RPCs on a non-OP chain), or indexer is still syncing through startup. |
| `FailedPrecondition` | Indexer is not yet ready to serve (e.g., migration not applied, DB unreachable). |
| `ResourceExhausted` | Server-side cap hit (rare; typically a symptom of a client not using pagination). |
| `Internal` | Bug. Consumer should surface opaque error to users. |

The indexer never puts PII or DB internals into error messages. Messages are short and enum-like (e.g., `"cursor malformed"`, `"page_size exceeds cap"`).

## Trust model implications for the API surface

A few concrete design choices flow from "indexer is not a security layer":

- **No `viewer` field on any request.** The indexer has no concept of who is asking. If a consumer needs per-viewer filtering, it does that filtering after receiving the response.
- **No redaction.** `Transaction.from`, `Address.address`, `Log.address` are always the real values. Pseudonyms and address masking are Open Privacy Suite's layer.
- **No ACL checks.** Any consumer with gRPC access can read any data.
- **No audit logging of consumer reads.** Consumers run their own audit pipelines. The indexer may log at the method level for operational observability, but not for compliance.

## Out of scope (and why)

These are **deliberately not** in v1:

- **Subscriptions / streaming.** Deferred until product confirms WS is a requirement. See RD-855 rationale.
- **Contract ABI, source code, verified flag.** Not chain data. Lives with ops-explorer's api in standalone mode; absent in privacy mode by product decision.
- **Price history time series for tokens.** Price is pulled from external feeds (CoinGecko, etc.) by consumers; the indexer carries only a single current `price_usd`.
- **Cross-chain queries.** One indexer instance indexes one chain. Multi-chain deployments run multiple indexers.
- **Mutations / writes.** No RPC writes. The indexer's own workers are the only writers to its DB.

## Hot path performance expectations

The following RPCs are called on every common UI load and have strict latency targets:

| RPC | Target p99 on a warm cache |
|---|---|
| `GetLatestBlockNumber` | < 5 ms |
| `GetChainStats` | < 10 ms (server-cached) |
| `GetBlock` | < 10 ms |
| `GetTransaction` | < 15 ms |
| `ListBlocks` (first page, page_size=25) | < 30 ms |
| `ListTransactions` (first page, page_size=25, no filter) | < 40 ms |
| `GetAddress` | < 20 ms |
| `Search` | < 50 ms |

These are internal targets for the indexer implementation. Consumers should add their own caching where appropriate.

## Consumer integration notes

### Open Privacy Suite

- Drops direct SQL to the chain-data DB. Replaces `internal/explorer/store.go` with a thin gRPC client.
- `RedactionEngine` redacts gRPC responses (mutates addresses / values / logs in-place on the response objects) before serializing to JSON for the client.
- A feature flag controls cutover during Phase 3; both code paths exist side-by-side briefly.

### ops-explorer (standalone mode only)

- api service calls the indexer for chain data and merges with its own small DB for verification metadata (ABI, source, verified).
- WS hub is deleted in Phase 4. No replacement in v1.
- `public-api` service is consolidated into `api` with a middleware differentiating authed / anonymous requests; both paths go through the same gRPC client.

### Privacy-mode deployment

- ops-explorer api, ops-explorer's own postgres, and ops-explorer's own indexer are **not deployed**.
- ops-explorer frontend nginx routes `/api/*` to Open Privacy Suite. `/ws` returns 404.
- Indexer listens on the trust network; Open Privacy Suite's backend is the only consumer.
