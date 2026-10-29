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

## History retention

With `PRUNE_RETAIN_BLOCKS` set (0 = off, the default), the indexer keeps only that many blocks of history (blocks,
transactions, logs, token transfers, internal transactions, balance history) below its head and drops older ranges.
Current state (addresses, tokens, holders, balances, NFT inventory, contracts, daily stats) is never pruned.

Retention keeps every UTC day that a reorg can reach whole, because reverting a block recounts its day's daily stats
from the day's first block. A range is dropped only once all of its blocks are older than the UTC day of the block
12,800 below the head, so up to one day plus one partition more is kept than `PRUNE_RETAIN_BLOCKS`. On a chain that
makes more than `PRUNE_RETAIN_BLOCKS` blocks a day, ranges are dropped once a day, a day's worth in one pass. A
database that a build without this rule pruned into the current UTC day prunes nothing more until that day ends, and
until then a reorg into that day still stops ingest; the indexer logs a warning naming the day when retention starts.

Reading the history tables with SQL: since migration 014 each is partitioned by block, and a view or foreign key
created on one before 014 is bound to its first partition, `<table>_legacy`. From the first cut on such a view sees
no new block, and retention keeps the legacy range on disk, detached, while anything depends on it: it logs the
objects, at the cut and at every pass. Re-create them on `<table>` and the next pass drops the range.

`GetChainStats` reports the watermark: `pruned_through_block` is the newest pruned block (every block at or below it
is gone; 0 = nothing pruned) and `pruned_through_timestamp` its timestamp (unset when nothing is pruned). With nothing
pruned, every call behaves as before.

**`OutOfRange`** answers explicit block targets at or below the watermark:

- `GetBlock` by number.
- `ListTransactions` and `ListInternalTransactions` `by_block` by number.
- `BatchGetBlockTransactionCounts` when any requested block is at or below it.
- `ListBlocks` when its upper bound (`before_number` or the cursor, exclusive) is at most watermark + 1.
- `ListTransactions` and `ListTokenTransfers` `by_address` with `block_range.to_block` (exclusive) at most
  watermark + 1 and no cursor.
- `ListLogs` with `by_address` + `topic0` and `block_range.to_block` at or below the watermark: this path treats
  `to_block` as **inclusive**. `ListLogs` honours `block_range` on that path only.

**Hash lookups stay `NotFound`.** `GetBlock` by hash, `GetTransaction`, and `by_block` by hash cannot tell a pruned
entity from one that never existed; while anything is pruned, the message ends with
`(history at or below block W is pruned)`. `by_tx_hash` lists of a pruned transaction return empty.

Lists that run into the watermark while paging (the feeds, `by_address` lists, deep offset pages) just end: no error.

**With `PRUNED_LOOKUP=true`** a pruned target is served from the node instead: `GetBlock` by number or hash,
`GetTransaction`, and `ListTransactions` `by_block` by number or hash, at or below the watermark, answer with the
node's block or transaction, and the response carries the header `x-indexer-source: node`. When the node is busy or
does not answer they return `Unavailable`. The other calls above still answer `OutOfRange`, `NotFound` or end as
described.

**Totals count pruned history**, from counters the writer keeps (never a `COUNT(*)` over retained rows):

- `GetChainStats` totals, `GetAddress`, `BatchGetAddressTransactionCounts`, `GetToken`.
- `ListTransactionsPaginated` `total_items` (`transactions_total`), when no hidden tx types are configured.
- `ListAllTokenTransfers` `total_items` without a `token_type` filter (`transfers_total`).
- `ListTokenTransfers` `by_token` `total_count` (`tokens.transfer_count`).
- `ListTokenTransfers` `by_address` `total_count` (`address_stats.token_transfer_count`) and
  `ListInternalTransactions` `by_address` `total_count` (`address_stats.internal_tx_count`). Each counts a transfer or
  internal call once per address, whichever sides it is on, so `total_count` equals the rows the list returns.
  **Correction:** before migration 017 a self-transfer or self-call counted twice, here and in `GetAddress`; 017 subtracts
  the extra side from existing rows (it refuses on a pruned database).
- `GetAddress` and `BatchGetAddressTransactionCounts` transaction counts (`address_stats.tx_count`): one per
  transaction the address sent, received or took part in through a Transfer event, as `ListTransactions` `by_address`
  returns them. **Correction:** a self-send used to count twice, and an address in Transfer events of several
  transactions of one block once; rows written before this build keep the old count until they are recounted.
- The zero address keeps no transfer count (mints and burns): `ListTokenTransfers` `by_address` `total_count` and
  `GetAddress` `token_count` are 0 for it, and its `GetAddress` transaction count leaves out mint and burn transactions
  it is not the sender or recipient of.
- `ListTokenHolders` (`tokens.holder_count`), `ListTokens`, `ListAddresses`, `ListTokenInventory`, `GetDailyStats`:
  current state, unaffected by pruning.
- `GetDailyStats` `new_addresses`, `new_contracts`, `cumulative_addresses`, `cumulative_contracts` count an address on
  the day of the block it was first seen in. **Correction (migration 021):** they compared that block number with epoch
  seconds, so `new_addresses` and `new_contracts` were 0 on every day and the cumulative columns counted every address
  known when the row was written. 021 rewrites every stored day (it refuses on a pruned database); values can rise or
  fall per day, and the cumulative columns now grow day by day to the totals in `GetChainStats`.

**Within retention** (counts or rows limited to retained history):

- `ListLogs` `total_count` by address, by topic0, by address + topic0, and by range.
- `ListAllTokenTransfers` `total_items` when filtered by `token_type`.
- `ListTransactionsPaginated` `total_items` when hidden tx types are configured.
- `GetTransactionHistory` `HOUR` buckets when the retention window is shorter than 168 h.
- `Search` block and transaction suggestions.
- `by_tx_hash` lists (`ListLogs`, `ListTokenTransfers`, `ListInternalTransactions`): their `total_count` is the row
  count, 0 for a pruned transaction.
- `GetGasPrices` samples the latest 10,000 transactions; a window holding fewer samples fewer.
- The rows of every list: they end at the watermark.

## Error model

Standard gRPC status codes:

| Code | When |
|---|---|
| `OK` | Success. |
| `NotFound` | Entity does not exist in the index (e.g., `GetBlock` on an unseen number). |
| `InvalidArgument` | Malformed address / hash / cursor, missing required filter, page size out of range. |
| `OutOfRange` | An explicit block target is at or below the retention watermark: that history was pruned on purpose (see [History retention](#history-retention)). |
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
