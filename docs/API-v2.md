# Indexer V2 API

`chain_indexer.v2.IndexerService` is a typed, consumer-agnostic read API. Its
protocol files are in `proto/chain_indexer/v2`; generated Go clients are in
`gen/go/chain_indexer/v2`. V1 remains a separately registered package.

The API has no viewer authentication, access rules or redaction. Privacy
installations isolate its port so only OPS can connect. Consumers authorize and
redact data. No request contains a database DSN, SQL expression, viewer token,
organization permission, policy or choice of upstream node.

## Read boundaries

Every RPC accepts `ReadContext`. The first request may omit `read_at`, epoch and
run floor. The response selects and returns an anchor, dataset epoch and run
floor. Continuations send all three unchanged, plus their typed `start_after`.
A continuation without that complete context is invalid. A caller retains the
same deadline and operation budget across calls.

Each call checks the following inside one `REPEATABLE READ READ ONLY`
transaction, queries its rows in that transaction, then waits for a known
successful commit before returning usable results:

1. Read the durable dataset epoch and initialized chain identity. An epoch
   mismatch is `ABORTED / DATASET_EPOCH_CHANGED`. Uninitialized identity or an
   empty dataset is `UNAVAILABLE / INDEXER_BEHIND`.
2. Read the fresh retention watermark. A pruned anchor or resume position is
   `OUT_OF_RANGE / HISTORY_PRUNED`; this check precedes contiguity failures.
3. Validate the exact anchor block number and 32-byte hash. A hash mismatch is
   `ABORTED / CHAIN_REORGED`; an anchor above the stored head is
   `UNAVAILABLE / INDEXER_BEHIND`.
4. Prove actual block coverage of the retained run. A physical block count is
   authoritative; `missing_block_ranges` is only a hint. On an initial mismatch,
   the highest actual hole determines the floor using a bounded ordered gap
   query. Empty spans, a missing first block and a missing anchor are handled
   explicitly. A new gap in a pinned retained run is
   `ABORTED / INDEX_RUN_CHANGED`.

A dataset beginning at block 1 is incomplete at block 0; zero is not silently
assumed empty. Unbounded exact aggregates fail with `INDEXER_BEHIND` there.
A caller may explicitly request a covered range beginning at 1. Complete-history
ingest includes the actual configured node's empty genesis header when applicable.

Backfill never widens a pinned run. When retention advances, the original
`run_floor_block` stays unchanged and coverage is checked only over
`max(history_from_block, run_floor_block)..read_at`. Ordinary pruning is not an
index-run change. Lists clip ranges to those floors, never returning rows still
physically present below the watermark. Exact aggregates return
`HISTORY_PRUNED` if their requested scope intersects pruned history and
`INDEXER_BEHIND` if it extends below the index run; they never silently clip.

This proves coverage of retained indexed block facts. It does not prove that
trace ingestion captured every chain frame. Internal transactions are all
retained indexed frames; historical receipt-skipping ingest settings can also
leave indexed children absent. Coverage does not certify capture of every chain
receipt, log or trace. A consumer requiring authoritative traces verifies
those frames against its node before display.

## Paging and keys

Default page size is 25; the maximum is 100. Zero selects the default, negative
is invalid, and an excessive value clamps to the maximum. Default order is
descending. All supported orders include their unique tie-breaker.

| Family | Key | Consistency |
|---|---|---|
| Blocks | number | SNAPSHOT |
| Transactions | block, transaction index, hash | SNAPSHOT |
| Logs | block, log index | SNAPSHOT |
| Transfers | block, log index | SNAPSHOT |
| Internals | block, transaction hash, trace address in C collation | SNAPSHOT of indexed frames |
| Contracts and tokens | creation block, address | CURRENT |
| Addresses | address | CURRENT |
| Balances | token address, holder address | CURRENT |
| Holders | numeric balance, address | CURRENT |
| Counterparties | address | CURRENT, atomic exact set |
| Search | kind, block, identity | CURRENT for mixed kinds |

Search visits kinds in the search key's total order and applies the typed
position in each family's SQL seek. It returns a scan-position continuation at
the page or shared candidate budget, including zero-match pages; it does not
materialize a whole matching catalog before returning the first page. Historical
search scope reports the applicable retention or index floor when exhausted.

Every returned fact carries its own typed key. `PageInfo.next_start_after` is
the last **scanned** position, including an empty page cut by the scan budget.
A consumer stopping partway through a response resumes from the last row it
used. Only an absent continuation means terminal. A terminal response reports
`END_OF_RANGE`, `RETENTION_FLOOR` or `INDEX_FLOOR`; a nonterminal response has
`ENDED_AT_UNSPECIFIED`. Enum values have their protobuf `ENDED_AT_` prefix.

CURRENT values are not historical snapshots. In particular, mutable balances
and holder ordering can cause skips or repeats across pages. Contracts and token
metadata can be backfilled after their creation blocks were indexed. No list
response contains an approximate or unfiltered total.

## Typed filters and exact aggregates

Addresses are 20 bytes, hashes and topics are 32 bytes. Quantity strings are
lossless decimal values; optional fields preserve SQL nulls. Native ABI JSON is
one fixed contract fact, never a query language. Blocks and timestamp ranges
are inclusive. Timestamps refer to block timestamps, not row creation times.

Set fields are messages: absent means no restriction; present and empty means
match nothing. Different filter fields combine with AND. Transaction
`destinations` is a union of destination intervals and exact creation keys,
ANDed with the remaining filters. A destination interval is half open at
`(block, transaction_index)` precision; it is never rounded to a block. A
creation requires a NULL transaction destination and matches its full
`(hash, block, index)` key. A single transaction hash
can be combined with these filters. Exact block selectors and a known indexed
parent hash narrow the block scope by intersection before retention checks;
the parent lookup uses the same read snapshot. Contradictory selectors match
nothing. An unknown hash does not establish coverage or turn missing history
into an exact zero. Broad time filters do not expand an explicitly narrower
block scope. V2 never implicitly applies `HiddenTxTypes`.

`ListCounterparties` returns the complete bounded exact destination set with
per-destination block bounds and global bounds over all selected transactions,
including creations whose contract metadata has not been backfilled. It includes
ordinary destinations and
contracts of top-level creations only: the transaction's `to` is NULL and the
contract joins on both creation hash and block. Factory-created contracts and
contracts from another block do not widen this set. Over-cap sets fail; they
are not truncated or paged. Protected consumers re-read this CURRENT set with
the original selectors and compare their canonical digest before delivery.

`BatchFirstActivity` preserves target input order. Each entry contains the first
matching transaction in ascending `(block, index, hash)` order, or an absent
transaction for a target without a match. All destination and scope predicates
still apply. `CountTransactions`, `CountLogs` and `CountTokenTransfers` use the
same filters as their corresponding lists and return an exact answer or error.

## Points and batch children

`GetBlock` and `GetTransaction` explicitly return `SOURCE_INDEXER` or
`SOURCE_NODE`. Only these point methods may use configured node lookup for
pruned data; range methods never fall back to the node. A point transaction
contains parent and receipt metadata only. It remains usable when a batch's
children exceed their cap.

`BatchGetTransactions` accepts at most 256 hashes and preserves request order,
including duplicates. It returns all retained indexed logs, transfers and
internal frames for every parent, or an explicit error for the whole call.
Missing parents fail the entire batch with `NOT_FOUND`. Child queries restrict
both selected parent hashes and their block numbers; they do not depend on an
optional global transaction-hash index. Child rows are not
silently truncated. Consumers can recover from a child cap using parent-only
`GetTransaction` and separately paged child methods.

## Admission and response ownership

V2 has four active application leases and at most 32 waiting requests. Its
nonblocking transport Tap reserves an active or waiting slot before decoding
and starts the original five-second context, bounded by any shorter caller
deadline. Waiting happens in the per-RPC dispatch goroutine before generated
request decoding; it never blocks the connection I/O goroutine or resets that
deadline. A full queue is `RESOURCE_EXHAUSTED / READ_BUDGET_EXCEEDED`.
Canceled or unadmitted requests are rejected by a fixed-method registration
guard **before** the generated decoder, even if their bytes are already buffered.

An active lease also owns its encoded response buffers. Handler return, RPC End
or expiration of the call deadline alone cannot release an outstanding encoded
payload. Release occurs exactly once after RPC End and either the final buffer
Free or terminal connection End. This caps active encoded payload reservations
at four times 16 MiB; decoded request reservations are at most four times 4 MiB.
The waiting slots do not decode application requests. The lease also covers
bounded fact construction and serialization. HTTP/2 transport bookkeeping,
transient runtime allocations and dead-connection objects awaiting garbage
collection are not an instantaneous RSS bound. A stalled connection can retain
its four charged payloads beyond the deadline; its closed transport cannot send
them, and connection End plus RPC End permits logical lease reuse. Buffer Free
still drops its references when physical cleanup follows. No handler-end or
TTL-only release admits another payload while the old live transport owns one.
V1 keeps its existing admission, contexts, decoder and codec behavior.

The database shared session history gate is acquired **before BEGIN**, using
`BeginHistorySnapshotWithLimits(ctx, 1s, 3s)` under the original call deadline.
This gives a one-second lock/pool wait and three-second statement timeout,
including a one-second transaction lock timeout; existing stricter nonzero
operator limits are preserved. A shorter caller deadline
still wins. The wipe marker is checked inside the resulting RR snapshot.
PostgreSQL TRUNCATE is not MVCC safe for a pre-wipe snapshot, so taking a
transaction advisory lock after BEGIN is insufficient. Cleanup rolls back
before unlocking and discards a connection whose bounded cleanup fails.

Focused transport validation covers an eight-request legitimate burst, all 32
waiting slots and a full queue, cancellation without slot leaks, pre-decode
rejection of buffered requests, the unchanged original deadline, send buffers
retained after handler End and TTL, final Free/connection cleanup, and V1
isolation. These tests exercise transport mechanics separately from the real
PostgreSQL paging contract; no replacement paging service supplies their data.

## Fixed resource bounds

| Bound | Maximum |
|---|---:|
| Entire call, including coverage proof and commit wait | 5 seconds |
| SQL statement | 3 seconds |
| SQL lock wait | 1 second |
| Request bytes | 4 MiB |
| Response bytes | 16 MiB |
| Individual fact field | 2 MiB |
| Total result/scanned candidate rows | 20,000 |
| Target addresses or items in a typed set | 4,096 |
| Combined destination intervals and exact creations | 16,384 |
| Batch parents | 256 |
| Each child family per parent | 20,000, also subject to global rows/bytes |
| Exact counterparties | 4,096 |

A parent consumes one global result row, so one batch parent cannot carry
20,000 child rows in addition to itself. Excessive batch children produce
`CHILD_LIST_TOO_LARGE`. Count execution stops with `COUNT_BUDGET_EXCEEDED` if
its exact count cannot be obtained within the fixed count/SQL budget. Filter
limits are checked before expensive work. Fixed SQL projections check fact byte
lengths and return an oversized marker instead of transferring an over-limit
field. Counts retain only a bounded match tally; batches and search share their
row and byte budget across their fixed queries. Caller cancellation propagates to
pgx. Timeout or uncertain read-commit completion never returns usable data.

## Error contract

Errors use `google.rpc.Status` and `google.rpc.ErrorInfo`, domain `indexer.ops`.
Reasons and metadata are fixed, sanitized values; never raw SQL or credentials.

| gRPC status | ErrorInfo reason |
|---|---|
| UNAVAILABLE | INDEXER_BEHIND, DATASET_UNAVAILABLE |
| ABORTED | CHAIN_REORGED, DATASET_EPOCH_CHANGED, INDEX_RUN_CHANGED |
| OUT_OF_RANGE | HISTORY_PRUNED; metadata history_from_block |
| NOT_FOUND | OBJECT_NOT_FOUND |
| DATA_LOSS | INCONSISTENT_DATA |
| RESOURCE_EXHAUSTED | COUNT_BUDGET_EXCEEDED, CHILD_LIST_TOO_LARGE, READ_BUDGET_EXCEEDED |
| INVALID_ARGUMENT | FILTER_SET_TOO_LARGE, INVALID_READ_CONTEXT, INVALID_KEY, INVALID_FILTER |
| DEADLINE_EXCEEDED | READ_DEADLINE_EXCEEDED |

Consumers treat these reasons as protocol, not strings from an upstream
failure. OPS maps unavailable and inconsistent-data errors to its fixed
Explorer-unavailable response. A viewer not authorized for an object gets the
same opaque result for not-found, pruned and node-sourced data. Count visibility
is authorized separately from exact count computation.

## Dataset identity and restoration

Migration 038 creates singleton `indexer_dataset_metadata`, with a random UUID
`dataset_epoch`, decimal `chain_id` and 32-byte `genesis_hash`. Node-derived
identity must be initialized before V2 traffic; a conflicting identity is not
silently accepted. Restarts preserve the epoch. Wipes and FORCE_REINDEX rotate
the epoch atomically before destructive data changes can be served.

A backup restore must rotate the epoch while API traffic is disabled, then
validate trusted network identity before traffic resumes. Restoring an identical
backup also restores its old UUID; the database cannot automatically detect
that event from the UUID alone. The operator restore command is therefore part
of the restoration protocol. An epoch is not an authorization token.

## Seek indexes and upgrade

Migration 040 adds exact C-collation seek indexes on the existing transaction,
internal-frame, catalog, address and current-balance tables. It also indexes the
contract-creation lookup. Existing unique block/log/transfer keys are reused.
These indexes add disk usage and write maintenance; they introduce no new state
tables or projections.

The migration runs transactionally during startup. Its one-second lock timeout
bounds lock acquisition; it does **not** bound the write-blocking build after the
lock is acquired. Drain indexing writers and plan a maintenance window for a
populated database. PostgreSQL does not support `CREATE INDEX CONCURRENTLY` on a
partitioned parent. Large deployments can prebuild ordinary-table indexes
concurrently and plan per-partition concurrent builds with parent attachment
before applying this migration. Existing valid named indexes are reused. Do not
resume the upgraded process until migrations complete. Newly created history
partitions inherit the parent seek indexes.

## Validation

`internal/grpcserver/v2_contract_test.go` exercises the production gRPC server
against a real migrated PostgreSQL container. It covers typed paging,
same-block ties, partial-batch resume, snapshot exclusion, physical gaps, epoch
and anchor changes, prune precedence, empty sets, explicit filtering, exact
intervals/counts, input ordering, null facts and batch cap recovery. No fake
paging service stands in for the Indexer. Buf lint and PACKAGE-level breaking
checks cover the protocol alongside those behavioral tests.
