# Benchmarking ingest

Three measurements, answering different questions. None substitutes for another.

| | question | needs |
|---|---|---|
| [Write-path benchmark](#write-path-benchmark) | did this commit make the inserts faster or slower? | docker, or a Postgres |
| [Derived-counter benchmarks](#derived-counter-benchmarks) | did it change `RefreshTokenStats`, or the real per-block cost? | same |
| [End-to-end ingest](#end-to-end-ingest-on-a-live-chain) | what sustained rate do we actually achieve? | a live chain + the indexer deployed |

**Pick the right one.** `BenchmarkInsertBlockDataBatch` covers the inserts only. On a real
chain the inserts have been a small fraction of total database time, with derived-counter
maintenance dominating — so a change to that path moves nothing in the insert benchmark.
`BenchmarkBlockCycle` is the number to watch when changing ingest.

## Write-path benchmark

`BenchmarkInsertBlockDataBatch` times `InsertBlockDataBatch`, which is the whole
realtime write path: one Postgres transaction per block covering `blocks`,
`transactions`, `logs`, `token_transfers`, `internal_transactions`,
`address_stats` and `chain_counters`.

```bash
make bench-quick      # write path, small table, minutes
make bench            # everything at the default scale, considerably longer
```

Override the table sizes with a comma-separated list:

```bash
BENCH_SCALES=100000,1000000 go test ./internal/db -run '^$' -bench InsertBlockDataBatch -benchtime 30x
```

### Choosing a scale, and why the default is large

The default is **10M rows**, which is deliberately not a convenient number.

At a sustained 500 tx/s a chain reaches 1M transactions in about **half an hour** and 43M in
a **day**, so a 1M-row table describes the first minutes of a chain's life.

Scale matters far more for the derived-counter benchmarks than for the insert benchmark, and
it is worth knowing which is which before spending an hour seeding.

**Insert cost barely moves with table size, and that is not an artefact of too small a
scale.** Measured against a server constrained to the deployed configuration
(`shared_buffers = 2GB`, 3 CPU, gp3), growing 100k → 10M transactions — 100×, ending with a
10 GB database whose indexes total 2.3× `shared_buffers` — cost the `erc20` insert path about
20%. Ingest is **append-ordered**: block numbers and hashes only increase, so every insert
lands on the right-hand edge of each B-tree and touches a handful of pages per index whatever
the table size. At 10M the index cache-hit rate was still **100.00%**, single-digit block
reads per table, with every miss in the heap. There is no index-residency cliff on this path.

> Two caveats. The synthetic hashes are **sequential**, whereas real transaction hashes are
> uniformly random — a random-key B-tree dirties a random leaf per insert and is far more
> sensitive to cache residency, so this is an optimistic bound on real index maintenance. And
> at 10M the run-to-run spread widens to ~25% (checkpoint and autovacuum pressure) from under
> 5% at 100k, so treat small deltas at the top scale with more suspicion, not less.

**The derived-counter path is where scale changes the answer**, because its cost tracks
stored history rather than the current block, and it grows very nearly linearly. A small
scale understates it by roughly the factor it understates the history. See below.

So: size the scale against the *deployment*, and constrain the server's memory so the
working-set-to-RAM ratio resembles production. Shrinking the server is much cheaper than
growing the table and reaches the same regime.

### Running against a real Postgres

testcontainers is convenient but caps the usable scale at what the local machine holds, and
inherits the host page cache. Set `BENCH_DATABASE_URL` to use an existing server instead:

```bash
BENCH_DATABASE_URL='postgres://user:pass@host:5432/benchdb?sslmode=disable' \
  BENCH_SCALES=10000000 go test ./internal/db -run '^$' -bench . -benchtime 30x -timeout 4h
```

Seeding is incremental: it counts what is already there and tops up, so re-running the *same*
scale against the same database skips seeding entirely. The benchmark writes to whatever you
point it at and never drops anything — use a throwaway database, not a real one.

**Use a fresh database for each scale.** Reusing one across *ascending* scales does not work,
and fails confusingly rather than cleanly. Seeding resumes from `COUNT(*) FROM transactions`,
but the benchmarks themselves also insert transactions, so after a run the row count no
longer matches the contiguous block of synthetic rows the seed laid down. The next scale
therefore skips a range of transaction hashes, and `benchSeedTokenHistory` — which derives
the `tx_hash` it hangs each token transfer off from a row index — generates hashes inside
that gap:

```
ERROR: insert or update on table "token_transfers" violates foreign key constraint
       "token_transfers_tx_hash_fkey"
DETAIL: Key (tx_hash)=(0xtx…) is not present in table "transactions".
```

So drive a multi-scale sweep as one invocation per scale, each against its own database,
rather than passing several scales in `BENCH_SCALES` with a persistent
`BENCH_DATABASE_URL`. (A `BENCH_SCALES` list is still fine on testcontainers, where every
sub-benchmark gets a brand-new container.)

Seeding generates rows one at a time rather than materialising them, so the client side costs
tens of MB regardless of scale; the memory that matters is the server's.

### Reading the output

Every run reports `blocks/s`, `tx/s` and `gas/s` alongside `ns/op`, where one op
is one block.

**Never quote `tx/s` on its own.** Ingest cost tracks block count and gas as
well as transaction count, so a bare `tx/s` figure is not comparable against
another run unless the shape matches. The benchmark defines three shapes for
exactly this reason:

| shape | per tx | what it exercises |
|---|---|---|
| `plain` | value transfer only | the `blocks` + `transactions` + `address_stats` floor |
| `erc20` | 1 log + 1 token transfer | the realistic path, including `logs` and `token_transfers` |
| `erc20-no-address-stats` | as above, `SkipAddressStats` | isolates the cost of `address_stats` maintenance |

Senders come from a pool the seed already wrote, so `address_stats` takes its
`ON CONFLICT DO UPDATE` branch — the steady-state hot path. Every tenth
recipient is new, so the `INSERT` branch and the `addresses_total` counter
increment are covered too.

### What it deliberately excludes

`RefreshTokenStats` is not on this path — it runs a level up, in the indexer — so these
numbers are insert cost alone. See the next section.

## Derived-counter benchmarks

`BenchmarkRefreshTokenStats` times one `RefreshTokenStats` call, and
`BenchmarkBlockCycle` times what the indexer actually does per block: the insert batch
followed by a refresh for each token the block touched.

These matter because the two halves scale on **different axes**:

- inserts scale with rows written per block, and barely with stored history;
- `RefreshTokenStats` scales with the history of the token being refreshed, and not at all
  with what the current block wrote.

For an ERC-20 token, one call issues three history-proportional statements — a `COUNT(*)`
over `token_transfers`, a `DISTINCT ON (address)` over `balances`, and a `SUM(...)` over
`token_transfers` inside the `UPDATE`. None is bounded by the block being processed, so cost
grows with the token's lifetime. A chain with one dominant token is the worst case, and is
what these benchmarks seed.

### What this costs, measured

Same constrained server as above (PG 17, `shared_buffers = 2GB`, 3 CPU, gp3), 250 tx/block,
one dominant ERC-20:

| scale | `RefreshTokenStats` | `BlockCycle` | `BlockCycle` tx/s |
|---|---|---|---|
| 100k | 32 ms | 71 ms | 3,521 |
| 1M | 264 ms | 305 ms | 820 |
| 10M | ~2.4 s | 2.53 s | **99** |

The 10M refresh figure is the sum of its three statements measured once the server had
settled. `BenchmarkRefreshTokenStats` itself reported 3.07 s, but it runs immediately after
the bulk seed while autovacuum is still working through the freshly loaded table, so it is
inflated; the number to quote is ~2.4 s.

Two things follow, and both are structural rather than tuning problems.

**It is very nearly linear in history.** Ten times the history costs about ten times as much,
per block, forever. The per-block cycle therefore collapsed **35×** between 100k and 10M, and
at 10M a single refresh costs about as much as sixty of the inserts it accompanies.

**It is CPU-bound, not I/O-bound.** At 10M only the `SUM` leaves `shared_buffers` at all, and
its disk I/O is ~176 ms of a 1,107 ms statement — the other 84%, and effectively all of the
other two statements, is CPU spent walking and aggregating rows that are already in memory.
Raising `shared_buffers` or moving to faster disks will not fix this; not recomputing
whole-history aggregates per block is the only thing that will.

That is also why `BenchmarkInsertBlockDataBatch` cannot be the regression gate for ingest:
at 10M it accounts for under 2% of the per-block cost that `BenchmarkBlockCycle` measures.

### Comparing across commits

Use `benchstat`, not eyeballs — a single run has enough variance to invent or
hide a 10% change.

```bash
go install golang.org/x/perf/cmd/benchstat@latest
BENCH_SCALES=10000 go test ./internal/db -run '^$' -bench InsertBlockDataBatch -benchtime 30x -count 6 > before.txt
# ... apply the change ...
BENCH_SCALES=10000 go test ./internal/db -run '^$' -bench InsertBlockDataBatch -benchtime 30x -count 6 > after.txt
benchstat before.txt after.txt
```

`-benchtime 30x` pins the iteration count. Without it the framework ramps `b.N`
and re-seeds the table for every ramp step, which is both slow and means
successive trials measure different table sizes.

## End-to-end ingest on a live chain

The benchmark cannot tell you the achieved rate: real ingest is bounded by RPC
latency, block density, node contention and derived-counter work, none of which
a local container reproduces. Measure the deployed indexer against its own
database.

### Sustained rate

Bucket by **insert time**, and read the whole curve rather than two endpoints:

```sql
SELECT date_trunc('hour', created_at)                    AS hour,
       count(*)                                          AS blocks,
       round(count(*) / 3600.0, 2)                       AS blocks_per_s,
       round(sum(transaction_count) / 3600.0, 0)         AS tx_per_s,
       round(sum(gas_used) / 3600.0, 0)                  AS gas_per_s,
       round(avg(transaction_count), 0)                  AS avg_tx_per_block
FROM blocks
GROUP BY 1
ORDER BY 1;
```

**Bucket by time, never by block number.** The catchup worker runs several
workers in parallel and does not insert in block order, so
`max(created_at) - min(created_at)` within a range of block numbers is
meaningless — it can differ by two orders of magnitude between adjacent ranges
of the same size.

**Expect a cliff, not a decay.** Ingest rate is not a single number: a
from-genesis catchup can run orders of magnitude faster while the tables are
small and then flatten for hours. Two spot readings will describe a smooth
decline that did not happen. Report the hourly curve.

**Cost is per-block and cumulative, not per-transaction.** If the floor holds
steady while block density falls, transaction count is not what you are bound
by.

### Where the time actually goes

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;   -- see below

SELECT round(total_exec_time / 1000.0)::bigint AS total_s,
       calls,
       round(mean_exec_time, 3)                AS mean_ms,
       left(query, 120)                        AS query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
```

`shared_preload_libraries = 'pg_stat_statements'` is necessary but **not
sufficient** — without `CREATE EXTENSION` the view does not exist and the query
fails with `relation "pg_stat_statements" does not exist`. Counters accumulate
in shared memory from library load onward, so creating the extension after a run
still exposes that run's history. Put it in the deployment's bootstrap so it is
never missing when you need it.

### Gotchas

- The column is `transactions.tx_type`, not `type`.
- `round(double precision, int)` does not exist in Postgres — cast to
  `::numeric` first.
- **`blocks/latest` is not a catch-up indicator.** The indexer follows the chain
  tip and backfills history behind it concurrently, so the API reports the tip
  within seconds of starting while most of history is still missing. Compare row
  counts against an independent indexer, or watch `catchup: progress` in the
  indexer log.
- Verify correctness against a second explorer on the same chain whenever a
  change touches derived counters. Cached counters on either side will differ
  slightly without either being wrong.
