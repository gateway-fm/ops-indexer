# Benchmarking ingest

Two measurements, answering different questions. Run both — neither substitutes
for the other.

| | question | needs |
|---|---|---|
| [Write-path benchmark](#write-path-benchmark) | did this commit make the insert path faster or slower? | docker |
| [End-to-end ingest](#end-to-end-ingest-on-a-live-chain) | what sustained rate do we actually achieve? | a live chain + the indexer deployed |

## Write-path benchmark

`BenchmarkInsertBlockDataBatch` times `InsertBlockDataBatch`, which is the whole
realtime write path: one Postgres transaction per block covering `blocks`,
`transactions`, `logs`, `token_transfers`, `internal_transactions`,
`address_stats` and `chain_counters`.

```bash
make bench-quick      # write path, 10k-row table, minutes
make bench            # everything, 10k/100k/1M tables, considerably longer
```

Each sub-benchmark starts its own Postgres via testcontainers, seeds it to the
target table size, and tears it down. Override the table sizes with a
comma-separated list:

```bash
BENCH_SCALES=10000,100000 go test ./internal/db -run '^$' -bench InsertBlockDataBatch -benchtime 30x
```

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

`RefreshTokenStats` is not on this path — it runs a level up, in the indexer —
so these numbers are insert cost alone. Derived-counter maintenance has to be
measured separately, and on a real chain it can dominate total database time by
a wide margin.

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
