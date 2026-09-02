# Benchmarking ingest

Four measurements, answering different questions. None substitutes for another.

| | question | needs |
|---|---|---|
| [Write-path benchmark](#write-path-benchmark) | did this commit make the inserts faster or slower? | docker, or a Postgres |
| [Derived-counter benchmarks](#derived-counter-benchmarks) | did it change a derived-counter refresh, or the real per-block cost? | same |
| [Read-path benchmarks](#read-path-benchmarks) | how slow are the explorer's queries as history grows? | same |
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

**Insert cost degrades gently with table size, not off a cliff.** Measured against a server
constrained to the deployed configuration (`shared_buffers = 2GB`, 3 CPU, gp3), growing
100k → 10M transactions — 100×, ending with a 12 GB database whose indexes total 3.1×
`shared_buffers` — cost the `erc20` insert path about 9%, and `plain` about 11%. (Both figures
were roughly double that until each shape stopped inheriting the rows its predecessors had
inserted; a good part of the apparent sensitivity to table size was really the within-run
growth described further down.) Ingest is **append-ordered** on the
primary key and on `block_number`: those only increase, so an insert lands on the right-hand
edge of the tree and the pages it touches stay hot however large the table is. At 10M the index
cache-hit rate is **100.00%** on every table, single-digit block reads apiece, with every miss
in the heap — even though the indexes total 3.1× `shared_buffers`. There is no index-residency
cliff on this path.

The obvious objection is that `transactions.hash` is the PRIMARY KEY and the synthetic hashes
ascend, whereas real hashes are uniformly distributed — so the ascending shapes would only
ever touch the right-hand edge of that index and flatter the result. That is what
`erc20-scattered-hash` exists to test: it is identical to `erc20` except that hashes are
scattered across the keyspace, at the same key length, so the delta is distribution alone.

**It is about 30% faster, not slower** (4,703 against 3,635 tx/s at 10M). Postgres compares `text`
btree keys through an abbreviated key — the leading bytes packed into an integer — and the
ascending hashes here share a long run of leading zeroes, so that fast path never discriminates
and each comparison degrades to a full byte-wise compare. Scattered keys resolve in one integer
comparison, and that saving outweighs the lost cache locality. So the ascending shapes are
**conservative** on this axis, and the no-cliff result holds under either distribution.

> Index size is dominated by **address** cardinality, not by hashes. Giving the seed a
> realistic address pool roughly doubled `idx_tx_from` and `idx_tx_to` — 1,035 MB each at 10M,
> against a `transactions_pkey` of 937 MB. With only 20 distinct addresses, btree
> deduplication had been compressing those two indexes to a fraction of their real size, which
> understated the total far more than hash length ever did.

Variance on a settled server is small: three consecutive trials of every shape landed inside
1–3.5% at both 100k and 10M. A much wider spread (~25%) appears only while checkpoints from the
bulk seed are still draining, which is exactly when the first trial after seeding runs — so
prefer `-count 3` and read the median, and distrust a single trial taken immediately after a
fresh seed.

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

Seeding is incremental and resumable: a database can be reused across runs and across
ascending scales, and re-running the same scale skips seeding entirely. The benchmark writes
to whatever you point it at and never drops anything — use a throwaway database, not a real
one.

Resumption keys off a counter in a `bench_seed_state` table that only `seed()` writes, not
off `COUNT(*) FROM transactions`. That matters more than it sounds. Every synthetic row
identity here — hash, block number, sender, timestamp — is a pure function of a row index, so
topping up requires knowing exactly how far the synthetic range extends; but the benchmarks
insert transactions too, at a different hash prefix and 250 rather than 125 per block, so a raw
`COUNT(*)` drifts away from the range the seed owns. When it did, the token seeder generated
`tx_hash` values inside the resulting gap and the foreign key rejected the whole COPY:

```
ERROR: insert or update on table "token_transfers" violates foreign key constraint
       "token_transfers_tx_hash_fkey"
```

The balance seeder restarted from holder 0 and collided with the `balances` primary key, and
block numbering grew silent holes. All three were the same wrong assumption, and all three
are gone. If you see any of them again, suspect something reading a table count where it
should be reading `bench_seed_state`.

Two smaller invariants the resume path depends on, both easy to break again:

- **A scale need not be a multiple of 125.** The row a resume starts from usually sits
  mid-block, and the block holding it already exists, so the block resume point rounds *up*.
  Rounding down re-copies that block — going 10001 → 20000 it retried block 81 and the primary
  key rejected it. The transactions COPY resumes at the row and fills the partial block in.
- **Timestamps come from a fixed origin stored at the first seed**, as `origin + 2×(number−1)`,
  so they stay monotonic in block number across top-ups. Computing them from the current target
  as `now − 2×(nBlocks − i)` — the obvious version — re-anchors the whole timeline every time
  while writing only the new rows, so after 100k → 1M block 801 landed **14,398 seconds earlier
  than block 800**. The residual is that a top-up extends forward from the stored origin, so
  the newest block can drift past wall-clock; if `GetTransactionHistory_24h` matters to you,
  seed the target scale directly rather than climbing to it.

Seeding generates rows one at a time rather than materialising them, so the client side costs
tens of MB regardless of scale; the memory that matters is the server's.

**Each shape and each `-count` trial is measured against the seeded size.** The harness deletes
the rows a run inserted before the next one starts, because otherwise a run measures a
progressively larger table: at 100k, one pass adds 46% and five shapes at `-count 3` add 116%,
and at `BENCH_SCALES=10000` with `-count 6` the table grows **24× during the comparison** —
systematically favouring whichever shape ran first. Deleting the benchmark's blocks is enough,
since transactions and through them logs, token_transfers and internal_transactions all
cascade.

One consequence worth knowing: the seed and the benchmarks share the `blocks.number` keyspace.
The benchmark appends at `MAX(number)+1`, which is a number a later, larger seed will want, so
a run interrupted before it can clean up will make the next top-up fail with a duplicate-key
error on `blocks`. That is deliberate — a loud failure beats a table that silently mixes seeded
and benchmark rows. Drop the database and reseed.

**What that restore does not do is return the database to its physical state.** `DELETE`
recovers the row counts, and autovacuum recovers the heap, but B-tree pages freed by a delete
are not handed back — they are only reusable for keys in the same range. Measured at 100k, a
full five-shape run at `-count 3` left `token_transfers`' indexes **70% larger** than a pristine
seed and `transactions`' **23% larger**, at identical live row counts. So later shapes in a run
are measured against physically larger indexes than earlier ones.

How much that matters depends entirely on scale, because it is driven by churn as a fraction of
the table:

- at 10M, a run inserts and deletes 7,750 rows against 10,000,000 — **0.078%**, structurally
  negligible;
- at 100k it is material in principle, but measured under 1%: `erc20` alone against a pristine
  database gave a median of 4,037 tx/s against 4,013 in the full run.

So the numbers in this document are sound, and `TestBenchmarkHarnessMeasuresRealWork` now
bounds the bloat rather than ignoring it. But if you need publication-grade cross-shape
comparison — especially at a small scale, where the churn fraction is largest — **use a fresh
database per measured result** rather than trusting the restore:

```bash
for shape in plain erc20 erc20-no-address-stats erc20-scattered-hash erc20-internal-calls; do
  dropdb --if-exists "bench_$shape"; createdb "bench_$shape"
  BENCH_DATABASE_URL="postgres://…/bench_$shape?sslmode=disable" BENCH_SCALES=100000 \
    go test ./internal/db -run '^$' -bench "InsertBlockDataBatch/$shape\$" -benchtime 30x -count 3
done
```

That is the only way to get byte-identical starting conditions. It costs a full reseed per
shape, which is why it is not the default.

`-benchtime` must be pinned to an iteration count (`30x`), and the harness now fails with
that instruction rather than letting a duration ramp `b.N` and re-seed the table at every
step. At the 10M default that ramp looks like a hang.

### Reading the output

Every run reports `blocks/s`, `tx/s` and `gas/s` alongside `ns/op`, where one op
is one block.

**Never quote `tx/s` on its own.** Ingest cost tracks block count and gas as
well as transaction count, so a bare `tx/s` figure is not comparable against
another run unless the shape matches. The benchmark defines five shapes for
exactly this reason:

| shape | per tx | what it exercises |
|---|---|---|
| `plain` | value transfer only | the `blocks` + `transactions` + `address_stats` floor |
| `erc20` | 1 log + 1 token transfer | the realistic path, including `logs` and `token_transfers` |
| `erc20-no-address-stats` | as above, `SkipAddressStats` | isolates the cost of `address_stats` maintenance |
| `erc20-scattered-hash` | as `erc20`, hashes spread across the keyspace | isolates the cost of key *distribution* in the primary key |
| `erc20-internal-calls` | as `erc20`, plus 2 traced internal calls | the `internal_transactions` branch, which no other shape reaches |

Senders come from a pool the seed already wrote, so `address_stats` takes its
`ON CONFLICT DO UPDATE` branch — the steady-state hot path. Every tenth
recipient is new, so the `INSERT` branch and the `addresses_total` counter
increment are covered too.

**Address cardinality is a scale axis, not a constant.** The pool grows with the seeded scale
(one address per ten transactions), so a 250-transaction block presents around 500 distinct
address deltas and `address_stats` grows alongside `transactions`. It used to hold 20 rows at
every scale — byte-for-byte identical at 100k and at 10M — which meant the UPSERT always hit a
two-level index that could not miss cache. That flattered every shape and made
`erc20-no-address-stats` nearly free; correcting it moved that shape's advantage from about 3%
to nearly 30%. If you shrink the pool, you are no longer measuring `address_stats`.

Transaction hashes are 66 characters, as production, in every shape. `seed()` always wrote
production-length hashes; the benchmark's own rows did not, so their index keys were 17 bytes
short of everything around them. Note that this only ever affected the rows a run inserts —
a fraction of a percent of the table — so it moved index *size*, and therefore the point where
the working set stops fitting, hardly at all.

### What this costs, measured

Same constrained server (PG 17, `shared_buffers = 2GB`, 3 CPU, gp3), 250 tx/block, median of
three trials on a settled server:

| shape | 100k | 1M | 10M |
|---|---|---|---|
| `plain` | 8,096 | 7,598 | 7,188 |
| `erc20` | 4,013 | 3,790 | 3,635 |
| `erc20-no-address-stats` | 4,749 | 4,456 | 4,353 |
| `erc20-scattered-hash` | 5,286 | 4,972 | 4,703 |
| `erc20-internal-calls` | 2,585 | 2,487 | 2,589 |

tx/s. Read them next to their shape and never on their own.

Three things worth taking from that table:

- **`SkipAddressStats` buys about 20%** at every scale, so catchup mode's workaround for the
  `address_stats` deadlock is a real throughput win and not only a deadlock fix.
- **Two traced internal calls per transaction cost about 30%** — the first measurement of that
  branch at any scale.
- **Scattered hashes are about 30% faster than ascending ones**, for the abbreviated-key reason
  described above.

### What it deliberately excludes

The derived-counter refreshes are not on this path — they run a level up, in the indexer — so
these numbers are insert cost alone. See the next section.

## Derived-counter benchmarks

`BenchmarkBlockCycle` times what the indexer actually does per block: the insert batch
followed by a refresh for each token the block touched.
`BenchmarkRefreshTokenTransferStats` and `BenchmarkRefreshTokenHolderCount` time the two
refreshes individually.

These matter because the two halves scale on **different axes**:

- inserts scale with rows written per block, and barely with stored history;
- the refreshes scale with the history of the token being refreshed, and not at all
  with what the current block wrote.

Each refresh issues history-proportional statements against a single token, none of them
bounded by the block being processed, so cost grows with the token's lifetime. A chain with
one dominant token is the worst case, and is what these benchmarks seed.

**The refresh is split across two paths, and only one of them is per-block.**
`RefreshTokenTransferStats` maintains `transfer_count` and `total_supply` and runs once per
token per block, from the ingest loop — a `COUNT(*)` over `token_transfers` and, for ERC20, a
mint-minus-burn `SUM(...)` over the same table inside the `UPDATE`. `RefreshTokenHolderCount`
maintains `holder_count` with a `DISTINCT ON (address)` over `balances`, and runs once per
token per **balance flush**, from the balance worker pool.

That split is why there are two benchmarks rather than one, and **measuring only
`BenchmarkBlockCycle` will overstate a change that merely moves cost between the two.**
`holder_count` cannot be maintained on the per-block path at all: balances are fetched over
RPC by the async workers and queued only after the block commits, so a refresh running for
block N cannot observe block N's balances. Its cost still reaches ingest, just indirectly —
the balance workers call it inline, so a slow one backs the balance queue up until
`QueueWorkBatch` starts dropping work.

### What this costs, measured

Same constrained server as above (PG 17, `shared_buffers = 2GB`, 3 CPU, gp3), 250 tx/block,
one dominant ERC-20:

These are the figures for the **combined** refresh, as it stood before it was split — one
call doing all three counters, on both paths:

| scale | combined refresh | `BlockCycle` | `BlockCycle` tx/s | blocks/s | gas/s |
|---|---|---|---|---|---|
| 100k | 28 ms | 94 ms | 2,663 | 10.6 | 173 M |
| 1M | 281 ms | 341 ms | 734 | 2.94 | 47.7 M |
| 10M | 2.86 s | 2.64 s | **95** | 0.378 | 6.1 M |

The refresh benchmarks run immediately after the bulk seed, while autovacuum is still
working through the freshly loaded table, so they read slightly high. Timing the three
statements individually on a settled server gives 2.44 s at 10M — 440 ms for the `COUNT(*)`,
863 ms for the `DISTINCT ON` over `balances`, 1,140 ms for the `total_supply` `SUM`. Quote
~2.5 s.

Note how that 2.44 s divides across the split: the `COUNT(*)` and the `SUM` (1,580 ms) went to
`RefreshTokenTransferStats`, on the per-block path; the `DISTINCT ON` (863 ms) went to
`RefreshTokenHolderCount`, on the flush path.

Two things follow, and both are structural rather than tuning problems.

**It is linear in history.** Ten times the history costs ten times as much (10.2× then 10.2×
across these three scales), per block, forever. The per-block cycle therefore collapsed **28×**
between 100k and 10M, and at 10M a single refresh costs around thirty-five times the insert it
accompanies.

**It is CPU-bound, not I/O-bound.** At 10M only the `SUM` leaves `shared_buffers` at all, and
its disk I/O is ~176 ms of a 1,107 ms statement — the other 84%, and effectively all of the
other two statements, is CPU spent walking and aggregating rows that are already in memory.
Raising `shared_buffers` or moving to faster disks will not fix this; not recomputing
whole-history aggregates per block is the only thing that will.

That is also why `BenchmarkInsertBlockDataBatch` cannot be the regression gate for ingest:
at 10M it accounts for about 2.5% of the per-block cost that `BenchmarkBlockCycle` measures.

`BenchmarkBlockCycle` writes its transfers against a different token from the one it
refreshes, so a run cannot extend the history it is timing. That is not cosmetic: refresh cost
tracks the history of the token being refreshed, and when the block extended that same token
each iteration measured a slightly larger table than the last. At the 10M default the 250 rows
an iteration adds are noise, but at the small scale recommended just below for before/after
work they nearly double the history mid-run — non-stationary in exactly the configuration
proposed for comparisons.

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

## Read-path benchmarks

The six `BenchmarkGet*` benchmarks time query latency against the same seeded table. At 10M,
on the constrained server, five of the six are microseconds and one is not:

| benchmark | at 10M |
|---|---|
| `GetChainStats` | 100 µs |
| `GetTransaction_byHash` | 109 µs |
| `GetContract` | 69 µs |
| `GetAddressStats` | 64 µs |
| `GetTransactionsWithCategories_latest10` | 258 µs |
| **`GetTransactionHistory_24h`** | **852 ms** |

`GetTransactionHistory` is the outlier and it is not a caching or indexing accident: to return
24 hourly points it joins every transaction in the window — **4.9M rows** on a chain producing
a block every two seconds at ~125 tx/block — and aggregates them, on every call. It is fully
resident in cache; the time is spent walking the join. A rollup would fix it, and the schema
already carries an unused `daily_stats` table.

**Two traps that made these benchmarks measure nothing, both now fixed.** Both are the same
mistake: a lookup key that does not match what `seed()` writes, which fails silently because
"row not found" is a perfectly good benchmark result.

- `GetAddressStats` searched for an address one character short of the seeded one, so it timed
  an index probe that missed.
- `setupBenchDB` built `&DB{pool: pool}` instead of going through a constructor, leaving
  `HiddenTxTypes` nil where `New()` sets an empty slice. pgx sends nil as SQL NULL, and
  `NOT (tx_type = ANY(NULL))` is NULL for every row — so the listing query matched nothing and
  the planner chose a parallel sequential scan over all 10M rows. That benchmark read
  **1.17 s**; with the parameter corrected it is **258 µs**, a factor of 4,500.

When adding a read benchmark, assert it returns a row before trusting its timing.

## Keeping the harness honest

`TestBenchmarkHarnessMeasuresRealWork` exists because a benchmark has no oracle. A test
asserts a value and fails; a benchmark prints a number, and a number produced by doing nothing
is indistinguishable from a number produced by doing the work. Every benchmark defect found so
far was of that kind — a nil parameter that matched no row, a lookup key one character short
of the seeded one, a table no generated batch ever wrote — and every one of them *passed*.

So the test runs at a small scale and asserts the harness actually did something:

- each shape changes the row count of every table it claims to exercise, and of none it does not;
- a block presents a realistic number of distinct address deltas;
- transaction hashes are production length and unique, so no insert is silently an
  `ON CONFLICT DO NOTHING` no-op;
- both refreshes have history to walk and write a non-zero result back;
- each read benchmark returns rows — `GetAddressStats` in particular returns a zero-valued
  struct with a nil error when the row is missing, so only a populated field distinguishes a
  hit from a miss;
- `BenchmarkBlockCycle` does not write to the token it refreshes.

It is cheap and it runs in CI. Add a case to it whenever you add a benchmark: the failure mode
being guarded against is not a wrong number, it is a plausible number that means nothing.

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
