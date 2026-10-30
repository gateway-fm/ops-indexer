# Totals ledger, verification and repair

Every total the read API serves from a counter (chain totals, token transfer counts and supply, per-address counts,
see [API.md](API.md#history-retention)) is backed by a ledger. Each check below exports metrics, so an alert can fire
on any drift. The ledger and the checks run with retention on or off.

## The ledger

- A **span** is a block interval `[lo, hi)`. Span bounds are the `LEDGER_SPAN_BLOCKS` grid, `START_BLOCK`, and every
  history partition bound, so a pruned partition is always whole spans.
- When a span is final (more than 12,800 blocks below the head) and complete (every block stored, no missing range),
  the closer records what it contributed. One row per span holds the chain counters (`ledger_chain`); one row per
  span and token holds the transfer count and ERC-20 supply (`ledger_token`); one row per span and address holds the
  transactions, sent transactions, internal calls, token transfers, and first and last block (`ledger_address`).
- Spans close in block order, so the closed spans are a prefix of the chain.
- A range is pruned only once all of its spans are closed. Pruned history is then the closed spans at or below the
  watermark; `prune_offsets` is their sum.
- The hot path keeps adding to the running counters. They must equal the closed ledger rows plus the open spans'
  history rows.
- **address_stats is derived from the ledger**: when a span closes, every address
  with a row in it, and every address of a block reverted since the last close, is set to that count. One statement
  counts the truth of all of them at once (one join with the closed ledger rows, one pass over the rows above the
  closed spans) and stages the rows that differ; the corrections are then added in short batches of 1,000, so no row
  is held while the truth is counted. An error in an address's running counters is gone one span after it happened.
  A restart resumes at the first span not rebased (`ledger_spans.rebased_at`); the log line `ledger: span's
  addresses rebased` gives the rows staged, changed and created.

## Checks

| check | what it compares | repairs |
|---|---|---|
| `recount` | One closed retained span per tick: its ledger rows against a recount of its history rows. | Replaces the span's ledger rows. |
| `running` | The running chain and token counters, and the address counters of one of 256 address buckets per tick, against the closed ledger rows plus the open rows, in one snapshot. | After the snapshot: moves a chain or token counter by its drift; sets a drifted address to the ledger's count, as the span rebase does. |
| `node` | One pruned span at a time, oldest-checked first: its ledger rows against the span recounted from the node with the ingest block builder. | Replaces the span's ledger rows; the `running` check then repairs any running counter they imply. |
| `chain_blocks` | `blocks_total` against the stored head + 1 − `START_BLOCK`, while no missing range is open. | Alert only. |
| `chain_supply` | One token per pass: `total_supply` against `totalSupply()` at the stored head. A token minted or burned without Transfer events differs legitimately. | Alert only. |
| `chain_balance` | A sample of current balances with no later transfer, against `balanceOf()` at the stored head; a key counts as drifted when it differs on two passes. This also covers `holder_count`. | Alert only. |
| `chain_nonce` | A sample of non-contract senders: `sent_count` against `eth_getTransactionCount` at the stored head. It starts once a full `running` cycle has recounted `sent_count`. | Alert only. |
| `chain_span_txs` | One closed span per pass, retained or pruned: its ledger transaction total against the sum of `eth_getBlockTransactionCountByNumber`. | Alert only (`node` repairs the span). |

- The `running` check, and the one-off address reconciliation below, wait until the open tail (stored blocks above
  the closed spans) is at most 12,800 + 3 × `LEDGER_SPAN_BLOCKS`: before the closer has caught up they would read
  most of history every pass. A span closes only once it is 12,800 blocks (the prune floor gap) below the head, so
  the tail never falls below that.
- **Address reconciliation** (once per database, after migration 031): with repair on, the drifted addresses of every
  bucket are set to the closed ledger rows plus the open tail, as the span rebase sets them, and the bucket is then
  recorded done, so a restart resumes at the next bucket. It repairs address_stats left short by a catch-up that did
  not write it (retention off, before catch-up wrote address_stats in every mode), without a rebuild.
- Every repair is logged per key in `ledger_repairs`.
- A span, or a running key, that drifts again after its repair is **quarantined**: it is no longer repaired, and
  `indexer_verify_quarantined` alerts.
- Repairs run in READ COMMITTED and never retry for a conflict with ingest: under REPEATABLE READ a row ingest wrote
  after the snapshot failed the repair, and a busy address always had one. A repair killed mid-way leaves each batch
  either done or not, and the next pass changes nothing a done batch set.
- The span rebase is not a repair: it is not logged in `ledger_repairs` and never quarantines.

## Checking address_stats against the ledger by hand

After `repair-address-stats`, or at any time, these confirm that every address_stats row equals its truth: the
closed ledger rows, the open stats backfill span's staged rows and the rows above the closed spans
(`ledger_address_truth`). Ingest keeps writing meanwhile, so run them on a quiet database, or expect the addresses of
the newest blocks to differ until the next block's write.

Per bucket, cheap (the same diff the `running` check reads; each bucket reads only its own addresses). No rows means
no drift. The zero address is in bucket 0 (addresses `0x00…` to `0x01…`), compared like any other since migration 036.

```sql
SELECT b AS bucket, count(DISTINCT key) AS drifted_addresses
FROM generate_series(0, 255) AS b, ledger_running_diff_address(b)
GROUP BY b ORDER BY b;
```

Every address at once, the zero address included. No rows means no drift. It reads every history table in full, so
expect it to take as long as a scan of transactions, token_transfers and internal_transactions.

```sql
WITH c AS (
    SELECT array_agg(DISTINCT address) AS all FROM (
        SELECT address FROM address_stats UNION ALL SELECT address FROM ledger_address
        UNION ALL SELECT from_address FROM transactions UNION ALL SELECT to_address FROM transactions
        UNION ALL SELECT from_address FROM token_transfers UNION ALL SELECT to_address FROM token_transfers
        UNION ALL SELECT from_address FROM internal_transactions UNION ALL SELECT to_address FROM internal_transactions
        UNION ALL SELECT address FROM contracts
    ) u WHERE address IS NOT NULL
), t AS (SELECT x.* FROM c, ledger_address_truth(c.all) x)
SELECT COALESCE(s.address, t.address) AS address,
       s.tx_count, t.txs, s.sent_count, t.sent, s.internal_tx_count, t.internal,
       s.token_transfer_count, t.transfers, s.first_seen, t.first_seen, s.last_seen, t.last_seen
FROM address_stats s FULL JOIN t ON t.address = s.address
WHERE s.address IS NULL OR t.address IS NULL
   OR (s.tx_count, s.sent_count, s.internal_tx_count, s.token_transfer_count, s.first_seen, s.last_seen)
      IS DISTINCT FROM (t.txs, t.sent, t.internal, t.transfers, t.first_seen, t.last_seen)
ORDER BY 1;
```

The zero address alone, a quick check. It has a row like any address: transactions sent to it and internal calls to
and from it count, token transfers from or to it do not, in ingest and in the ledger alike. `true` means equal.

```sql
SELECT (SELECT row(tx_count, sent_count, internal_tx_count, token_transfer_count, first_seen, last_seen)
        FROM address_stats WHERE address = '0x0000000000000000000000000000000000000000')
       IS NOT DISTINCT FROM
       (SELECT row(txs::BIGINT, sent::BIGINT, internal::BIGINT, transfers::BIGINT, first_seen, last_seen)
        FROM ledger_address_truth(ARRAY['0x0000000000000000000000000000000000000000'])) AS zero_address_ok;
```

## Knobs (one budget per job)

| variable | default | bounds |
|---|---|---|
| `LEDGER_SPAN_BLOCKS` | 100000 | Span size (grid). |
| `LEDGER_CLOSE_INTERVAL` | 10s | One span closed per interval. |
| `LEDGER_WORK_MEM` | 512MB | work_mem of the transaction that closes or recounts a span, so its per-address grouping stays in memory. One backend, no parallel workers: at most about 2.5 GB (two hash nodes at 2 × work_mem, one sort), far less in practice. |
| `LEDGER_MAX_HEAD_LAG_BLOCKS` | 0 (off) | The closer skips its interval while ingest is more than this many blocks behind the node. A span already being closed finishes. |
| `LEDGER_COMPACT_SPANS` | 0 (off) | When 2 or more: per interval, that many consecutive pruned spans the `node` check verified are merged into one. Totals do not move; per-address rows shrink where an address is active in several of them; the merged span is recounted from the node over its larger range. |
| `VERIFY_RECOUNT_INTERVAL` | 1m | One retained span and one address bucket per interval (`recount`, `running`). |
| `VERIFY_REPAIR` | true | Whether `recount`, `running` and `node` repair. |
| `VERIFY_NODE_RECOUNT_RPS` | 50 | Node calls per second of the `node` recount; 0 turns it off. A span costs about two calls per block, plus traces. |
| `VERIFY_CHAIN_RPS` | 20 | Node calls per second of the chain checks; 0 turns them off. |
| `VERIFY_CHAIN_SAMPLE` | 20 | Balances and senders sampled per pass. |
| `VERIFY_CHAIN_INTERVAL` | 1m | One chain-check pass per interval. |

With `START_BLOCK` > 0 and retention on, the stats backfill (docs/API.md "Stats backfill") counts `[0, START_BLOCK)`
from the node into the counters and into ledger spans of source `node`, which the `node` check recounts like any
pruned span. It then reads each touched balance once, at the node's latest block. A read the EVM fails (a revert, an
invalid opcode, out of gas) drops the key (not a token); a read that fails otherwise (rate limit, timeout,
transport) keeps it, to be retried after every other key, ten seconds later at the soonest. A node that keeps
failing holds `stats_incomplete` true and logs `stats backfill: batch failed; retrying`. Retention prunes nothing until
the backfill has written its days, which it counts from the blocks stored from `START_BLOCK`: history grows past
`PRUNE_RETAIN_BLOCKS` for as long as the backfill runs. Once every block below `START_BLOCK` is counted, the backfill
waits until daily_stats has started (on a day after `START_BLOCK`'s) and catch-up has stored every block from
`START_BLOCK` to daily_stats' first, and logs that once.

| variable | default | bounds |
|---|---|---|
| `STATS_BACKFILL_RPS` | 100 | Node calls per second of the backfill: about two per block (block, receipts), plus traces with internal transactions indexed, `eth_getCode` per contract creation, token metadata per new token, `tokenURI` per NFT mint. The head path's `RPC_RATE_LIMIT` is separate. |
| `STATS_BACKFILL_WORKERS` | 4 | Parallelism of the block fetches and of the balance reads, under that budget. |
| `STATS_BACKFILL_BATCH_BLOCKS` | 1000 | Blocks per transaction; a restart resumes after the last committed batch. |

## Metrics

| metric | meaning |
|---|---|
| `indexer_ledger_closed_through` | Every block at or below this is in a closed span. |
| `indexer_ledger_close_seconds` | Time to close one span from its rows. |
| `indexer_stats_backfill_through` | Every block at or below this is counted by the stats backfill. |
| `indexer_stats_backfill_waiting_for_catchup` | 1 while the backfill has counted every block below `START_BLOCK` and waits for catch-up to store every block from it. |
| `indexer_prune_blocked_by_stats_backfill` | 1 while retention prunes nothing because the backfill has not written its days; history grows meanwhile. |
| `indexer_verify_ranges_checked_total{check}` | Spans, buckets or samples checked. |
| `indexer_verify_ranges_drifted_total{check}` | Checks that found a difference. |
| `indexer_verify_drift{check}` | Differences found by the last check. |
| `indexer_verify_repairs_total{check}` | Checks whose drift was repaired. |
| `indexer_verify_quarantined` | Spans and running keys that drifted again after a repair. |
| `indexer_verify_cycle_seconds{check}`, `indexer_verify_last_full_cycle_timestamp{check}` | The last full cycle over every span or bucket. |

Suggested alert: `increase(indexer_verify_ranges_drifted_total[1h]) > 0 or indexer_verify_quarantined > 0`.
