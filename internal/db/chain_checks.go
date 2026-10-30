package db

import (
	"context"
	"math/big"

	"github.com/jackc/pgx/v5"
)

// BlocksCheck is blocks_total against the stored head: they agree when every block from the first counted one up
// to the head is counted. Applicable only with no missing range open and no stats backfill still counting;
// FromGenesis once a stats backfill has counted every block below START_BLOCK; Genesis when block 0 is counted
// (ingest never stores it: it starts after the stored head, 0 on an empty database): stored, counted by a stats
// backfill, or, once history is pruned, counted by span 0's ledger (its blocks_total is its width).
type BlocksCheck struct {
	Total, Head                      int64
	Applicable, FromGenesis, Genesis bool
}

func (d *DB) CheckBlocksTotal(ctx context.Context) (BlocksCheck, error) {
	var c BlocksCheck
	err := d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'blocks_total'),
		       COALESCE((SELECT max(number) FROM blocks), -1),
		       NOT EXISTS (SELECT 1 FROM missing_block_ranges) AND NOT EXISTS (SELECT 1 FROM stats_backfill WHERE completed_at IS NULL),
		       EXISTS (SELECT 1 FROM stats_backfill WHERE completed_at IS NOT NULL),
		       EXISTS (SELECT 1 FROM blocks WHERE number = 0) OR EXISTS (SELECT 1 FROM stats_backfill WHERE completed_at IS NOT NULL)
		       OR (pruned_through() > 0
		           AND EXISTS (SELECT 1 FROM ledger_spans s JOIN ledger_chain c ON c.lo = s.lo AND c.name = 'blocks_total'
		                       WHERE s.lo = 0 AND c.value = s.hi - s.lo))`).
		Scan(&c.Total, &c.Head, &c.Applicable, &c.FromGenesis, &c.Genesis)
	return c, err
}

// TokenSupply is a token's stored supply and the stored head it reflects, read in one statement.
type TokenSupply struct {
	Token  string
	Supply *big.Int
	Head   uint64
}

// NextTokenSupply is the first token with a supply after the given address, wrapping around; ok is false when
// there is none.
func (d *DB) NextTokenSupply(ctx context.Context, after string) (TokenSupply, bool, error) {
	var s TokenSupply
	var supply string
	err := d.pool.QueryRow(ctx, `
		SELECT address, total_supply::TEXT, (SELECT max(number) FROM blocks)
		FROM tokens WHERE total_supply IS NOT NULL
		ORDER BY address <= $1, address LIMIT 1`, after).Scan(&s.Token, &supply, &s.Head)
	if err == pgx.ErrNoRows {
		return s, false, nil
	}
	if err != nil {
		return s, false, err
	}
	s.Supply, _ = new(big.Int).SetString(supply, 10)
	return s, true, nil
}

type BalanceSample struct {
	Token, Address string
	Balance        *big.Int
	Block, Head    uint64
}

// SampleSettledBalances samples about n current balances with no transfer of their key after the block they were
// written for, so the node's balance at the stored head must equal them.
func (d *DB) SampleSettledBalances(ctx context.Context, n int) ([]BalanceSample, error) {
	rows, err := d.pool.Query(ctx, `
		WITH head AS (SELECT max(number) AS n FROM blocks),
		pct AS (SELECT LEAST(100, GREATEST(0.01, 400.0 * $1 / GREATEST(reltuples, 1)))::FLOAT4 AS p
		        FROM pg_class WHERE oid = 'token_balances_current'::regclass)
		SELECT c.token_address, c.address, c.balance::TEXT, c.block_number, head.n
		FROM token_balances_current c TABLESAMPLE BERNOULLI ((SELECT p FROM pct)), head
		WHERE c.block_number <= head.n
		  AND NOT EXISTS (SELECT 1 FROM token_transfers t
		                  WHERE t.token_address = c.token_address AND t.block_number > c.block_number
		                    AND (t.from_address = c.address OR t.to_address = c.address))
		LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (BalanceSample, error) {
		var s BalanceSample
		var balance string
		err := r.Scan(&s.Token, &s.Address, &balance, &s.Block, &s.Head)
		s.Balance, _ = new(big.Int).SetString(balance, 10)
		return s, err
	})
}

type SenderSample struct {
	Address string
	Sent    uint64
	Head    uint64
}

// senderProbeTxs bounds the transactions of a sender the nonce sample reads to find its types.
const senderProbeTxs = 10_000

const senderSampleSQL = `
	WITH head AS (SELECT max(number) AS n FROM blocks),
	ready AS (SELECT (SELECT value FROM ledger_verify_state WHERE name = 'running_cycles') > 0
	                 AND NOT EXISTS (SELECT 1 FROM missing_block_ranges) AS ok),
	pct AS (SELECT LEAST(100, GREATEST(0.01, 400.0 * $1 / GREATEST(reltuples, 1)))::FLOAT4 AS p
	        FROM pg_class WHERE oid = 'address_stats'::regclass)
	SELECT a.address, a.sent_count, head.n
	FROM address_stats a TABLESAMPLE BERNOULLI ((SELECT p FROM pct)), head, ready
	WHERE ready.ok AND a.sent_count > 0 AND NOT a.is_contract AND a.tx_count <= $2
	  AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.from_address = a.address AND t.tx_type NOT BETWEEN 0 AND 4)
	LIMIT $1`

// SampleSenders samples about n non-contract addresses that sent transactions, with the stored head their count
// reflects. None until a full running-counter cycle has recounted sent_count, or while a missing range is open.
//
// sent_count counts the types 0-4 only. On chains with system or deposit transaction types (OP Stack 126, Arbitrum
// 100-106) some of those also raise the sender's nonce, so only senders whose stored transactions are all of the
// counted types are sampled. Transactions no longer stored (pruned, or counted by the stats backfill) cannot be
// checked: on such a chain a sender with another type among them can show as drifted (the check only alerts). Each
// sampled sender's transactions are read through the from_address index, which does not carry the type, so a sender
// with more than senderProbeTxs transactions is not sampled: the probe stays bounded.
func (d *DB) SampleSenders(ctx context.Context, n int) ([]SenderSample, error) {
	rows, err := d.pool.Query(ctx, senderSampleSQL, n, senderProbeTxs)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (SenderSample, error) {
		var s SenderSample
		return s, r.Scan(&s.Address, &s.Sent, &s.Head)
	})
}

// NextTxCountSpan is the closed span whose transaction total was compared with the node longest ago.
func (d *DB) NextTxCountSpan(ctx context.Context) (lo, hi, txs int64, ok bool, err error) {
	err = d.pool.QueryRow(ctx, `
		SELECT s.lo, s.hi, c.value FROM ledger_spans s JOIN ledger_chain c ON c.lo = s.lo AND c.name = 'transactions_total'
		ORDER BY s.chain_checked_at NULLS FIRST, s.lo LIMIT 1`).Scan(&lo, &hi, &txs)
	if err == pgx.ErrNoRows {
		return 0, 0, 0, false, nil
	}
	return lo, hi, txs, err == nil, err
}

func (d *DB) RecordTxCountCheck(ctx context.Context, lo int64, agrees bool) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE ledger_spans SET chain_checked_at = now(), chain_state = CASE WHEN $2 THEN 'ok' ELSE 'drift' END WHERE lo = $1`,
		lo, agrees)
	return err
}

func (d *DB) countRunningCycle(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `UPDATE ledger_verify_state SET value = value + 1 WHERE name = 'running_cycles'`)
	return err
}
