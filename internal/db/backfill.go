package db

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// ErrBackfillNotNext refuses a batch that does not start right after the recorded progress: applying it would
// count its blocks twice.
var ErrBackfillNotNext = errors.New("stats backfill: batch does not continue the recorded progress")

type BackfillState struct {
	Exists, Complete bool
	Target, Through  int64
}

func (d *DB) StatsBackfillState(ctx context.Context) (BackfillState, error) {
	var s BackfillState
	err := d.pool.QueryRow(ctx, `SELECT target, through, completed_at IS NOT NULL FROM stats_backfill WHERE id = 1`).
		Scan(&s.Target, &s.Through, &s.Complete)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, nil
	}
	s.Exists = err == nil
	return s, err
}

// InitStatsBackfill makes [0, target] pruned from day one, to be counted by the backfill, on a database that holds
// no block at or below target. targetTimestamp is block target's timestamp.
func (d *DB) InitStatsBackfill(ctx context.Context, target, targetTimestamp uint64) error {
	return pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var below bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM blocks WHERE number <= $1)`, int64(target)).Scan(&below); err != nil {
			return err
		}
		if below {
			return fmt.Errorf("stats backfill: blocks at or below %d are already stored; the backfill only counts blocks a fresh deployment never stored", target)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO stats_backfill (id, target, through) VALUES (1, $1, -1)`, int64(target)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE prune_state SET pruned_through = $1, pruned_through_ts = $2, updated_at = now() WHERE id = 1`,
			int64(target), int64(targetTimestamp))
		return err
	})
}

// BackfillBatch is what blocks [From, To) of the backfill add: the shared rows (tokens, nft_tokens, address_stats
// merged over the batch), the contracts created, and each span's totals.
type BackfillBatch struct {
	From, To    uint64
	Shared      *BlockData
	Contracts   []*types.Contract
	Spans       map[uint64]*SpanTotals
	SpanHi      map[uint64]uint64
	BalanceKeys []BalanceKey
	// TokenFirstSeen is each token's first transfer in the batch: a token row created by a later block takes it.
	TokenFirstSeen map[string]TokenSeen
}

type TokenSeen struct {
	Block uint64
	Tx    string
}

type BalanceKey struct {
	Token, Address string
	Block          uint64
}

// CommitBackfillBatch applies a batch in one transaction: counters and state, the batch's share of each span in
// the stage tables, the spans it completes moved into the ledger, its balance keys, and the progress.
func (d *DB) CommitBackfillBatch(ctx context.Context, b *BackfillBatch) error {
	return pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var through, target int64
		if err := tx.QueryRow(ctx, `SELECT through, target FROM stats_backfill WHERE id = 1 FOR UPDATE`).Scan(&through, &target); err != nil {
			return err
		}
		if through+1 != int64(b.From) || int64(b.To)-1 > target {
			return fmt.Errorf("%w: through %d, batch [%d, %d)", ErrBackfillNotNext, through, b.From, b.To)
		}

		var blocks, txs, transfers int64
		deltas := map[string]*tokenStatsDelta{}
		for _, s := range b.Spans {
			blocks, txs, transfers = blocks+s.Blocks, txs+s.Transactions, transfers+s.Transfers
			for token, t := range s.Tokens {
				d := deltas[token]
				if d == nil {
					d = &tokenStatsDelta{supplyDelta: new(big.Int)}
					deltas[token] = d
				}
				d.transferDelta += t.Transfers
				d.supplyDelta.Add(d.supplyDelta, t.Supply)
			}
		}
		if err := writeSharedRows(ctx, tx, sharedRows{data: b.Shared, blocks: blocks, txs: txs, transfers: transfers,
			tokenDeltas: deltas}); err != nil {
			return err
		}
		if err := insertContracts(ctx, tx, b.Contracts); err != nil {
			return err
		}
		if err := lowerTokenCreation(ctx, tx, b.TokenFirstSeen); err != nil {
			return err
		}

		los := make([]uint64, 0, len(b.Spans))
		for lo := range b.Spans {
			los = append(los, lo)
		}
		sort.Slice(los, func(i, j int) bool { return los[i] < los[j] })
		var openSpan *int64
		for _, lo := range los {
			if err := addToStage(ctx, tx, int64(lo), b.Spans[lo]); err != nil {
				return err
			}
			if hi := b.SpanHi[lo]; hi <= b.To {
				if _, err := tx.Exec(ctx, `INSERT INTO ledger_spans (lo, hi, source) VALUES ($1, $2, 'node')`, int64(lo), int64(hi)); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `SELECT ledger_stage_commit($1)`, int64(lo)); err != nil {
					return err
				}
			} else {
				open := int64(lo)
				openSpan = &open
			}
		}

		if len(b.BalanceKeys) > 0 {
			rows := make([][]any, len(b.BalanceKeys))
			for i, k := range b.BalanceKeys {
				rows[i] = []any{k.Token, k.Address, int64(k.Block)}
			}
			if _, err := tx.Exec(ctx, `CREATE TEMP TABLE backfill_keys (token_address TEXT, address TEXT, block_number BIGINT) ON COMMIT DROP`); err != nil {
				return err
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"backfill_keys"}, []string{"token_address", "address", "block_number"}, pgx.CopyFromRows(rows)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO stats_backfill_balance_keys (token_address, address, block_number)
				SELECT token_address, address, max(block_number) FROM backfill_keys GROUP BY 1, 2
				ON CONFLICT (token_address, address) DO UPDATE
					SET block_number = GREATEST(stats_backfill_balance_keys.block_number, EXCLUDED.block_number)`); err != nil {
				return err
			}
		}

		_, err := tx.Exec(ctx, `
			UPDATE stats_backfill SET through = $1, span_lo = $2,
			       completed_at = CASE WHEN $1 = target THEN now() END
			WHERE id = 1`, int64(b.To)-1, openSpan)
		return err
	})
}

func lowerTokenCreation(ctx context.Context, tx pgx.Tx, seen map[string]TokenSeen) error {
	if len(seen) == 0 {
		return nil
	}
	tokens := make([]string, 0, len(seen))
	for token := range seen {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	batch := &pgx.Batch{}
	for _, token := range tokens {
		batch.Queue(`UPDATE tokens SET block_number = $2, creation_tx = $3 WHERE address = $1 AND block_number > $2`,
			token, int64(seen[token].Block), seen[token].Tx)
	}
	return tx.SendBatch(ctx, batch).Close()
}

func insertContracts(ctx context.Context, tx pgx.Tx, contracts []*types.Contract) error {
	if len(contracts) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, c := range contracts {
		batch.Queue(`
			INSERT INTO contracts (address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified)
			VALUES (LOWER($1), $2, $3, $4, $5, $6, $7)
			ON CONFLICT (address) DO NOTHING`,
			c.Address, c.Bytecode, c.BytecodeHash, c.Creator, c.CreationTx, c.BlockNumber, c.IsVerified)
	}
	return tx.SendBatch(ctx, batch).Close()
}

// addToStage adds a span's totals to what the stage tables already hold for it.
func addToStage(ctx context.Context, tx pgx.Tx, lo int64, s *SpanTotals) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_stage_chain (lo, name, value)
		VALUES ($1, 'blocks_total', $2), ($1, 'transactions_total', $3), ($1, 'transfers_total', $4)
		ON CONFLICT (lo, name) DO UPDATE SET value = ledger_stage_chain.value + EXCLUDED.value`,
		lo, s.Blocks, s.Transactions, s.Transfers); err != nil {
		return err
	}
	if len(s.Tokens) > 0 {
		batch := &pgx.Batch{}
		for token, t := range s.Tokens {
			batch.Queue(`
				INSERT INTO ledger_stage_token (lo, token, transfers, supply) VALUES ($1, $2, $3, $4::NUMERIC)
				ON CONFLICT (lo, token) DO UPDATE SET transfers = ledger_stage_token.transfers + EXCLUDED.transfers,
				                                      supply = ledger_stage_token.supply + EXCLUDED.supply`,
				lo, token, t.Transfers, t.Supply.String())
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
	}
	if len(s.Addresses) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(s.Addresses))
	for addr, a := range s.Addresses {
		rows = append(rows, []any{lo, addr, a.Txs, a.Sent, a.Internal, a.Transfers, int64(a.FirstSeen), int64(a.LastSeen)})
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE IF NOT EXISTS backfill_addresses (LIKE ledger_stage_address) ON COMMIT DELETE ROWS`); err != nil {
		return err
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"backfill_addresses"},
		[]string{"lo", "address", "txs", "sent", "internal", "transfers", "first_seen", "last_seen"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_stage_address SELECT * FROM backfill_addresses WHERE lo = $1
		ON CONFLICT (lo, address) DO UPDATE SET
			txs = ledger_stage_address.txs + EXCLUDED.txs,
			sent = ledger_stage_address.sent + EXCLUDED.sent,
			internal = ledger_stage_address.internal + EXCLUDED.internal,
			transfers = ledger_stage_address.transfers + EXCLUDED.transfers,
			first_seen = LEAST(ledger_stage_address.first_seen, EXCLUDED.first_seen),
			last_seen = GREATEST(ledger_stage_address.last_seen, EXCLUDED.last_seen)`, lo)
	return err
}
