package db

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// SpanTotals is what a span of blocks contributes to the ledger, summed from the writer's own BlockData: the stats
// backfill and the recount of a pruned span build it from node data, so they count exactly what ingest counts.
type SpanTotals struct {
	Blocks, Transactions, Transfers int64
	Tokens                          map[string]*TokenTotals
	Addresses                       map[string]*AddressTotals
}

type TokenTotals struct {
	Transfers int64
	Supply    *big.Int
}

type AddressTotals struct {
	Txs, Sent, Internal, Transfers int64
	FirstSeen, LastSeen            uint64
}

func NewSpanTotals() *SpanTotals {
	return &SpanTotals{Tokens: map[string]*TokenTotals{}, Addresses: map[string]*AddressTotals{}}
}

func (s *SpanTotals) AddBlock(b *BlockData) error {
	if b.Block != nil {
		s.Blocks++
	}
	s.Transactions += int64(len(b.Transactions))
	s.Transfers += int64(len(b.Transfers))
	for _, t := range b.Transfers {
		token := strings.ToLower(t.TokenAddress)
		tt := s.Tokens[token]
		if tt == nil {
			tt = &TokenTotals{Supply: new(big.Int)}
			s.Tokens[token] = tt
		}
		tt.Transfers++
		if t.TokenType != types.TokenTypeERC20 {
			continue
		}
		v, ok := new(big.Int).SetString(string(t.Value), 10)
		if !ok {
			return fmt.Errorf("transfer %s/%d: invalid value %q", t.TxHash, t.LogIndex, string(t.Value))
		}
		if strings.ToLower(t.From) == zeroAddress {
			tt.Supply.Add(tt.Supply, v)
		}
		if strings.ToLower(t.To) == zeroAddress {
			tt.Supply.Sub(tt.Supply, v)
		}
	}
	for addr, delta := range b.AddressStats {
		a := s.Addresses[addr]
		if a == nil {
			a = &AddressTotals{FirstSeen: delta.BlockNumber, LastSeen: delta.BlockNumber}
			s.Addresses[addr] = a
		}
		a.Txs += int64(delta.TxCountDelta)
		a.Sent += int64(delta.SentDelta)
		a.Internal += int64(delta.InternalTxCountDelta)
		a.Transfers += int64(delta.TokenTransferDelta)
		a.FirstSeen = min(a.FirstSeen, delta.BlockNumber)
		a.LastSeen = max(a.LastSeen, delta.BlockNumber)
	}
	return nil
}

// stageSpanTotals writes a span's totals into the stage tables, replacing what they held for it.
func stageSpanTotals(ctx context.Context, tx pgx.Tx, lo int64, s *SpanTotals) error {
	if _, err := tx.Exec(ctx, `SELECT ledger_stage_clear($1)`, lo); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_stage_chain (lo, name, value)
		VALUES ($1, 'blocks_total', $2), ($1, 'transactions_total', $3), ($1, 'transfers_total', $4)`,
		lo, s.Blocks, s.Transactions, s.Transfers); err != nil {
		return err
	}
	tokens := make([][]any, 0, len(s.Tokens))
	for token, t := range s.Tokens {
		tokens = append(tokens, []any{lo, token, t.Transfers, t.Supply.String()})
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ledger_stage_token"}, []string{"lo", "token", "transfers", "supply"},
		pgx.CopyFromRows(tokens)); err != nil {
		return err
	}
	addresses := make([][]any, 0, len(s.Addresses))
	for addr, a := range s.Addresses {
		addresses = append(addresses, []any{lo, addr, a.Txs, a.Sent, a.Internal, a.Transfers, int64(a.FirstSeen), int64(a.LastSeen)})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"ledger_stage_address"},
		[]string{"lo", "address", "txs", "sent", "internal", "transfers", "first_seen", "last_seen"}, pgx.CopyFromRows(addresses))
	return err
}

// SettleNodeSpan compares a closed span's stored rows with its totals counted from the node and settles it, as the
// retained recount does with totals counted from rows. It takes compaction's lock and settles only a stored span that
// is exactly the range it counted, whatever its source: one compaction widened while the count ran keeps its rows, and
// its State says so.
func (d *DB) SettleNodeSpan(ctx context.Context, lo, hi int64, s *SpanTotals, repair bool) (SpanCheck, error) {
	c := SpanCheck{Lo: lo, Hi: hi}
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4672, 1)`); err != nil {
			return err
		}
		// The rebase lock before any span row (truthMoved): a settlement that corrects the ledger must not land between
		// a rebase batch's read and its write.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, 1)`, rebaseLock); err != nil {
			return err
		}
		var same bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ledger_spans WHERE lo = $1 AND hi = $2)`,
			lo, hi).Scan(&same); err != nil {
			return err
		}
		if !same {
			c.State = "changed"
			return nil
		}
		if err := stageSpanTotals(ctx, tx, lo, s); err != nil {
			return err
		}
		return settleSpan(ctx, tx, &c, repair)
	})
	return c, err
}

// DeferPrunedSpan puts a pruned span its recount could not settle at the back of the recount's queue, leaving its
// rows and its state as they are.
func (d *DB) DeferPrunedSpan(ctx context.Context, lo int64) error {
	_, err := d.pool.Exec(ctx, `UPDATE ledger_spans SET checked_at = now() WHERE lo = $1`, lo)
	return err
}

// NextPrunedSpan is the pruned span checked longest ago (quarantined ones excepted); ok is false when none is pruned.
func (d *DB) NextPrunedSpan(ctx context.Context) (lo, hi int64, ok bool, err error) {
	err = d.pool.QueryRow(ctx, `
		SELECT lo, hi FROM ledger_spans
		WHERE hi <= (SELECT history_from()) AND check_state <> 'quarantined'
		ORDER BY checked_at NULLS FIRST, lo LIMIT 1`).Scan(&lo, &hi)
	if err == pgx.ErrNoRows {
		return 0, 0, false, nil
	}
	return lo, hi, err == nil, err
}

// PrunedSpanFrom is the first block a recount of the closed span [lo, hi) counts: lo, except that block 0 counts only
// when the span's ledger counted it. Ingest never stores block 0 on an empty database, and a closed span is complete
// but for that block, so span 0 counted it exactly when its blocks_total is its width (the stats backfill counts it).
func (d *DB) PrunedSpanFrom(ctx context.Context, lo, hi int64) (int64, error) {
	if lo != 0 {
		return lo, nil
	}
	var whole bool
	err := d.pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT value FROM ledger_chain WHERE lo = $1 AND name = 'blocks_total') = $2 - $1, false)`,
		lo, hi).Scan(&whole)
	if err != nil || whole {
		return lo, err
	}
	return 1, nil
}
