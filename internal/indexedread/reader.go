package indexedread

import (
	"context"
	_ "embed"
	"errors"
	"strconv"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/indexedquery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed catalog.sql
var catalogSQL string

//go:embed roles.sql
var rolesSQL string

// Parsing this fixed zero-row read takes ACCESS SHARE locks until completion.
// Explicit LOCK TABLE requires table-wide SELECT, which would accidentally
// force a grant for contracts.abi despite its deliberately excluded column.
const lockCatalogSQL = `SELECT 1 FROM public.address_stats, public.balances, public.blocks, public.contracts, public.indexer_progress, public.internal_transactions, public.logs, public.sync_status, public.token_transfers, public.tokens, public.transactions WHERE false`

// Reader owns only the existing indexer's operational read connection. It has
// no configuration/user authority and exports neither a driver nor arbitrary
// SQL callbacks. Fixed purposes in this package borrow snapshots privately.
type Reader struct {
	pool     *pgxpool.Pool
	metadata indexedquery.Metadata
	limits   Limits
	role     string
}

func OpenReader(ctx context.Context, dsn string, expected indexedquery.Binding, limits Limits) (*Reader, error) {
	if ctx == nil || !expected.Valid() || !limits.Valid() || expected.SchemaDigest != SchemaDigest() {
		return nil, ErrInvalid
	}
	selected, err := parseConnection(dsn)
	if err != nil {
		return nil, err
	}
	// Pair the independent configured source before making any connection.
	if selected.reference != expected.SourceReference {
		return nil, ErrInvalid
	}
	selected.config.MaxConns = int32(limits.MaxConns)
	selected.config.ConnConfig.ConnectTimeout = limits.Timeout
	// The pool otherwise performs an implicit ping after idle acquisition.
	// Dedicated simple protocol plus this hook keeps SQL dispatch count explicit.
	selected.config.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return false }
	pool, err := pgxpool.NewWithConfig(ctx, selected.config)
	if err != nil {
		return nil, ErrUnavailable
	}
	r := &Reader{pool: pool, metadata: indexedquery.Metadata{Binding: expected, Mode: "existing-indexer-fixed-reads/v1", Identity: indexedquery.NewReaderIdentity()}, limits: limits, role: selected.role}
	err = r.withSnapshot(ctx, newBudget(limits), func(*snapshot) error { return nil })
	if err != nil {
		pool.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) Metadata() indexedquery.Metadata {
	if r == nil {
		return indexedquery.Metadata{}
	}
	return r.metadata
}

func (r *Reader) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

type snapshot struct {
	ctx context.Context
	tx  pgx.Tx
	b   *budget
}

func publicError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{ErrInvalid, ErrTooBroad, ErrConflict} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrUnavailable
}

// withSnapshot is deliberately private. Node/share/wallet calls belong after
// it returns; a callback result becomes usable only after known COMMIT.
// A purpose owns b through its original read and fresh recheck together.
func (r *Reader) withSnapshot(parent context.Context, b *budget, read func(*snapshot) error) (err error) {
	if r == nil || r.pool == nil || parent == nil || b == nil || b.limits != r.limits || read == nil {
		return ErrInvalid
	}
	if b.deadline.IsZero() {
		return ErrInvalid
	}
	ctx, cancel := context.WithDeadline(parent, b.deadline)
	defer cancel()
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer conn.Release()
	const begin = "begin isolation level repeatable read read only"
	if err = b.command(false, begin); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		// A failed BEGIN can leave an uncertain protocol/transaction state.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		_ = conn.Conn().Close(closeCtx)
		closeCancel()
		return ErrUnavailable
	}
	completed := false
	defer func() {
		if !completed {
			cleanup, done := context.WithTimeout(context.Background(), time.Second)
			if b.command(true, "rollback") != nil {
				_ = conn.Conn().Close(cleanup)
			} else if tx.Rollback(cleanup) != nil {
				_ = conn.Conn().Close(cleanup)
			}
			done()
		}
		err = publicError(err)
	}()
	s := &snapshot{ctx, tx, b}
	// Ignore connection/database role defaults; fixed queries are fully
	// qualified. TEMP is not app authority and is never used by this reader.
	setup := "set local statement_timeout = '" + strconv.FormatInt(r.limits.StatementTimeout.Milliseconds(), 10) + "ms'; set local lock_timeout = '" + strconv.FormatInt(r.limits.LockTimeout.Milliseconds(), 10) + "ms'; set local search_path = pg_catalog; set local row_security = off"
	if err = s.exec(setup); err != nil {
		return err
	}
	if err = s.exec(lockCatalogSQL); err != nil {
		return err
	}
	definition, err := compiledDefinition()
	if err != nil {
		return err
	}
	names := make([]string, len(definition.Tables))
	for i, t := range definition.Tables {
		names[i] = t.Name
	}
	var allowed bool
	if err = s.one(rolesSQL, []any{r.role, names}, &allowed); err != nil || !allowed {
		return ErrUnavailable
	}
	var catalog *string
	if err = s.one(catalogSQL, []any{names, r.limits.MaxFieldBytes}, &catalog); err != nil {
		return err
	}
	if catalog == nil {
		return ErrTooBroad
	}
	// Detached JSON bytes and decoded field payload supplement the wire/string
	// charges. The schema fixes table/column/key object cardinalities too.
	if err = b.retain(2 * len(*catalog)); err != nil {
		return err
	}
	if err = validateSchema([]byte(*catalog), r.limits.MaxFieldBytes); err != nil {
		return err
	}
	if err = read(s); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ErrUnavailable
	}
	if err = b.command(true, "commit"); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	completed = true
	if ctx.Err() != nil {
		return ErrUnavailable
	}
	return nil
}

func (s *snapshot) exec(sql string, args ...any) error {
	if err := s.b.command(false, sql, args...); err != nil {
		return err
	}
	_, err := s.tx.Exec(s.ctx, sql, args...)
	return publicError(err)
}

// query only serves compiled package purposes. Each query must SQL-bound
// variable fields before transport; charging RawValues alone is too late.
func (s *snapshot) query(sql string, args ...any) (*chargedRows, error) {
	if err := s.b.command(false, sql, args...); err != nil {
		return nil, err
	}
	rows, err := s.tx.Query(s.ctx, sql, args...)
	if err != nil {
		return nil, publicError(err)
	}
	return &chargedRows{rows: rows, b: s.b}, nil
}

func (s *snapshot) one(sql string, args []any, dest ...any) error {
	rows, err := s.query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return publicError(rows.Err())
		}
		return ErrUnavailable
	}
	if err := rows.Scan(dest...); err != nil {
		return publicError(err)
	}
	if rows.Next() {
		return ErrUnavailable
	}
	if rows.Err() != nil {
		return publicError(rows.Err())
	}
	return nil
}

type chargedRows struct {
	rows pgx.Rows
	b    *budget
	err  error
}

func (r *chargedRows) Next() bool {
	if r.err != nil || !r.rows.Next() {
		return false
	}
	if err := r.b.row(r.rows.RawValues()); err != nil {
		r.err = err
		r.rows.Close()
		return false
	}
	return true
}

func (r *chargedRows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Err()
}

func (r *chargedRows) Close() { r.rows.Close() }

func (r *chargedRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	n := 0
	for _, raw := range r.rows.RawValues() {
		n += len(raw)
	}
	if err := r.b.retain(n); err != nil {
		r.err = err
		r.rows.Close()
		return err
	}
	return r.rows.Scan(dest...)
}
