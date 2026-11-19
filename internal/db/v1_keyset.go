package db

// V1 keeps its original wire types and ordering. These queries provide the
// bounded keyset adapter behind both its opaque and legacy numbered pages.
import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

var ErrV1PageBudget = errors.New("legacy numbered page exceeds the keyset walk budget; continue using next_cursor")
var ErrV1BlockNotFound = errors.New("block hash not found")
var ErrV1Cursor = errors.New("cursor does not belong to this source or has an invalid key")

type V1HistoryPruned struct{ Floor uint64 }

func (e *V1HistoryPruned) Error() string {
	return fmt.Sprintf("history before block %d is pruned", e.Floor)
}

// V1PagePosition is private server continuation state, never a wire schema.
type V1PagePosition struct {
	Key      []string `json:"k,omitempty"`
	Source   string   `json:"s"`
	Block    uint64   `json:"b,omitempty"`
	Terminal bool     `json:"p,omitempty"`
	Page     int      `json:"n,omitempty"`
}
type V1PageInput struct {
	Position    V1PagePosition
	Limit, Page int
	Skip        int
}
type V1PageResult[T any] struct {
	Rows  []T
	Total int64
	Next  *V1PagePosition
	Page  int
}
type v1Order struct {
	column, cast string
	descending   bool
}
type v1Query[T any] struct {
	columns, from, history string
	blockHash              string
	conditions             []string
	args                   []any
	order                  []v1Order
	lower, upper           uint64
	total                  string
	skipTotal              bool
	totalArgs              []any
	scan                   func(pgx.Rows) ([]T, error)
	key                    func(T) []string
	block                  func(T) uint64
}

func (q *v1Query[T]) add(condition string, value any) {
	q.args = append(q.args, value)
	q.conditions = append(q.conditions, fmt.Sprintf(condition, len(q.args)))
}
func (q *v1Query[T]) blockRange(lower, upper uint64) {
	q.lower, q.upper = lower, upper
	if lower > 0 {
		q.add(q.history+">=$%d", lower)
	}
	if upper > 0 {
		q.add(q.history+"<$%d", upper)
	}
}
func (q *v1Query[T]) where() string {
	if len(q.conditions) == 0 {
		return "TRUE"
	}
	return strings.Join(q.conditions, " AND ")
}
func (q *v1Query[T]) orderSQL() string {
	parts := make([]string, len(q.order))
	for i, o := range q.order {
		parts[i] = o.column
		if o.descending {
			parts[i] += " DESC"
		} else {
			parts[i] += " ASC"
		}
	}
	return strings.Join(parts, ",")
}
func (q *v1Query[T]) seek(key []string) (string, []any, error) {
	args := append([]any(nil), q.args...)
	if len(key) == 0 {
		return "", args, nil
	}
	if len(key) != len(q.order) {
		return "", nil, ErrV1Cursor
	}
	parts := make([]string, len(key))
	prefix := []string{}
	for i, o := range q.order {
		args = append(args, key[i])
		ref := fmt.Sprintf("$%d::%s", len(args), o.cast)
		op := ">"
		if o.descending {
			op = "<"
		}
		parts[i] = "(" + strings.Join(append(append([]string(nil), prefix...), o.column+op+ref), " AND ") + ")"
		prefix = append(prefix, o.column+"="+ref)
	}
	return " AND (" + strings.Join(parts, " OR ") + ")", args, nil
}

const v1LegacyMaxRows = 10000
const v1LegacyMaxPages = 100

func v1Read[T any](ctx context.Context, d *DB, q v1Query[T], in V1PageInput) (out V1PageResult[T], err error) {
	if in.Limit < 1 || in.Limit > 1000 {
		return out, ErrV1PageBudget
	}
	if in.Page < 1 {
		in.Page = 1
	}
	if len(in.Position.Key) > 0 || in.Position.Terminal {
		in.Page = in.Position.Page + 1
	}
	// Arithmetic is checked before a multiplication or any database work.
	if in.Skip < 0 || in.Skip > v1LegacyMaxRows || (in.Position.Source == "" && (in.Page > v1LegacyMaxPages || in.Page-1 > v1LegacyMaxRows/in.Limit)) {
		return out, ErrV1PageBudget
	}
	tx, cleanup, err := d.BeginHistorySnapshot(ctx)
	if err != nil {
		return out, err
	}
	defer cleanup()
	// Database identity and genesis bind the old API to the selected data source.
	// The dataset epoch also invalidates continuations after a destructive wipe.
	var source string
	err = tx.QueryRow(ctx, `SELECT dataset_epoch::text || ':' || COALESCE(chain_id,'') || ':' || COALESCE(encode(genesis_hash,'hex'),'') FROM indexer_dataset_metadata WHERE id=1`).Scan(&source)
	if err != nil {
		return out, err
	}
	if in.Position.Source != "" && in.Position.Source != source {
		return out, ErrV1Cursor
	}
	var floor uint64
	if q.history != "" {
		if err = tx.QueryRow(ctx, `SELECT history_from()`).Scan(&floor); err != nil {
			return out, err
		}
		if q.blockHash != "" {
			var number uint64
			err = tx.QueryRow(ctx, `SELECT number FROM blocks WHERE hash=$1`, q.blockHash).Scan(&number)
			if errors.Is(err, pgx.ErrNoRows) {
				return out, ErrV1BlockNotFound
			}
			if err != nil {
				return out, err
			}
			if number < floor {
				return out, ErrV1BlockNotFound
			}
			q.lower = max(q.lower, number)
			if q.upper == 0 || number+1 < q.upper {
				q.upper = number + 1
			}
		}
		if floor > 0 && ((q.upper > 0 && q.upper <= floor) || (in.Position.Block > 0 && in.Position.Block < floor) || in.Position.Terminal) {
			return out, &V1HistoryPruned{Floor: floor}
		}
		q.add(q.history+">=$%d", floor)
	}
	if in.Position.Terminal {
		return out, ErrV1Cursor
	}
	where := q.where()
	countSQL := "SELECT COUNT(*) FROM " + q.from + " WHERE " + where
	countArgs := q.args
	if q.total != "" {
		countSQL = q.total
		countArgs = q.totalArgs
	}
	if !q.skipTotal {
		if err = tx.QueryRow(ctx, countSQL, countArgs...).Scan(&out.Total); err != nil {
			return out, err
		}
	}
	key := in.Position.Key
	// Old page-number consumers walk complete keysets, with a hard work bound.
	// No OFFSET, row_number, giant prefix query, or silent substitution of page1.
	skip := in.Skip
	if skip == 0 && in.Position.Source == "" {
		skip = (in.Page - 1) * in.Limit
	}
	for skipped, steps := 0, 0; skipped < skip && in.Position.Source == ""; steps++ {
		if steps >= v1LegacyMaxPages {
			return out, ErrV1PageBudget
		}
		step := min(in.Limit, skip-skipped)
		seek, args, e := q.seek(key)
		if e != nil {
			return out, e
		}
		args = append(args, step)
		rows, e := tx.Query(ctx, "SELECT "+q.columns+" FROM "+q.from+" WHERE "+where+seek+" ORDER BY "+q.orderSQL()+fmt.Sprintf(" LIMIT $%d", len(args)), args...)
		if e != nil {
			return out, e
		}
		items, e := q.scan(rows)
		rows.Close()
		if e != nil {
			return out, e
		}
		if len(items) < step {
			if q.history != "" && floor > q.lower {
				return out, &V1HistoryPruned{Floor: floor}
			}
			out.Page = in.Page
			return out, tx.Commit(ctx)
		}
		key = q.key(items[len(items)-1])
		skipped += len(items)
	}
	seek, args, err := q.seek(key)
	if err != nil {
		return out, err
	}
	args = append(args, in.Limit+1)
	rows, err := tx.Query(ctx, "SELECT "+q.columns+" FROM "+q.from+" WHERE "+where+seek+" ORDER BY "+q.orderSQL()+fmt.Sprintf(" LIMIT $%d", len(args)), args...)
	if err != nil {
		return out, err
	}
	out.Rows, err = q.scan(rows)
	rows.Close()
	if err != nil {
		return out, err
	}
	out.Page = in.Page
	more := len(out.Rows) > in.Limit
	if more {
		out.Rows = out.Rows[:in.Limit]
	}
	prunedEnd := q.history != "" && floor > q.lower
	if len(out.Rows) == 0 && prunedEnd {
		return out, &V1HistoryPruned{Floor: floor}
	}
	if len(out.Rows) > 0 && (more || prunedEnd) {
		last := out.Rows[len(out.Rows)-1]
		next := &V1PagePosition{Key: q.key(last), Source: source, Page: in.Page, Terminal: !more}
		if q.block != nil {
			next.Block = q.block(last)
		}
		out.Next = next
	}
	return out, tx.Commit(ctx)
}
func v1U(v uint64) string                { return strconv.FormatUint(v, 10) }
func v1I(v int) string                   { return strconv.Itoa(v) }
func v1I64(v int64) string               { return strconv.FormatInt(v, 10) }
func v1Desc(column, cast string) v1Order { return v1Order{column, cast, true} }
func v1Asc(column, cast string) v1Order  { return v1Order{column, cast, false} }

// V1PageSource binds an optional archive-node block continuation to the same
// selected dataset as database-backed pages. Node data is never persisted here.
func (d *DB) V1PageSource(ctx context.Context) (string, error) {
	tx, cleanup, err := d.BeginHistorySnapshot(ctx)
	if err != nil {
		return "", err
	}
	defer cleanup()
	var source string
	err = tx.QueryRow(ctx, `SELECT dataset_epoch::text || ':' || COALESCE(chain_id,'') || ':' || COALESCE(encode(genesis_hash,'hex'),'') FROM indexer_dataset_metadata WHERE id=1`).Scan(&source)
	if err != nil {
		return "", err
	}
	return source, tx.Commit(ctx)
}
