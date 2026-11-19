package indexedread

import (
	"context"
	"time"
)

// Limits bound one fixed indexed-read purpose and its recheck together.
// They bound returned data, not the number of rows PostgreSQL may scan.
type Limits struct {
	Timeout, StatementTimeout, LockTimeout        time.Duration
	MaxConns, MaxQueries, MaxResultRows           int
	MaxParamBytes, MaxChargedBytes, MaxFieldBytes int
}

func DefaultLimits() Limits {
	return Limits{5 * time.Second, 3 * time.Second, time.Second, 4, 128, 20000, 4 << 20, 16 << 20, 2 << 20}
}

func (l Limits) Valid() bool {
	m := DefaultLimits()
	return l.Timeout > 0 && l.Timeout <= m.Timeout &&
		l.StatementTimeout >= time.Millisecond && l.StatementTimeout%time.Millisecond == 0 && l.StatementTimeout <= m.StatementTimeout && l.StatementTimeout <= l.Timeout &&
		l.LockTimeout >= time.Millisecond && l.LockTimeout%time.Millisecond == 0 && l.LockTimeout <= m.LockTimeout && l.LockTimeout <= l.StatementTimeout &&
		l.MaxConns > 0 && l.MaxConns <= m.MaxConns && l.MaxQueries >= 6 && l.MaxQueries <= m.MaxQueries &&
		l.MaxResultRows > 0 && l.MaxResultRows <= m.MaxResultRows &&
		l.MaxParamBytes > 0 && l.MaxParamBytes <= m.MaxParamBytes &&
		l.MaxChargedBytes > 0 && l.MaxChargedBytes <= m.MaxChargedBytes &&
		l.MaxFieldBytes > 0 && l.MaxFieldBytes <= m.MaxFieldBytes && l.MaxFieldBytes <= l.MaxChargedBytes
}

// A budget remains owned by the concrete original read pass through recheck;
// fixed purpose methods must not restart these counters for their subqueries.
type budget struct {
	limits                         Limits
	queries, rows, params, charged int
	deadline                       time.Time
	reserve                        func(int) bool
}

func newBudget(l Limits) *budget { return &budget{limits: l, deadline: time.Now().Add(l.Timeout)} }

// Service reservations follow the existing pre-allocation charged-work ledger.
// They conservatively remain reserved until the original handle is discarded.
type retentionKey struct{}

func newBudgetForContext(ctx context.Context, l Limits) *budget {
	b := newBudget(l)
	b.reserve, _ = ctx.Value(retentionKey{}).(func(int) bool)
	return b
}

func (b *budget) command(terminal bool, sql string, args ...any) error {
	if b == nil || !b.limits.Valid() {
		return ErrInvalid
	}
	ceiling := b.limits.MaxQueries
	paramCeiling := b.limits.MaxParamBytes
	if !terminal {
		ceiling -= 2
		paramCeiling -= len("commit") + len("rollback")
	}
	// Reserve both commands and their bytes for completion and uncertain cleanup.
	if b.queries >= ceiling || len(sql) > paramCeiling-b.params {
		return ErrTooBroad
	}
	used := len(sql)
	remaining := paramCeiling - b.params - used
	for _, value := range args {
		n, err := parameterBytes(value, remaining)
		if err != nil {
			return err
		}
		used += n
		remaining -= n
	}
	b.params += used
	b.queries++
	return nil
}

// Fixed internal SQL accepts a closed set of driver primitives. Framing is
// charged even for empty strings/arrays, before a driver expands the values.
func parameterBytes(value any, remaining int) (int, error) {
	if remaining < 8 {
		return 0, ErrTooBroad
	}
	add := func(n int) (int, error) {
		if n < 0 || n > remaining-8 {
			return 0, ErrTooBroad
		}
		return n + 8, nil
	}
	switch v := value.(type) {
	case nil:
		return add(0)
	case string:
		return add(len(v))
	case []byte:
		return add(len(v))
	case bool, int, int16, int32, int64, uint16, uint32, uint64:
		return add(24)
	case time.Time:
		return add(64)
	case []string:
		if len(v) > (remaining-8)/8 {
			return 0, ErrTooBroad
		}
		n := 8
		for _, s := range v {
			if len(s) > remaining-n-8 {
				return 0, ErrTooBroad
			}
			n += len(s) + 8
		}
		return n, nil
	case [][]byte:
		if len(v) > (remaining-8)/8 {
			return 0, ErrTooBroad
		}
		n := 8
		for _, raw := range v {
			if len(raw) > remaining-n-8 {
				return 0, ErrTooBroad
			}
			n += len(raw) + 8
		}
		return n, nil
	case []uint64:
		if len(v) > (remaining-8)/32 {
			return 0, ErrTooBroad
		}
		return 8 + 32*len(v), nil
	default:
		return 0, ErrInvalid
	}
}

func (b *budget) row(raw [][]byte) error {
	if b == nil || b.rows >= b.limits.MaxResultRows {
		return ErrTooBroad
	}
	n := 0
	for _, field := range raw {
		if len(field) > b.limits.MaxFieldBytes || len(field) > b.limits.MaxChargedBytes-b.charged-n {
			return ErrTooBroad
		}
		n += len(field)
	}
	if b.reserve != nil && !b.reserve(n) {
		return ErrTooBroad
	}
	b.rows++
	b.charged += n
	return nil
}

// retain charges additional owned/decoded payload copies before allocation.
// Slice/object cardinalities are independently bounded by each fixed purpose;
// this byte count is not a claim to measure Go allocator metadata.
func (b *budget) retain(n int) error {
	if b == nil || n < 0 || n > b.limits.MaxChargedBytes-b.charged {
		return ErrTooBroad
	}
	if b.reserve != nil && !b.reserve(n) {
		return ErrTooBroad
	}
	b.charged += n
	return nil
}
