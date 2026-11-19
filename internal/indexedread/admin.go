package indexedread

import (
	"context"
	"github.com/jackc/pgx/v5"
	"sort"
	"strings"
)

// ProvisionReader is an offline operator action, not a serving API. The indexer
// owns this descriptor and grants; consumers never receive either DSN.
func ProvisionReader(ctx context.Context, ownerDSN, readerDSN string) error {
	owner, e := parseConnection(ownerDSN)
	if e != nil {
		return e
	}
	reader, e := parseConnection(readerDSN)
	if e != nil {
		return e
	}
	a, b := owner.config.ConnConfig, reader.config.ConnConfig
	if a.Host != b.Host || a.Port != b.Port || a.Database != b.Database || a.User == b.User {
		return ErrInvalid
	}
	columns, e := ReadColumns()
	if e != nil {
		return e
	}
	conn, e := pgx.ConnectConfig(ctx, a)
	if e != nil {
		return ErrUnavailable
	}
	defer conn.Close(context.Background())
	tx, e := conn.Begin(ctx)
	if e != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(context.Background())
	role := pgx.Identifier{b.User}.Sanitize()
	for _, sql := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC",
		"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM " + role,
		"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM " + role,
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM " + role,
		"GRANT USAGE ON SCHEMA public TO " + role,
	} {
		if _, e = tx.Exec(ctx, sql); e != nil {
			return ErrUnavailable
		}
	}
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, table := range names {
		list := make([]string, 0, len(columns[table]))
		for _, col := range columns[table] {
			list = append(list, pgx.Identifier{col}.Sanitize())
		}
		sql := "GRANT SELECT (" + strings.Join(list, ",") + ") ON " + pgx.Identifier{"public", table}.Sanitize() + " TO " + role
		if _, e = tx.Exec(ctx, sql); e != nil {
			return ErrUnavailable
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return ErrUnavailable
	}
	return nil
}
