package indexedread

import (
	"context"
	_ "embed"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/indexedquery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

//go:embed testdata/existing-indexer.sql
var existingIndexerSQL string

//go:embed testdata/indexer-009-up.sql
var currentCounterSQL string

type pgFixture struct {
	owner   *pgx.Conn
	dsn     string
	binding indexedquery.Binding
}

func testBinding(reference [32]byte) indexedquery.Binding {
	return indexedquery.Binding{Network: indexedquery.Network{Environment: "test", ChainID: "1", GenesisHash: "0x" + strings.Repeat("0", 63) + "1", DeploymentID: "index-reader-test"}, SourceReference: reference, SchemaDigest: SchemaDigest(), ConfiguredEpoch: uuid.NewString()}
}

func indexFixture(t *testing.T) *pgFixture { return indexFixtureVersion(t, true) }
func indexFixtureVersion(t *testing.T, current bool) *pgFixture {
	if os.Getenv("INDEXED_READ_INTEGRATION") != "1" {
		t.Skip("set INDEXED_READ_INTEGRATION=1 for actual owned PostgreSQL tests")
	}
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "image", "inspect", "postgres:16-alpine").CombinedOutput(); err != nil {
		t.Fatal("owned cached PostgreSQL required", err, string(output))
	}
	output, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach", "--pull=never", "--memory=256m", "--cpus=1", "--publish", "127.0.0.1::5432", "--env", "POSTGRES_PASSWORD=index-public-test-only", "--env", "POSTGRES_DB=index_test", "--name", "ops-v2-index-test-"+uuid.NewString()[:8], "postgres:16-alpine").CombinedOutput()
	if err != nil {
		t.Fatal(err, string(output))
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if output, err := exec.CommandContext(cleanup, "docker", "rm", "--force", id).CombinedOutput(); err != nil {
			t.Error("owned PostgreSQL cleanup", err, string(output))
		}
	})
	output, err = exec.CommandContext(ctx, "docker", "port", id, "5432/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse("postgres://postgres:index-public-test-only@" + strings.TrimSpace(string(output)) + "/index_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	f := &pgFixture{}
	for ctx.Err() == nil {
		f.owner, err = pgx.Connect(ctx, u.String())
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = f.owner.Close(cleanup)
	})
	f.exec(t, existingIndexerSQL)
	if current {
		f.exec(t, currentCounterSQL)
	}
	f.exec(t, "CREATE ROLE index_reader LOGIN NOINHERIT PASSWORD 'index-public-test-only'")
	f.exec(t, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
	f.exec(t, "GRANT USAGE ON SCHEMA public TO index_reader")
	definition, err := compiledDefinition()
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range definition.Tables {
		var columns []string
		for _, column := range table.Columns {
			if column.Select {
				columns = append(columns, pgx.Identifier{column.Name}.Sanitize())
			}
		}
		f.exec(t, "GRANT SELECT ("+strings.Join(columns, ",")+") ON public."+pgx.Identifier{table.Name}.Sanitize()+" TO index_reader")
	}
	u.User = url.UserPassword("index_reader", "index-public-test-only")
	f.dsn = u.String()
	reference, err := ConnectionReference(f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	f.binding = testBinding(reference)
	return f
}

func (f *pgFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.owner.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal("owned test setup", err)
	}
}

func (f *pgFixture) open(t *testing.T) *Reader {
	t.Helper()
	r, err := OpenReader(t.Context(), f.dsn, f.binding, DefaultLimits())
	if err != nil {
		f.diagnoseCold(t)
		t.Fatal("expected fixed read-only reader", err)
	}
	t.Cleanup(r.Close)
	return r
}

func (f *pgFixture) diagnoseCold(t *testing.T) {
	t.Helper()
	c, err := pgx.Connect(t.Context(), f.dsn)
	if err != nil {
		t.Log("owned cold diagnostic connect", err)
		return
	}
	defer c.Close(t.Context())
	definition, _ := compiledDefinition()
	names := make([]string, len(definition.Tables))
	for i, table := range definition.Tables {
		names[i] = table.Name
	}
	for _, sql := range []string{"begin isolation level repeatable read read only", "set local search_path = pg_catalog;set local row_security=off", lockCatalogSQL} {
		if _, err := c.Exec(t.Context(), sql); err != nil {
			t.Log("owned cold diagnostic command", err)
			return
		}
	}
	var allowed bool
	if err := c.QueryRow(t.Context(), rolesSQL, "index_reader", names).Scan(&allowed); err != nil {
		t.Log("owned cold diagnostic ACL", err)
		return
	}
	t.Log("owned cold diagnostic ACL allowed", allowed)
	var catalog *string
	if err := c.QueryRow(t.Context(), catalogSQL, names, 2<<20).Scan(&catalog); err != nil {
		t.Log("owned cold diagnostic catalog", err)
		return
	}
	if catalog == nil {
		t.Log("owned catalog over cap")
		return
	}
	t.Log("owned catalog validation", validateSchema([]byte(*catalog), 2<<20))
	if validateSchema([]byte(*catalog), 2<<20) != nil {
		t.Log("owned catalog", *catalog)
	}
}

func TestReaderPostgres(t *testing.T) {
	f := indexFixture(t)
	r := f.open(t)
	t.Run("exact actual schema and excluded ABI", func(t *testing.T) {
		metadata := r.Metadata()
		if metadata.Binding != f.binding || !metadata.Identity.Valid() || metadata.Mode != "existing-indexer-fixed-reads/v1" {
			t.Fatal("reader metadata did not bind actual instance")
		}
		var abiSelect, tempAllowed bool
		err := r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error {
			return s.one("SELECT has_column_privilege('public.contracts','abi','SELECT'), has_database_privilege(current_database(),'TEMP')", nil, &abiSelect, &tempAllowed)
		})
		if err != nil || abiSelect || !tempAllowed {
			t.Fatal("column ceiling or ordinary TEMP precision", abiSelect, tempAllowed, err)
		}
	})
	t.Run("repeatable snapshot then fresh snapshot", func(t *testing.T) {
		f.exec(t, "INSERT INTO address_stats(address,tx_count) VALUES ('snapshot-address',1)")
		var before, after int
		read := func(s *snapshot, dest *int) error {
			return s.one("SELECT tx_count FROM public.address_stats WHERE address=$1", []any{"snapshot-address"}, dest)
		}
		err := r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error {
			if err := read(s, &before); err != nil {
				return err
			}
			f.exec(t, "UPDATE address_stats SET tx_count=2 WHERE address='snapshot-address'")
			return read(s, &after)
		})
		if err != nil || before != 1 || after != 1 {
			t.Fatal("mixed RR snapshot", before, after, err)
		}
		err = r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error { return read(s, &after) })
		if err != nil || after != 2 {
			t.Fatal("fresh recheck did not see committed write", after, err)
		}
	})
	t.Run("write and wrong role rejected", func(t *testing.T) {
		err := r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error { return s.exec("UPDATE public.address_stats SET tx_count=3") })
		if err != ErrUnavailable {
			t.Fatal("write not refused", err)
		}
		r.role = "foreign_reader"
		err = r.withSnapshot(t.Context(), newBudget(r.limits), func(*snapshot) error { t.Fatal("wrong role reached purpose"); return nil })
		r.role = "index_reader"
		if err != ErrUnavailable {
			t.Fatal("wrong role accepted", err)
		}
	})
	t.Run("effective ACL and structural changes reject cold", func(t *testing.T) {
		cases := []struct{ name, apply, undo string }{
			{"extra column privilege", "GRANT SELECT(abi) ON contracts TO index_reader", "REVOKE SELECT(abi) ON contracts FROM index_reader"},
			{"column write", "GRANT UPDATE(tx_count) ON address_stats TO index_reader", "REVOKE UPDATE(tx_count) ON address_stats FROM index_reader"},
			{"public write", "GRANT DELETE ON address_stats TO PUBLIC", "REVOKE DELETE ON address_stats FROM PUBLIC"},
			{"grant option", "GRANT SELECT(tx_count) ON address_stats TO index_reader WITH GRANT OPTION", "REVOKE GRANT OPTION FOR SELECT(tx_count) ON address_stats FROM index_reader"},
			{"role membership", "CREATE ROLE test_extra; GRANT test_extra TO index_reader", "REVOKE test_extra FROM index_reader; DROP ROLE test_extra"},
			{"schema CREATE", "GRANT CREATE ON SCHEMA public TO index_reader", "REVOKE CREATE ON SCHEMA public FROM index_reader"},
			{"foreign relation", "GRANT SELECT ON chain_counters TO index_reader", "REVOKE SELECT ON chain_counters FROM index_reader"},
			{"sequence", "GRANT USAGE ON SEQUENCE logs_id_seq TO index_reader", "REVOKE USAGE ON SEQUENCE logs_id_seq FROM index_reader"},
			{"application function", "CREATE FUNCTION public.test_unreviewed() RETURNS int LANGUAGE SQL AS 'SELECT 1'", "DROP FUNCTION public.test_unreviewed()"},
			{"owned persistent enum", "CREATE TYPE public.test_owned_enum AS ENUM ('public-test'); ALTER TYPE public.test_owned_enum OWNER TO index_reader", "DROP TYPE public.test_owned_enum"},
			{"RLS", "ALTER TABLE address_stats ENABLE ROW LEVEL SECURITY", "ALTER TABLE address_stats DISABLE ROW LEVEL SECURITY"},
			{"extra column", "ALTER TABLE address_stats ADD COLUMN extra int", "ALTER TABLE address_stats DROP COLUMN extra"},
			{"missing unique key", "ALTER TABLE blocks DROP CONSTRAINT blocks_hash_key", "ALTER TABLE blocks ADD UNIQUE(hash)"},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				f.exec(t, c.apply)
				unexpected, err := OpenReader(t.Context(), f.dsn, f.binding, DefaultLimits())
				if unexpected != nil {
					unexpected.Close()
				}
				f.exec(t, c.undo)
				if err != ErrUnavailable {
					t.Fatal("changed ACL/catalog accepted", err)
				}
			})
		}
	})
	t.Run("budget counts setup and reserves completion", func(t *testing.T) {
		b := newBudget(r.limits)
		called := false
		err := r.withSnapshot(t.Context(), b, func(s *snapshot) error {
			called = true
			for i := 0; i < 200; i++ {
				if err := s.exec("SELECT 1 WHERE false"); err != nil {
					return err
				}
			}
			return nil
		})
		if err != ErrTooBroad || !called || b.queries != r.limits.MaxQueries-1 {
			t.Fatal("dispatch/cleanup cap", b.queries, err)
		}
		if err := r.withSnapshot(t.Context(), b, func(*snapshot) error { return nil }); err != ErrTooBroad {
			t.Fatal("recheck reset budget", err)
		}
	})
	t.Run("row field and parameter caps", func(t *testing.T) {
		err := r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error {
			rows, err := s.query("SELECT 'bounded'::text FROM generate_series(1,20001)")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var value string
				if err := rows.Scan(&value); err != nil {
					return err
				}
			}
			return rows.Err()
		})
		if err != ErrTooBroad {
			t.Fatal("row cap", err)
		}
		err = r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error {
			var value string
			return s.one("SELECT $1::text", []any{strings.Repeat("p", r.limits.MaxParamBytes)}, &value)
		})
		if err != ErrTooBroad {
			t.Fatal("parameter cap", err)
		}
		// Fixed purposes guard variable fields in SQL before transport. NULL is
		// an explicit too-broad marker, never an empty/absent success.
		f.exec(t, "INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count) VALUES (0,'field-block','parent',0,0,0,1)")
		f.exec(t, "INSERT INTO transactions(hash,block_number,tx_index,from_address,value,gas_used,gas_price,status,error) VALUES ('field-tx',0,0,'sender',0,0,0,1,$1)", strings.Repeat("a", r.limits.MaxFieldBytes+1))
		var field *string
		err = r.withSnapshot(t.Context(), newBudget(r.limits), func(s *snapshot) error {
			if err := s.one("SELECT CASE WHEN octet_length(error)<=$1::int THEN error ELSE NULL END FROM public.transactions WHERE hash='field-tx'", []any{r.limits.MaxFieldBytes}, &field); err != nil {
				return err
			}
			if field == nil {
				return ErrTooBroad
			}
			return nil
		})
		if err != ErrTooBroad {
			t.Fatal("SQL field guard", err)
		}
		f.exec(t, "DELETE FROM blocks WHERE number=0")
	})
	t.Run("original budget deadline cancellation and final usability", func(t *testing.T) {
		b := newBudget(r.limits)
		b.deadline = time.Now().Add(-time.Second)
		if err := r.withSnapshot(t.Context(), b, func(*snapshot) error { t.Fatal("expired original pass reached SQL purpose"); return nil }); err != ErrUnavailable {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := r.withSnapshot(ctx, newBudget(r.limits), func(*snapshot) error { t.Fatal("cancelled request reached SQL purpose"); return nil }); err != ErrUnavailable {
			t.Fatal(err)
		}
		if err := r.withSnapshot(t.Context(), newBudget(r.limits), func(*snapshot) error { return nil }); err != nil {
			t.Fatal("cleanup poisoned healthy pool", err)
		}
	})
	t.Run("server committed but response lost stays unavailable", func(t *testing.T) {
		proxy := commitResponseProxy(t, f.dsn)
		u, _ := url.Parse(f.dsn)
		u.Host = proxy.address
		expected := f.binding
		var err error
		expected.SourceReference, err = ConnectionReference(u.String())
		if err != nil {
			t.Fatal(err)
		}
		reader, err := OpenReader(t.Context(), u.String(), expected, DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		proxy.drop.Store(true)
		purposeRan := false
		err = reader.withSnapshot(t.Context(), newBudget(reader.limits), func(s *snapshot) error {
			var one int
			if err := s.one("SELECT 1", nil, &one); err != nil {
				return err
			}
			purposeRan = one == 1
			return nil
		})
		if err != ErrUnavailable || !purposeRan {
			t.Fatal("uncertain completion exposed positive result", purposeRan, err)
		}
		select {
		case <-proxy.committed:
		case <-time.After(time.Second):
			t.Fatal("did not observe actual server COMMIT before dropped response")
		}
	})
}
