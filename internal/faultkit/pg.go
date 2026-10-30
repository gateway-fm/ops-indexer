//go:build faults

// Package faultkit injects faults for the fault-injection tests (build tag "faults"): a Postgres container, a
// proxy on the Postgres wire protocol that loses a COMMIT's acknowledgement, fails a COMMIT or kills a connection
// before a chosen statement, a JSON-RPC node over HTTP that lags, forks, errors, returns null or does not answer, and
// long readers that hold a lock and the xmin horizon.
package faultkit

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	pgOnce sync.Once
	pgURL  *url.URL
	pgErr  error
	dbSeq  atomic.Int64
)

// Image is the server image; production runs 17.
var Image = "postgres:17-alpine"

func startPG() {
	ctx := context.Background()
	c, err := postgres.Run(ctx, Image, postgres.WithDatabase("postgres"), postgres.WithUsername("fk"), postgres.WithPassword("fk"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		testcontainers.WithCmd("postgres", "-c", "max_connections=300", "-c", "max_locks_per_transaction=256"))
	if err != nil {
		pgErr = err
		return
	}
	s, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgErr = err
		return
	}
	pgURL, pgErr = url.Parse(s)
}

// NewDB creates an empty database on the shared server and returns its URL (dropped at cleanup).
func NewDB(t testing.TB) string {
	t.Helper()
	pgOnce.Do(startPG)
	if pgErr != nil {
		t.Skipf("no postgres: %v", pgErr)
	}
	ctx := context.Background()
	name := fmt.Sprintf("fk_%d_%d", time.Now().UnixNano()%1_000_000, dbSeq.Add(1))
	conn, err := pgx.Connect(ctx, pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u := *pgURL
	u.Path = "/" + name
	t.Cleanup(func() {
		if c, err := pgx.Connect(ctx, pgURL.String()); err == nil {
			_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			c.Close(ctx)
		}
	})
	return u.String()
}

// Conn opens a plain connection (a consumer, an admin) to url, closed at cleanup.
func Conn(t testing.TB, url string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// LongReader opens a REPEATABLE READ transaction that reads table, holding its lock and the xmin horizon until
// the returned func ends it.
func LongReader(t testing.TB, url, table string) func() {
	t.Helper()
	c := Conn(t, url)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "SELECT count(*) FROM "+table); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	end := func() { once.Do(func() { _, _ = c.Exec(ctx, "ROLLBACK") }) }
	t.Cleanup(end)
	return end
}
