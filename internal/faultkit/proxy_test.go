//go:build faults

package faultkit

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProxyFaults(t *testing.T) {
	url := NewDB(t)
	admin := Conn(t, url)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `CREATE TABLE t (v int)`); err != nil {
		t.Fatal(err)
	}
	p := NewProxy(t, url)
	pool, err := pgxpool.New(ctx, p.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	count := func(v int) int {
		var n int
		_ = admin.QueryRow(ctx, `SELECT count(*) FROM t WHERE v = $1`, v).Scan(&n)
		return n
	}
	p.Arm(Rule{Match: "insert into t values (1", Action: LostAck})
	_, err = pool.Exec(ctx, `INSERT INTO t VALUES (1)`)
	t.Logf("autocommit lost ack: err=%v rows=%d", err, count(1))
	if err == nil || count(1) != 1 {
		t.Fatal("lost ack: want an error and the row")
	}
	p.Arm(Rule{Match: "insert into t values ($1", Action: LostAck})
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { _, err := tx.Exec(ctx, `INSERT INTO t VALUES ($1)`, 2); return err })
	t.Logf("tx lost ack: err=%v rows=%d", err, count(2))
	if err == nil || count(2) != 1 {
		t.Fatal("tx lost ack: want an error and the row")
	}
	p.Arm(Rule{Match: "insert into t values ($1", Action: FailCommit})
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { _, err := tx.Exec(ctx, `INSERT INTO t VALUES ($1)`, 3); return err })
	t.Logf("fail commit: err=%v rows=%d", err, count(3))
	if err == nil || count(3) != 0 {
		t.Fatal("fail commit: want an error and no row")
	}
	p.Arm(Rule{Match: "insert into t values ($1", Action: KillBefore})
	_, err = pool.Exec(ctx, `INSERT INTO t VALUES ($1)`, 4)
	t.Logf("kill before: err=%v rows=%d", err, count(4))
	if err == nil || count(4) != 0 {
		t.Fatal("kill before: want an error and no row")
	}
	_, err = pool.Exec(ctx, `INSERT INTO t VALUES ($1)`, 5)
	if err != nil || count(5) != 1 {
		t.Fatalf("after the rules: %v", err)
	}
}
