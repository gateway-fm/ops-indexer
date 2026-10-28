//go:build faults

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

func fkTS(n int64) int64 { return dayStart + n*600 } // 144 blocks a day

func fkStore(t *testing.T, d *DB, lo, hi int64, fork string) {
	t.Helper()
	for n := lo; n <= hi; n++ {
		_, err := d.pool.Exec(context.Background(), `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, size)
			VALUES ($1, $2, $3, $4, $5, 30000000, 1, $6)`, n, fmt.Sprintf("0x%s%d", fork, n), fmt.Sprintf("0x%s%d", fork, n-1), fkTS(n),
			21000+n, 600+n%7)
		require.NoError(t, err)
	}
}

func fkAdvance(t *testing.T, d *DB) {
	t.Helper()
	_, err := d.AdvanceDailyStats(context.Background())
	require.NoError(t, err)
}

func TestFaultDailyAdvanceFaults(t *testing.T) {
	for _, c := range []struct {
		name   string
		action faultkit.Action
		match  string
		late   bool
	}{
		{"lost-ack", faultkit.LostAck, "daily_stats_advance", false},
		{"fail-commit", faultkit.FailCommit, "daily_stats_advance", false},
		{"kill-before", faultkit.KillBefore, "daily_stats_advance", false},
		{"lost-ack-late", faultkit.LostAck, "daily_stats_advance", true},
		{"fail-commit-late", faultkit.FailCommit, "daily_stats_advance", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref, _, _ := fkDB(t)
			d, p, url := fkDB(t)
			for _, x := range []*DB{ref, d} {
				if c.late {
					fkStore(t, x, 0, 299, "a")
					fkStore(t, x, 321, 500, "a")
				} else {
					fkStore(t, x, 0, 500, "a")
				}
				fkAdvance(t, x)
				if c.late {
					fkStore(t, x, 300, 320, "a")
				}
				fkStore(t, x, 501, 1000, "a")
			}
			fkAdvance(t, ref)
			fkAdvance(t, ref)
			r := p.Arm(faultkit.Rule{Match: c.match, Action: c.action})
			_, err := d.AdvanceDailyStats(context.Background())
			require.Error(t, err)
			require.Equal(t, 1, p.Fired(r))
			fkAdvance(t, d)
			fkAdvance(t, fkRestart(t, url))
			fkSame(t, ref, d, fkDailyQueries)
		})
	}
}

func TestFaultDailyReorgInvalidateFaults(t *testing.T) {
	for _, c := range []struct {
		name   string
		action faultkit.Action
	}{
		{"lost-ack", faultkit.LostAck},
		{"fail-commit", faultkit.FailCommit},
		{"kill-before", faultkit.KillBefore},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref, _, _ := fkDB(t)
			d, p, url := fkDB(t)
			for _, x := range []*DB{ref, d} {
				fkStore(t, x, 0, 1000, "a")
				fkAdvance(t, x)
			}
			revert := func(x *DB, faulty bool) {
				for n := uint64(1000); n >= 990; n-- {
					if faulty && n == 995 {
						r := p.Arm(faultkit.Rule{Match: "daily_stats_invalidate", Action: c.action})
						require.Error(t, x.DeleteBlock(context.Background(), n))
						require.Equal(t, 1, p.Fired(r))
						x = fkRestart(t, url) // the revert resumes after a restart, from the stored head
						var head int64
						require.NoError(t, x.pool.QueryRow(context.Background(), `SELECT max(number) FROM blocks`).Scan(&head))
						for m := uint64(head); m >= n; m-- {
							require.NoError(t, x.DeleteBlock(context.Background(), m))
						}
						continue
					}
					require.NoError(t, x.DeleteBlock(context.Background(), n))
				}
			}
			revert(ref, false)
			revert(d, true)
			for _, x := range []*DB{ref, d} {
				fkStore(t, x, 990, 1010, "b")
				fkAdvance(t, x)
			}
			fkSame(t, ref, d, fkDailyQueries)
		})
	}
}

func TestFaultDailyAdvanceUnderALongReader(t *testing.T) {
	ref, _, _ := fkDB(t)
	d, _, url := fkDB(t)
	for _, x := range []*DB{ref, d} {
		fkStore(t, x, 0, 500, "a")
		fkAdvance(t, x)
		fkStore(t, x, 501, 1000, "a")
	}
	fkAdvance(t, ref)
	end := faultkit.LongReader(t, url, "blocks")
	defer end()
	done := make(chan error, 1)
	go func() { _, err := d.AdvanceDailyStats(context.Background()); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the advance waited on the reader")
	}
	fkSame(t, ref, d, fkDailyQueries)
}
