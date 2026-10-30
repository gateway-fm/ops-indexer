//go:build faults

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

func fkLedgerPair(t *testing.T) (ref, d *DB, p *faultkit.Proxy, url string) {
	ref, _, _ = fkDB(t)
	d, p, url = fkDB(t)
	for _, x := range []*DB{ref, d} {
		seedHistory(t, x, 0, 20_000, 1)
	}
	closeLedger(t, ref)
	return
}

func TestFaultLedgerCloseFaults(t *testing.T) {
	for _, c := range []struct {
		name   string
		action faultkit.Action
		match  string
	}{
		{"lost-ack-close", faultkit.LostAck, "ledger_close_span"},
		{"fail-commit-close", faultkit.FailCommit, "ledger_close_span"},
		{"kill-before-close", faultkit.KillBefore, "ledger_close_span"},
		{"kill-before-next-span", faultkit.KillBefore, "ledger_next_span"},
		{"lost-ack-gate", faultkit.LostAck, "pg_advisory_xact_lock_shared"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref, d, p, url := fkLedgerPair(t)
			ctx := context.Background()
			n, err := d.CloseLedgerSpans(ctx, testLedger, 3)
			require.NoError(t, err)
			require.Equal(t, 3, n)
			r := p.Arm(faultkit.Rule{Match: c.match, Action: c.action})
			_, err = d.CloseLedgerSpans(ctx, testLedger, 1_000_000)
			require.Error(t, err)
			require.Equal(t, 1, p.Fired(r))
			// the same process retries
			_, err = d.CloseLedgerSpans(ctx, testLedger, 2)
			require.NoError(t, err)
			// a restart closes the rest
			_, err = fkRestart(t, url).CloseLedgerSpans(ctx, testLedger, 1_000_000)
			require.NoError(t, err)
			fkSame(t, ref, d, fkLedgerQueries)
		})
	}
}

// RunLedger goes on after the database was unreachable for a while.
func TestFaultRunLedgerAfterAnOutage(t *testing.T) {
	ref, d, p, _ := fkLedgerPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Down()
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.RunLedger(ctx, LedgerConfig{SpanBlocks: 1000, Interval: 50 * time.Millisecond,
			HeadLag: func(context.Context) (uint64, error) { return 0, context.DeadlineExceeded }, MaxHeadLag: 10})
	}()
	time.Sleep(time.Second)
	p.Up()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var spans int
		require.NoError(t, ref.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_spans`).Scan(&spans))
		var got int
		_ = d.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_spans`).Scan(&got)
		if got == spans {
			break
		}
		require.True(t, time.Now().Before(deadline), "closed %d of %d spans after the outage", got, spans)
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
	fkSame(t, ref, d, fkLedgerQueries)
}

// A consumer's long REPEATABLE READ reader on a history table during the close.
func TestFaultLedgerCloseUnderALongReader(t *testing.T) {
	ref, d, _, url := fkLedgerPair(t)
	end := faultkit.LongReader(t, url, "transactions")
	defer end()
	done := make(chan error, 1)
	go func() { _, err := d.CloseLedgerSpans(context.Background(), testLedger, 1_000_000); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("the close waited on the reader")
	}
	fkSame(t, ref, d, fkLedgerQueries)
}
