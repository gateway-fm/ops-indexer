//go:build faults

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

func TestFaultSmoke(t *testing.T) {
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d, err := db.New(proxy.URL())
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Migrate())
	node := faultkit.NewNode(t, 300)
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	idx := NewWithConfig(d, client, 50*time.Millisecond, 0, &Config{RPCWorkers: 4, RPCRateLimit: 1000, DBBatchSize: 50,
		CatchupEnabled: true, CatchupWorkers: 4, CatchupBatchSize: 50, CatchupQueueSize: 100, TokenMetadataWorkers: 2, BalanceWorkers: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() { _ = idx.Start(ctx) }()
	admin := faultkit.Conn(t, url)
	for {
		var n int64
		require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM blocks`).Scan(&n))
		if n >= 300 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("only %d blocks", n)
		}
		time.Sleep(200 * time.Millisecond)
	}
	var tx, tt int64
	require.NoError(t, admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM transactions), (SELECT count(*) FROM token_transfers)`).Scan(&tx, &tt))
	t.Logf("blocks>=300 txs=%d transfers=%d", tx, tt)
	cancel()
}
