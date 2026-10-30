package indexer

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// A reorg revert left a request to read a holder's balance again (and the fork's value in current): the reread loop
// reads it at the head and writes the node's value, which clears the request; with the balance pool and without.
func TestTheRereadLoopWritesTheNodesBalanceAndClearsTheRequest(t *testing.T) {
	holder := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	token := common.HexToAddress("0x00000000000000000000000000000000000070c1")
	for _, pooled := range []bool{false, true} {
		name := "sync reads"
		if pooled {
			name = "balance pool"
		}
		t.Run(name, func(t *testing.T) {
			store, sql := setupPostgres(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := sql.Exec(ctx, `
				INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
				VALUES (10, '0xb10', '0xb9', 1700000600, 0, 1, 0);
				INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number, holder_count)
				VALUES ('`+lower(token)+`', 'T', 'T', 18, 'ERC20', 1, 1);
				INSERT INTO token_balances_current (token_address, address, balance, block_number)
				VALUES ('`+lower(token)+`', '`+lower(holder)+`', 6, 9);
				INSERT INTO balance_rereads (token_address, address, epoch)
				VALUES ('`+lower(token)+`', '`+lower(holder)+`', nextval('balance_reorg_epoch'))`)
			require.NoError(t, err)

			chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 10, balances: map[string]*big.Int{holder.Hex(): big.NewInt(42)}}
			idx := NewWithConfig(store, chain, time.Second, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
			if pooled {
				idx.balanceWorkers = NewBalanceWorkerPool(store, chain, 2, 1000)
				idx.balanceWorkers.Start()
				defer idx.balanceWorkers.Stop()
			}
			idx.rereadBalances(ctx, store)
			require.Eventually(t, func() bool {
				var open int
				_ = sql.QueryRow(ctx, `SELECT count(*) FROM balance_rereads WHERE open`).Scan(&open)
				return open == 0
			}, 20*time.Second, 50*time.Millisecond, "the request is answered")
			var balance string
			require.NoError(t, sql.QueryRow(ctx, `SELECT balance::TEXT FROM token_balances_current WHERE address = $1`,
				lower(holder)).Scan(&balance))
			require.Equal(t, "42", balance, "the node's value at the head, over the fork's")
		})
	}
}

func lower(a common.Address) string { return toLowerHex(a.Hex()) }

func toLowerHex(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'F' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// After a revert of a key, an ordinary later read of it, through each path that writes balances, is applied: the
// balance pool stamps the epoch current when the read is queued, and sync reads (no pool) write history rows directly.
func TestAReadDecidedAfterARevertIsAppliedThroughEveryPath(t *testing.T) {
	holder := common.HexToAddress("0x00000000000000000000000000000000000000a2")
	token := common.HexToAddress("0x00000000000000000000000000000000000070c2")
	setup := func(t *testing.T) (*db.DB, *pgxpool.Pool, context.Context) {
		store, sql := setupPostgres(t)
		ctx := context.Background()
		_, err := sql.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
			VALUES (10, '0xb10', '0xb9', 1700000600, 0, 1, 0);
			INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number, holder_count)
			VALUES ('`+lower(token)+`', 'T', 'T', 18, 'ERC20', 1, 0);
			INSERT INTO balance_rereads (token_address, address, epoch, open)
			VALUES ('`+lower(token)+`', '`+lower(holder)+`', nextval('balance_reorg_epoch'), false)`)
		require.NoError(t, err, "a key a revert touched, its reread answered")
		return store, sql, ctx
	}
	chain := func() *fakeChain {
		return &fakeChain{MockRPCClient: new(MockRPCClient), head: 10, balances: map[string]*big.Int{holder.Hex(): big.NewInt(5)}}
	}

	t.Run("balance pool", func(t *testing.T) {
		store, sql, ctx := setup(t)
		idx := NewWithConfig(store, chain(), time.Second, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
		idx.balanceWorkers = NewBalanceWorkerPool(store, chain(), 2, 1000)
		refreshBalanceEpoch(ctx, store, idx.balanceWorkers)
		idx.balanceWorkers.Start()
		defer idx.balanceWorkers.Stop()
		require.True(t, idx.balanceWorkers.QueueWork(BalanceWork{Address: holder, TokenAddress: token, BlockNumber: 10}))
		require.Eventually(t, func() bool {
			var b string
			_ = sql.QueryRow(ctx, `SELECT balance::TEXT FROM token_balances_current WHERE address = $1`, lower(holder)).Scan(&b)
			return b == "5"
		}, 20*time.Second, 50*time.Millisecond, "the pool's read is applied")
	})

	t.Run("sync reads", func(t *testing.T) {
		store, sql, ctx := setup(t)
		idx := NewWithConfig(store, chain(), time.Second, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
		require.Nil(t, idx.balanceWorkers)
		idx.trackBalance(ctx, holder, token, 10)
		var b string
		require.NoError(t, sql.QueryRow(ctx, `SELECT balance::TEXT FROM balances WHERE address = $1 AND block_number = 10`,
			lower(holder)).Scan(&b))
		require.Equal(t, "5", b, "the sync read's history row")
	})
}

// counterValue reads a counter from the default registry by name.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

// A request whose attempts ran out is given up, counted in indexer_balance_rereads_abandoned_total and logged.
func TestAnAbandonedRereadIsCounted(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	_, err := sql.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (10, '0xb10', '0xb9', 1700000600, 0, 1, 0);
		INSERT INTO balance_rereads (token_address, address, epoch, attempts)
		VALUES ('0x00000000000000000000000000000000000070c3', '0x00000000000000000000000000000000000000a3',
		        nextval('balance_reorg_epoch'), 20)`)
	require.NoError(t, err)
	idx := NewWithConfig(store, &fakeChain{MockRPCClient: new(MockRPCClient), head: 10}, time.Second, 0,
		&Config{RPCWorkers: 1, RPCRateLimit: 100})
	before := counterValue(t, "indexer_balance_rereads_abandoned_total")
	idx.rereadBalances(ctx, store)
	require.Equal(t, before+1, counterValue(t, "indexer_balance_rereads_abandoned_total"))
	var open bool
	require.NoError(t, sql.QueryRow(ctx, `SELECT open FROM balance_rereads`).Scan(&open))
	require.False(t, open, "closed; its epoch still drops reads decided before the revert")
}
