package indexer

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
				VALUES ('`+lower(token)+`', '`+lower(holder)+`', 6, 11);
				INSERT INTO balance_rereads (token_address, address, seq)
				VALUES ('`+lower(token)+`', '`+lower(holder)+`', nextval('balance_reread_seq'))`)
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
				_ = sql.QueryRow(ctx, `SELECT count(*) FROM balance_rereads`).Scan(&open)
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
