package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainChecksPassOnKnownGoodAndFailOnEachSabotage(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 14_000, busy: 1_000,
		balances: map[string]*big.Int{chainRecipient.Hex(): big.NewInt(3500)}}
	idx := newTestIndexer(store, chain)
	ingestChain(t, idx, store, sql, chain)
	require.NoError(t, store.Migrate(), "the startup re-seed counts the filler blocks")
	_, err := sql.Exec(ctx, `
		INSERT INTO token_balances_current (token_address, address, balance, block_number)
		VALUES (lower($1), lower($2), 3500, 998)`, chainToken.Hex(), chainRecipient.Hex())
	require.NoError(t, err)
	_, err = sql.Exec(ctx, `UPDATE ledger_verify_state SET value = 1 WHERE name = 'running_cycles'`)
	require.NoError(t, err)
	checker := idx.newChainChecker(store, ChainCheckConfig{RPS: 1_000_000, Sample: 50})

	r, err := checker.pass(ctx)
	require.NoError(t, err)
	require.Equal(t, ChainCheckResult{BlocksApplicable: true, SupplyChecked: 1, BalancesChecked: 1, NoncesChecked: 1,
		SpanChecked: true}, r, "known good: blocks = head + 1, supply, balance, nonce and the span's transactions agree")

	exec := func(q string, args ...any) {
		_, err := sql.Exec(ctx, q, args...)
		require.NoError(t, err)
	}
	exec(`DELETE FROM blocks WHERE number = 0`)
	exec(`UPDATE chain_counters SET count = count - 1 WHERE name = 'blocks_total'`)
	r, err = checker.pass(ctx)
	require.NoError(t, err)
	require.Zero(t, r.BlocksDrift, "a database without block 0 (ingest never stores it) counts from block 1")
	require.Equal(t, 1, r.NoncesChecked, "and still counts every sent transaction from genesis")
	exec(`UPDATE chain_counters SET count = count + 1 WHERE name = 'blocks_total'`)
	exec(`UPDATE tokens SET total_supply = total_supply + 1`)
	exec(`UPDATE token_balances_current SET balance = balance - 1`)
	exec(`UPDATE address_stats SET sent_count = sent_count - 1 WHERE address = lower($1)`, chainSender.Hex())
	exec(`UPDATE ledger_chain SET value = value + 1 WHERE lo = 0 AND name = 'transactions_total'`)

	r, err = checker.pass(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), r.BlocksDrift, "blocks_total off by one")
	require.Equal(t, 1, r.SupplyDrifted, "total_supply off by one wei")
	require.Equal(t, 0, r.BalancesDrifted, "a balance that differs once may be a read ahead of the head")
	require.Equal(t, 1, r.NoncesDrifted, "sent_count off by one")
	require.True(t, r.SpanDrifted, "a span's transactions off by one")

	r, err = checker.pass(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.BalancesDrifted, "the same stored balance differs again")
	var state string
	require.NoError(t, sql.QueryRow(ctx, `SELECT chain_state FROM ledger_spans WHERE lo = 0`).Scan(&state))
	require.Equal(t, "drift", state)
	require.True(t, strings.HasPrefix(checker.lastToken, "0x"), "the supply check walks the tokens")
}
