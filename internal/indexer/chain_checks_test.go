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

// A restart of a deployment indexed from genesis with retention raises the ingest start above the prune watermark.
// The chain checks still count from the configured start: blocks_total against every block from genesis, and the
// nonce sample, which needs every sent transaction counted.
func TestTheChainChecksCountFromTheConfiguredStartAfterPruning(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 14_000, busy: 1_000}
	idx := newTestIndexer(store, chain)
	ingestChain(t, idx, store, sql, chain)
	require.NoError(t, store.Migrate(), "the startup re-seed counts the filler blocks")
	_, err := sql.Exec(ctx, `UPDATE prune_state SET pruned_through = 999;
		UPDATE ledger_verify_state SET value = 1 WHERE name = 'running_cycles'`)
	require.NoError(t, err)
	idx.startBlock = 1000 // what Start does above the watermark
	r, err := idx.newChainChecker(store, ChainCheckConfig{RPS: 1_000_000, Sample: 50}).pass(ctx)
	require.NoError(t, err)
	require.Zero(t, r.BlocksDrift)
	require.Equal(t, 1, r.NoncesChecked, "the nonce sample still runs")
}

// Block 0 stored and counted, then span 0 pruned with it: the blocks check still counts from genesis, as span 0's
// ledger did, instead of finding blocks_total one above the head.
func TestTheBlocksCheckCountsGenesisAfterItsSpanIsPruned(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 14_000, busy: 1_000}
	idx := newTestIndexer(store, chain)
	ingestChain(t, idx, store, sql, chain)
	require.NoError(t, store.Migrate(), "the startup re-seed counts the filler blocks")
	_, err := sql.Exec(ctx, `UPDATE prune_state SET pruned_through = 999; DELETE FROM blocks WHERE number < 1000`)
	require.NoError(t, err, "span 0 pruned, block 0 with it")
	checker := idx.newChainChecker(store, ChainCheckConfig{RPS: 1_000_000, Sample: 50})
	r, err := checker.pass(ctx)
	require.NoError(t, err)
	require.True(t, r.BlocksApplicable)
	require.Zero(t, r.BlocksDrift)
}
