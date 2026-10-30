//go:build faults

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

var fkBalanceQueries = []string{
	`SELECT address, token_address, block_number, balance FROM balances`,
	`SELECT token_address, address, balance, block_number FROM token_balances_current`,
	`SELECT address, holder_count FROM tokens`,
	`SELECT token_address, address FROM balance_rereads`,
}

// fkBalanceChain stores blocks 1..3 of transfers and their balances, block 3's reads of B and X not yet written.
func fkBalanceChain(t *testing.T, d *DB) {
	t.Helper()
	ctx := context.Background()
	id := "1"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil),
		reorgBalTransfer(reorgBalNFT, types.TokenTypeERC721, zeroAddress, reorgBalO, "1", &id))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1,
		[3]string{reorgBalA, reorgBalToken, "10"}, [3]string{reorgBalO, reorgBalNFT, "1"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2,
		[3]string{reorgBalA, reorgBalToken, "6"}, [3]string{reorgBalB, reorgBalToken, "4"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(3,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalB, reorgBalX, "1", nil),
		reorgBalTransfer(reorgBalNFT, types.TokenTypeERC721, reorgBalO, reorgBalX, "1", &id))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(3, [3]string{reorgBalO, reorgBalNFT, "0"})))
}

// fkAnswer writes the node's balances after the reorg for every open request, as the reread loop does.
func fkAnswer(t *testing.T, d *DB) error {
	t.Helper()
	node := map[string]string{
		reorgBalToken + " " + reorgBalA: "10", reorgBalToken + " " + reorgBalB: "0", reorgBalToken + " " + reorgBalX: "0",
		reorgBalNFT + " " + reorgBalO: "1", reorgBalNFT + " " + reorgBalX: "0",
	}
	var answers []*types.Balance
	for k, seq := range rereads(t, d) {
		b := reorgBalances(1, [3]string{k[len(reorgBalToken)+1:], k[:len(reorgBalToken)], node[k]})[0]
		b.Reread = seq
		answers = append(answers, b)
	}
	return d.InsertBalancesBatch(context.Background(), answers)
}

// A fault on each statement the balance revert and the reread add: the revert of blocks 3 and 2, resumed after a
// restart from the stored head, and the rereads answered, end as a revert without the fault.
func TestFaultBalanceRevertFaults(t *testing.T) {
	for _, step := range []struct{ name, match string }{
		{"keys", "bool_or(had_row)"},
		{"delete-history", "DELETE FROM balances WHERE block_number"},
		{"restore", "prev AS ("},
		{"requests", "INSERT INTO balance_rereads"},
		{"answer-current", "SET block_number = k.block_number"},
		{"answer-clear", "DELETE FROM balance_rereads r USING"},
	} {
		for _, act := range []struct {
			name string
			a    faultkit.Action
		}{{"lost-ack", faultkit.LostAck}, {"fail-commit", faultkit.FailCommit}, {"kill-before", faultkit.KillBefore}} {
			t.Run(step.name+"/"+act.name, func(t *testing.T) {
				ctx := context.Background()
				ref, _, _ := fkDB(t)
				d, p, url := fkDB(t)
				fkBalanceChain(t, ref)
				fkBalanceChain(t, d)
				for _, n := range []uint64{3, 2} {
					require.NoError(t, ref.DeleteBlock(ctx, n))
				}
				require.NoError(t, fkAnswer(t, ref))

				r := p.Arm(faultkit.Rule{Match: step.match, Action: act.a})
				x := d
				for _, n := range []uint64{3, 2} {
					if err := x.DeleteBlock(ctx, n); err != nil {
						x = fkRestart(t, url)
						var head int64
						require.NoError(t, x.pool.QueryRow(ctx, `SELECT max(number) FROM blocks`).Scan(&head))
						for m := uint64(head); m >= n; m-- {
							require.NoError(t, x.DeleteBlock(ctx, m))
						}
					}
				}
				if err := fkAnswer(t, x); err != nil {
					x = fkRestart(t, url)
					require.NoError(t, fkAnswer(t, x))
				}
				require.Equal(t, 1, p.Fired(r), "the fault fired")
				fkSame(t, ref, fkRestart(t, url), fkBalanceQueries)
				require.Empty(t, rereads(t, fkRestart(t, url)), "every request answered")
			})
		}
	}
}
