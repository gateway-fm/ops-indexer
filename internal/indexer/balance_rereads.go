package indexer

import (
	"context"
	"math/big"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// balanceRereadStore is where a reorg revert records the balances to read again at the new head.
type balanceRereadStore interface {
	ClaimBalanceRereads(ctx context.Context, limit int) (claimed, dropped []db.BalanceReread, err error)
}

// balanceRereadInterval is how often the open requests are handed to the balance reads.
const balanceRereadInterval = 5 * time.Second

// RunBalanceRereads reads again, at the stored head, every balance a reorg revert could not restore with certainty.
// Each read carries its request's seq; the write that carries it clears the request (InsertBalancesBatch). A read that
// fails leaves the request for the next pass.
func (i *Indexer) RunBalanceRereads(ctx context.Context, store balanceRereadStore) {
	ticker := time.NewTicker(balanceRereadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		i.rereadBalances(ctx, store)
	}
}

func (i *Indexer) rereadBalances(ctx context.Context, store balanceRereadStore) {
	claimed, dropped, err := store.ClaimBalanceRereads(ctx, 1000)
	if err != nil {
		log.Warn("balance rereads: could not read the open requests; retrying", "error", err)
		return
	}
	for _, r := range dropped {
		log.Warn("balance rereads: gave up on a balance a reorg reverted; it stays as restored until its next transfer",
			"token", r.Token, "holder", r.Address, "attempts", db.MaxBalanceRereadAttempts)
	}
	if len(claimed) == 0 {
		return
	}
	head, err := i.db.GetLatestBlockNumber(ctx)
	if err != nil {
		log.Warn("balance rereads: could not read the stored head; retrying", "error", err)
		return
	}
	if i.balanceWorkers != nil {
		works := make([]BalanceWork, len(claimed))
		for n, r := range claimed {
			works[n] = BalanceWork{Address: common.HexToAddress(r.Address), TokenAddress: common.HexToAddress(r.Token),
				BlockNumber: head, Reread: r.Seq}
		}
		i.balanceWorkers.QueueWorkBatchWait(ctx, works)
		return
	}
	var balances []*types.Balance
	for _, r := range claimed {
		holder := common.HexToAddress(r.Address)
		data, err := i.rpc.CallContract(ctx, common.HexToAddress(r.Token),
			append(common.FromHex("0x70a08231"), common.LeftPadBytes(holder.Bytes(), 32)...))
		if err != nil || len(data) < 32 {
			continue
		}
		balances = append(balances, &types.Balance{Address: r.Address, TokenAddress: r.Token, BlockNumber: head,
			Balance: types.JSONString(new(big.Int).SetBytes(data).String()), Reread: r.Seq})
	}
	if err := i.db.InsertBalancesBatch(ctx, balances); err != nil {
		log.Warn("balance rereads: could not write the balances read again; retrying", "error", err)
	}
}
