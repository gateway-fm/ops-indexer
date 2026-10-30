package indexer

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// balanceRereadStore is where a reorg revert records the balances to read again at the new head.
type balanceRereadStore interface {
	ClaimBalanceRereads(ctx context.Context, limit int) (claimed, dropped []db.BalanceReread, err error)
	CurrentBalanceEpoch(ctx context.Context) (int64, error)
}

// loadBalanceEpoch sets the balance pool's reorg epoch from the database at start. Without it every read of a key a
// revert ever touched would be dropped for the whole run, so a failure fails the start.
func loadBalanceEpoch(ctx context.Context, d Database, pool *BalanceWorkerPool) error {
	store, ok := d.(balanceRereadStore)
	if !ok || pool == nil {
		return nil
	}
	e, err := store.CurrentBalanceEpoch(ctx)
	if err != nil {
		return fmt.Errorf("read the balance reorg epoch: %w", err)
	}
	pool.SetEpoch(e)
	return nil
}

// epochRetry is the first wait before the reorg epoch is read again after a failed read; it doubles to a minute.
var epochRetry = time.Second

// holdBalanceEpoch makes the balance pool's new work wait for its stamp for the length of a revert, and returns the
// release, which reads the revert's epoch, retrying until it is read, and only then lets new work be stamped with
// it. Work stamped meanwhile with the epoch from before the revert would be dropped for every key the revert touched,
// for good.
func holdBalanceEpoch(d Database, pool *BalanceWorkerPool) (release func(ctx context.Context)) {
	store, ok := d.(balanceRereadStore)
	if !ok || pool == nil {
		return func(context.Context) {}
	}
	pool.holdEpoch()
	return func(ctx context.Context) {
		wait := epochRetry
		for {
			e, err := store.CurrentBalanceEpoch(ctx)
			if err == nil {
				pool.releaseEpoch(e)
				return
			}
			if ctx.Err() != nil {
				pool.releaseEpoch(0)
				return
			}
			log.Warn("balance rereads: could not read the reorg epoch after a revert; new balance reads wait for it",
				"retry_in", wait, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
			wait = min(2*wait, time.Minute)
		}
	}
}

// balanceRereadInterval is how often the open requests are handed to the balance reads.
var balanceRereadInterval = 5 * time.Second

// RunBalanceRereads reads again, at the stored head, every balance a reorg revert could not restore with certainty.
// Each read carries the key's reorg epoch, so it is not taken for a read decided before the revert, and the first such
// read written closes the request (InsertBalancesBatch). A read that fails leaves the request for the next pass.
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
	metrics.BalanceRereadsAbandoned(len(dropped))
	for _, r := range dropped {
		log.Warn("balance rereads: gave up on a balance a reorg reverted; it stays as restored until its next transfer",
			"token", r.Token, "holder", r.Address, "attempts", db.MaxBalanceRereadAttempts,
			"metric", "indexer_balance_rereads_abandoned_total")
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
				BlockNumber: head, Epoch: r.Epoch}
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
			Balance: types.JSONString(new(big.Int).SetBytes(data).String()), Epoch: r.Epoch})
	}
	if err := i.db.InsertBalancesBatch(ctx, balances); err != nil {
		log.Warn("balance rereads: could not write the balances read again; retrying", "error", err)
	}
}
