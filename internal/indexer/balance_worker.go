package indexer

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

type BalanceWork struct {
	Address      common.Address
	TokenAddress common.Address
	BlockNumber  uint64
}

type BalanceWorkerPool struct {
	db         Database
	rpc        RPCClient
	workChan   chan BalanceWork
	numWorkers int
	rateLimit  int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewBalanceWorkerPool(database Database, rpcClient RPCClient, numWorkers, rateLimit int) *BalanceWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &BalanceWorkerPool{
		db:         database,
		rpc:        rpcClient,
		workChan:   make(chan BalanceWork, 10000), // Buffer for 10k balance requests
		numWorkers: numWorkers,
		rateLimit:  rateLimit,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (p *BalanceWorkerPool) Start() {
	log.Info("starting balance workers", "count", p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func (p *BalanceWorkerPool) Stop() {
	log.Info("stopping balance workers")
	p.cancel()
	close(p.workChan)
	p.wg.Wait()
	log.Info("balance workers stopped")
}

// QueueWork returns false if the queue is full (non-blocking).
func (p *BalanceWorkerPool) QueueWork(work BalanceWork) bool {
	select {
	case p.workChan <- work:
		return true
	default:
		return false
	}
}

func (p *BalanceWorkerPool) QueueWorkBatch(works []BalanceWork) int {
	queued := 0
	for _, work := range works {
		if p.QueueWork(work) {
			queued++
		}
	}
	return queued
}

func (p *BalanceWorkerPool) QueueSize() int {
	return len(p.workChan)
}

func (p *BalanceWorkerPool) worker(id int) {
	defer p.wg.Done()

	batch := make([]*types.Balance, 0, 100)

	for {
		select {
		case <-p.ctx.Done():
			if len(batch) > 0 {
				p.flushBatch(batch)
			}
			return
		case work, ok := <-p.workChan:
			if !ok {
				if len(batch) > 0 {
					p.flushBatch(batch)
				}
				return
			}

			if work.Address == (common.Address{}) {
				continue
			}

			balance, err := p.fetchBalance(work)
			if err != nil {
				continue // Skip on error
			}

			batch = append(batch, balance)

			if len(batch) >= 100 {
				p.flushBatch(batch)
				batch = make([]*types.Balance, 0, 100)
			}
		}
	}
}

func (p *BalanceWorkerPool) fetchBalance(work BalanceWork) (*types.Balance, error) {
	// balanceOf(address) = 0x70a08231
	addrPadded := common.LeftPadBytes(work.Address.Bytes(), 32)
	callData := append(common.FromHex("0x70a08231"), addrPadded...)

	balanceData, err := p.rpc.CallContract(p.ctx, work.TokenAddress, callData)
	if err != nil {
		return nil, err
	}

	if len(balanceData) < 32 {
		return nil, nil
	}

	balance := new(big.Int).SetBytes(balanceData)

	return &types.Balance{
		Address:      work.Address.Hex(),
		TokenAddress: work.TokenAddress.Hex(),
		BlockNumber:  work.BlockNumber,
		Balance:      types.JSONString(balance.String()),
	}, nil
}

func (p *BalanceWorkerPool) flushBatch(batch []*types.Balance) {
	if len(batch) == 0 {
		return
	}

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := p.db.InsertBalancesBatch(p.ctx, batch)
		if err == nil {
			return
		}

		// Check if it's a retryable error (deadlock or serialization failure)
		errStr := err.Error()
		isRetryable := strings.Contains(errStr, "40P01") || // deadlock
			strings.Contains(errStr, "40001") // serialization failure

		if !isRetryable || attempt == maxRetries-1 {
			log.Error("balance worker: failed to insert batch", "count", len(batch), "error", err)
			return
		}

		// Exponential backoff: 10ms, 20ms, 40ms
		time.Sleep(time.Duration(10<<attempt) * time.Millisecond)
	}
}
