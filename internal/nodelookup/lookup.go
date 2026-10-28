// Package nodelookup serves pruned blocks and transactions from the node: what retention removed from Postgres the
// node still answers by number and hash (PRST-5375).
package nodelookup

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// ErrBusy: the lookup's node concurrency was taken for the whole request timeout.
var ErrBusy = errors.New("node lookup: busy")

// ErrReceiptsUnavailable: the node returned the block but not a receipt for each of its transactions (null, or a
// partial list). The block is not served or cached: without its receipt a transaction's status, gas used, contract
// address and categories are unknown.
var ErrReceiptsUnavailable = errors.New("node lookup: block receipts unavailable")

// Source is the node, as the lookup reads it. A missing block or transaction is (nil, nil) or found = false.
type Source interface {
	BlockByNumber(ctx context.Context, number uint64) (*rpc.RawBlock, error)
	BlockReceipts(ctx context.Context, number uint64) ([]*rpclient.Receipt, error)
	BlockNumberByHash(ctx context.Context, hash string) (number uint64, found bool, err error)
	TxBlockNumber(ctx context.Context, hash string) (number uint64, found bool, err error)
}

type Config struct {
	MaxBlocks   int
	MaxBytes    int
	Timeout     time.Duration
	Concurrency int
}

func (c Config) withDefaults() Config {
	if c.MaxBlocks <= 0 {
		c.MaxBlocks = 2_000
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 256 << 20
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	return c
}

// Block is a node block mapped to the types the database serves.
type Block struct {
	Block *types.Block
	Txs   []types.Transaction
	size  int
}

// Lookup fetches whole blocks (block with transactions, and its receipts) and keeps them in an LRU bounded by
// entries and bytes; finalized blocks never change, so nothing expires. Concurrent requests for one block share a
// fetch, and at most Concurrency fetches reach the node at once.
type Lookup struct {
	src   Source
	cfg   Config
	slots chan struct{}
	group singleflight.Group

	mu        sync.Mutex
	order     *list.List
	byNumber  map[uint64]*list.Element
	bytes     int
	txBlock   map[string]uint64
	hashBlock map[string]uint64
}

func New(src Source, cfg Config) *Lookup {
	cfg = cfg.withDefaults()
	return &Lookup{
		src:       src,
		cfg:       cfg,
		slots:     make(chan struct{}, cfg.Concurrency),
		order:     list.New(),
		byNumber:  map[uint64]*list.Element{},
		txBlock:   map[string]uint64{},
		hashBlock: map[string]uint64{},
	}
}

// BlockByNumber returns the node's block, or nil when the node does not have it.
func (l *Lookup) BlockByNumber(ctx context.Context, number uint64) (*Block, error) {
	if b := l.cached(number); b != nil {
		metrics.NodeLookupCache(true)
		return b, nil
	}
	metrics.NodeLookupCache(false)
	v, err, _ := l.group.Do(fmt.Sprint(number), func() (any, error) {
		return l.fetch(context.WithoutCancel(ctx), number)
	})
	if err != nil {
		return nil, err
	}
	return v.(*Block), nil
}

// BlockByHash returns the node's block if it is at or below through, else nil.
func (l *Lookup) BlockByHash(ctx context.Context, hash string, through uint64) (*Block, error) {
	hash = strings.ToLower(hash)
	number, found, err := l.resolve(ctx, hash, l.hashBlock, l.src.BlockNumberByHash, "eth_getBlockByHash")
	if err != nil || !found || number > through {
		return nil, err
	}
	return l.BlockByNumber(ctx, number)
}

// Transaction returns the node's transaction if its block is at or below through, else nil.
func (l *Lookup) Transaction(ctx context.Context, hash string, through uint64) (*types.Transaction, error) {
	hash = strings.ToLower(hash)
	number, found, err := l.resolve(ctx, hash, l.txBlock, l.src.TxBlockNumber, "eth_getTransactionByHash")
	if err != nil || !found || number > through {
		return nil, err
	}
	b, err := l.BlockByNumber(ctx, number)
	if err != nil || b == nil {
		return nil, err
	}
	for i := range b.Txs {
		if strings.EqualFold(b.Txs[i].Hash, hash) {
			return &b.Txs[i], nil
		}
	}
	return nil, nil
}

func (l *Lookup) resolve(ctx context.Context, hash string, index map[string]uint64,
	ask func(context.Context, string) (uint64, bool, error), method string) (uint64, bool, error) {
	l.mu.Lock()
	number, ok := index[hash]
	l.mu.Unlock()
	if ok {
		return number, true, nil
	}
	var found bool
	err := l.withSlot(ctx, func(ctx context.Context) error {
		start := time.Now()
		var err error
		number, found, err = ask(ctx, hash)
		metrics.ObserveNodeLookupRPC(method, time.Since(start), err)
		return err
	})
	return number, found, err
}

func (l *Lookup) fetch(ctx context.Context, number uint64) (*Block, error) {
	var raw *rpc.RawBlock
	var receipts []*rpclient.Receipt
	err := l.withSlot(ctx, func(ctx context.Context) error {
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			start := time.Now()
			var err error
			raw, err = l.src.BlockByNumber(gctx, number)
			metrics.ObserveNodeLookupRPC("eth_getBlockByNumber", time.Since(start), err)
			return err
		})
		g.Go(func() error {
			start := time.Now()
			var err error
			receipts, err = l.src.BlockReceipts(gctx, number)
			metrics.ObserveNodeLookupRPC("eth_getBlockReceipts", time.Since(start), err)
			return err
		})
		return g.Wait()
	})
	if err != nil || raw == nil {
		return nil, err
	}
	if missing := missingReceipts(raw, receipts); missing > 0 {
		return nil, fmt.Errorf("%w: block %d, %d of %d transactions without a receipt",
			ErrReceiptsUnavailable, number, missing, len(raw.Transactions))
	}
	b := MapBlock(raw, receipts)
	l.store(b)
	return b, nil
}

func missingReceipts(raw *rpc.RawBlock, receipts []*rpclient.Receipt) int {
	have := make(map[common.Hash]bool, len(receipts))
	for _, r := range receipts {
		if r != nil {
			have[r.TxHash] = true
		}
	}
	missing := 0
	for _, tx := range raw.Transactions {
		if !have[tx.Hash] {
			missing++
		}
	}
	return missing
}

// withSlot runs fn with one of the node slots, within the request timeout; a slot not free in that time is ErrBusy.
func (l *Lookup) withSlot(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cancel()
	select {
	case l.slots <- struct{}{}:
	case <-ctx.Done():
		metrics.NodeLookupBusy()
		return ErrBusy
	}
	defer func() { <-l.slots }()
	return fn(ctx)
}

func (l *Lookup) cached(number uint64) *Block {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byNumber[number]
	if !ok {
		return nil
	}
	l.order.MoveToFront(e)
	return e.Value.(*Block)
}

func (l *Lookup) store(b *Block) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.byNumber[b.Block.Number]; ok {
		return
	}
	l.byNumber[b.Block.Number] = l.order.PushFront(b)
	l.bytes += b.size
	l.hashBlock[strings.ToLower(b.Block.Hash)] = b.Block.Number
	for i := range b.Txs {
		l.txBlock[strings.ToLower(b.Txs[i].Hash)] = b.Block.Number
	}
	for l.order.Len() > l.cfg.MaxBlocks || (l.bytes > l.cfg.MaxBytes && l.order.Len() > 1) {
		l.evict(l.order.Back())
	}
	metrics.SetNodeLookupCache(l.order.Len(), l.bytes)
}

func (l *Lookup) evict(e *list.Element) {
	b := l.order.Remove(e).(*Block)
	delete(l.byNumber, b.Block.Number)
	l.bytes -= b.size
	delete(l.hashBlock, strings.ToLower(b.Block.Hash))
	for i := range b.Txs {
		delete(l.txBlock, strings.ToLower(b.Txs[i].Hash))
	}
}
