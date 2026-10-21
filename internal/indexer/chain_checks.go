package indexer

import (
	"context"
	"math/big"
	"time"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

var (
	totalSupplySelector = common.FromHex("0x18160ddd")
	balanceOfSelector   = common.FromHex("0x70a08231")
)

const txCountBatch = 100

type chainCheckStore interface {
	CheckBlocksTotal(ctx context.Context) (db.BlocksCheck, error)
	NextTokenSupply(ctx context.Context, after string) (db.TokenSupply, bool, error)
	SampleSettledBalances(ctx context.Context, n int) ([]db.BalanceSample, error)
	SampleSenders(ctx context.Context, n int) ([]db.SenderSample, error)
	NextTxCountSpan(ctx context.Context) (lo, hi, txs int64, ok bool, err error)
	RecordTxCountCheck(ctx context.Context, lo int64, agrees bool) error
}

type ChainCheckConfig struct {
	// RPS bounds every node call of the chain checks; Sample is the balance and sender sample per pass.
	RPS      int
	Sample   int
	Interval time.Duration
}

// ChainCheckResult is one pass: each check's compared and differing counts. The chain checks alert; they repair
// nothing (a token whose supply moves without Transfer events differs from its event-derived supply legitimately).
type ChainCheckResult struct {
	BlocksDrift                      int64
	BlocksApplicable                 bool
	SupplyChecked, SupplyDrifted     int
	BalancesChecked, BalancesDrifted int
	NoncesChecked, NoncesDrifted     int
	SpanChecked, SpanDrifted         bool
}

type chainChecker struct {
	idx       *Indexer
	store     chainCheckStore
	limiter   *rate.Limiter
	sample    int
	lastToken string
	suspects  map[string]*big.Int
}

func (i *Indexer) newChainChecker(store chainCheckStore, cfg ChainCheckConfig) *chainChecker {
	return &chainChecker{idx: i, store: store, limiter: rate.NewLimiter(rate.Limit(cfg.RPS), max(cfg.RPS, txCountBatch)),
		sample: cfg.Sample, suspects: map[string]*big.Int{}}
}

// pass runs every chain check once.
func (c *chainChecker) pass(ctx context.Context) (ChainCheckResult, error) {
	var r ChainCheckResult
	blocks, err := c.store.CheckBlocksTotal(ctx)
	if err != nil {
		return r, err
	}
	r.BlocksApplicable = blocks.Applicable
	first := int64(c.idx.startBlock)
	if blocks.FromGenesis {
		first = 0
	}
	if blocks.Applicable && blocks.Head >= 0 {
		r.BlocksDrift = blocks.Total - (blocks.Head + 1 - first)
	}
	if err := c.checkSupply(ctx, &r); err != nil {
		return r, err
	}
	if err := c.checkBalances(ctx, &r); err != nil {
		return r, err
	}
	if first == 0 && blocks.Applicable {
		if err := c.checkNonces(ctx, &r); err != nil {
			return r, err
		}
	}
	return r, c.checkSpanTxCount(ctx, &r)
}

func (c *chainChecker) call(ctx context.Context, to common.Address, data []byte, block uint64) (*big.Int, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	out, err := c.idx.rpc.CallContractAt(ctx, to, data, block)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(out), nil
}

func (c *chainChecker) checkSupply(ctx context.Context, r *ChainCheckResult) error {
	s, ok, err := c.store.NextTokenSupply(ctx, c.lastToken)
	if err != nil || !ok {
		return err
	}
	c.lastToken = s.Token
	onChain, err := c.call(ctx, common.HexToAddress(s.Token), totalSupplySelector, s.Head)
	if err != nil {
		log.Debug("chain check: totalSupply() unavailable", "token", s.Token, "error", err)
		return nil
	}
	r.SupplyChecked++
	if onChain.Cmp(s.Supply) != 0 {
		r.SupplyDrifted++
		log.Warn("chain check: total_supply differs from totalSupply()", "token", s.Token, "block", s.Head,
			"stored", s.Supply, "node", onChain)
	}
	return nil
}

// checkBalances counts a key as drifted only when it differs on two passes with the same stored value: a balance
// read at latest can briefly run ahead of the stored head.
func (c *chainChecker) checkBalances(ctx context.Context, r *ChainCheckResult) error {
	samples, err := c.store.SampleSettledBalances(ctx, c.sample)
	if err != nil {
		return err
	}
	for _, s := range samples {
		data := append(append([]byte{}, balanceOfSelector...), common.LeftPadBytes(common.HexToAddress(s.Address).Bytes(), 32)...)
		onChain, err := c.call(ctx, common.HexToAddress(s.Token), data, s.Head)
		if err != nil {
			continue
		}
		r.BalancesChecked++
		key := s.Token + "/" + s.Address
		if onChain.Cmp(s.Balance) == 0 {
			delete(c.suspects, key)
			continue
		}
		if prior, seen := c.suspects[key]; seen && prior.Cmp(s.Balance) == 0 {
			r.BalancesDrifted++
			log.Warn("chain check: current balance differs from balanceOf()", "token", s.Token, "address", s.Address,
				"block", s.Head, "stored", s.Balance, "node", onChain)
			continue
		}
		c.suspects[key] = s.Balance
	}
	return nil
}

func (c *chainChecker) checkNonces(ctx context.Context, r *ChainCheckResult) error {
	samples, err := c.store.SampleSenders(ctx, c.sample)
	if err != nil {
		return err
	}
	for _, s := range samples {
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		nonce, err := c.idx.rpc.TransactionCountAt(ctx, common.HexToAddress(s.Address), s.Head)
		if err != nil {
			continue
		}
		r.NoncesChecked++
		if nonce != s.Sent {
			r.NoncesDrifted++
			log.Warn("chain check: sent_count differs from eth_getTransactionCount", "address", s.Address,
				"block", s.Head, "stored", s.Sent, "node", nonce)
		}
	}
	return nil
}

func (c *chainChecker) checkSpanTxCount(ctx context.Context, r *ChainCheckResult) error {
	lo, hi, txs, ok, err := c.store.NextTxCountSpan(ctx)
	if err != nil || !ok {
		return err
	}
	var onChain uint64
	for from := uint64(lo); from < uint64(hi); from += txCountBatch {
		to := min(from+txCountBatch, uint64(hi))
		if err := c.limiter.WaitN(ctx, int(to-from)); err != nil {
			return err
		}
		n, err := c.idx.rpc.BlockTransactionCounts(ctx, from, to, txCountBatch)
		if err != nil {
			return err
		}
		onChain += n
	}
	r.SpanChecked = true
	r.SpanDrifted = int64(onChain) != txs
	if r.SpanDrifted {
		log.Warn("chain check: a span's transactions differ from the node's block counts", "lo", lo, "hi", hi,
			"ledger", txs, "node", onChain)
	}
	return c.store.RecordTxCountCheck(ctx, lo, !r.SpanDrifted)
}

// RunChainChecks runs a pass of the chain checks per interval within cfg.RPS, retention on or off.
func (i *Indexer) RunChainChecks(ctx context.Context, store chainCheckStore, cfg ChainCheckConfig) {
	log.Info("chain checks started", "rps", cfg.RPS, "sample", cfg.Sample, "interval", cfg.Interval)
	c := i.newChainChecker(store, cfg)
	for ctx.Err() == nil {
		r, err := c.pass(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("chain checks: pass failed", "error", err)
		}
		if r.BlocksApplicable {
			metrics.VerifyChecked("chain_blocks", int(abs(r.BlocksDrift)), "")
		}
		metrics.VerifyCheckedN("chain_supply", r.SupplyChecked, r.SupplyDrifted)
		metrics.VerifyCheckedN("chain_balance", r.BalancesChecked, r.BalancesDrifted)
		metrics.VerifyCheckedN("chain_nonce", r.NoncesChecked, r.NoncesDrifted)
		if r.SpanChecked {
			metrics.VerifyCheckedN("chain_span_txs", 1, boolCount(r.SpanDrifted))
		}
		select {
		case <-ctx.Done():
		case <-time.After(cfg.Interval):
		}
	}
}

func boolCount(b bool) int {
	if b {
		return 1
	}
	return 0
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
