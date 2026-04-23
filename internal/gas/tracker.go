package gas

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

type GasPrice struct {
	Price       float64  `json:"price"`       // Gwei
	PriceWei    string   `json:"priceWei"`    // Wei (string for precision)
	BaseFee     *float64 `json:"baseFee"`     // Base fee (Gwei) for EIP-1559
	PriorityFee *float64 `json:"priorityFee"` // Priority fee (Gwei) for EIP-1559
}

type GasPrices struct {
	Slow      *GasPrice `json:"slow"`
	Normal    *GasPrice `json:"normal"`
	Fast      *GasPrice `json:"fast"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Database interface {
	GetGasPercentiles(ctx context.Context, numBlocks int, slowPct, avgPct, fastPct float64) (*db.GasPercentiles, error)
}

type Config struct {
	CacheTTL        time.Duration // Default: 30s
	NumBlocks       int           // Default: 200
	SlowPercentile  int           // Default: 35
	AvgPercentile   int           // Default: 60
	FastPercentile  int           // Default: 90
}

func DefaultConfig() *Config {
	return &Config{
		CacheTTL:       30 * time.Second,
		NumBlocks:      200,
		SlowPercentile: 35,
		AvgPercentile:  60,
		FastPercentile: 90,
	}
}

type Tracker struct {
	db             Database
	mu             sync.RWMutex
	cached         *GasPrices
	cacheTTL       time.Duration
	numBlocks      int
	slowPercentile int
	avgPercentile  int
	fastPercentile int
}

func NewTracker(database Database, cfg *Config) *Tracker {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Tracker{
		db:             database,
		cacheTTL:       cfg.CacheTTL,
		numBlocks:      cfg.NumBlocks,
		slowPercentile: cfg.SlowPercentile,
		avgPercentile:  cfg.AvgPercentile,
		fastPercentile: cfg.FastPercentile,
	}
}

func (t *Tracker) GetGasPrices(ctx context.Context) (*GasPrices, error) {
	t.mu.RLock()
	if t.cached != nil && time.Since(t.cached.UpdatedAt) < t.cacheTTL {
		defer t.mu.RUnlock()
		return t.cached, nil
	}
	t.mu.RUnlock()

	prices, err := t.calculate(ctx)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.cached = prices
	t.mu.Unlock()

	return prices, nil
}

func (t *Tracker) calculate(ctx context.Context) (*GasPrices, error) {
	percentiles, err := t.db.GetGasPercentiles(
		ctx,
		t.numBlocks,
		float64(t.slowPercentile)/100,
		float64(t.avgPercentile)/100,
		float64(t.fastPercentile)/100,
	)
	if err != nil {
		return nil, err
	}

	prices := &GasPrices{
		UpdatedAt: time.Now(),
	}

	weiToGwei := func(wei *uint64) float64 {
		if wei == nil || *wei == 0 {
			return 0
		}
		return float64(*wei) / 1e9
	}

	var baseFeeGwei *float64
	if percentiles.BaseFee != nil && *percentiles.BaseFee > 0 {
		bf := weiToGwei(percentiles.BaseFee)
		baseFeeGwei = &bf
	}

	if percentiles.SlowWei != nil && *percentiles.SlowWei > 0 {
		slowGwei := weiToGwei(percentiles.SlowWei)
		var priorityFee *float64
		if baseFeeGwei != nil && slowGwei > *baseFeeGwei {
			pf := slowGwei - *baseFeeGwei
			priorityFee = &pf
		}
		prices.Slow = &GasPrice{
			Price:       slowGwei,
			PriceWei:    fmt.Sprintf("%d", *percentiles.SlowWei),
			BaseFee:     baseFeeGwei,
			PriorityFee: priorityFee,
		}
	}

	if percentiles.NormalWei != nil && *percentiles.NormalWei > 0 {
		normalGwei := weiToGwei(percentiles.NormalWei)
		var priorityFee *float64
		if baseFeeGwei != nil && normalGwei > *baseFeeGwei {
			pf := normalGwei - *baseFeeGwei
			priorityFee = &pf
		}
		prices.Normal = &GasPrice{
			Price:       normalGwei,
			PriceWei:    fmt.Sprintf("%d", *percentiles.NormalWei),
			BaseFee:     baseFeeGwei,
			PriorityFee: priorityFee,
		}
	}

	if percentiles.FastWei != nil && *percentiles.FastWei > 0 {
		fastGwei := weiToGwei(percentiles.FastWei)
		var priorityFee *float64
		if baseFeeGwei != nil && fastGwei > *baseFeeGwei {
			pf := fastGwei - *baseFeeGwei
			priorityFee = &pf
		}
		prices.Fast = &GasPrice{
			Price:       fastGwei,
			PriceWei:    fmt.Sprintf("%d", *percentiles.FastWei),
			BaseFee:     baseFeeGwei,
			PriorityFee: priorityFee,
		}
	}

	return prices, nil
}

func (t *Tracker) StartBackgroundRefresh(ctx context.Context) {
	ticker := time.NewTicker(t.cacheTTL / 2) // Refresh before expiry
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if _, err := t.GetGasPrices(ctx); err != nil {
					log.Error("gas tracker refresh failed", "error", err)
				}
			}
		}
	}()
}
