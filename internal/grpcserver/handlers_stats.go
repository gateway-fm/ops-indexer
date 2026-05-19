package grpcserver

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func (s *Server) GetChainStats(ctx context.Context, _ *indexerv1.Empty) (*indexerv1.ChainStats, error) {
	c, err := s.db.GetChainStats(ctx)
	if err != nil {
		slog.Error("GetChainStats", "error", err)
		return nil, internalErr(err, "GetChainStats")
	}
	if c == nil {
		return &indexerv1.ChainStats{}, nil
	}
	// Enrich with latest block number for convenience.
	resp := mapChainStats(c)
	if n, e := s.db.GetLatestBlockNumber(ctx); e == nil {
		resp.LatestBlockNumber = n
	}
	return resp, nil
}

func (s *Server) GetTransactionHistory(ctx context.Context, req *indexerv1.GetTransactionHistoryRequest) (*indexerv1.TransactionHistory, error) {
	// DAY/WEEK serve from the pre-aggregated daily_stats table; HOUR uses the
	// windowed raw query since daily_stats has no sub-day granularity.
	// TODO: honor req.GetRange() once the indexerclient sends it.
	switch req.GetBucket() {
	case indexerv1.TimeBucket_TIME_BUCKET_DAY:
		return s.txHistoryFromDailyStats(ctx, 30, 86400)
	case indexerv1.TimeBucket_TIME_BUCKET_WEEK:
		return s.txHistoryFromDailyStats(ctx, 52*7, 86400*7)
	}

	points, err := s.db.GetTransactionHistory(ctx, 3600, 168)
	if err != nil {
		slog.Error("GetTransactionHistory", "error", err)
		return nil, internalErr(err, "GetTransactionHistory")
	}
	return mapTxHistory(points), nil
}

// txHistoryFromDailyStats serves DAY/WEEK buckets from daily_stats. WEEK rolls
// daily rows into 7-day windows in Go (≤365 rows, cheap).
func (s *Server) txHistoryFromDailyStats(ctx context.Context, days int, bucketSec int) (*indexerv1.TransactionHistory, error) {
	to := time.Now().UTC().Truncate(24 * time.Hour)
	from := to.AddDate(0, 0, -days)
	rows, err := s.db.GetDailyStats(ctx, from, to)
	if err != nil {
		slog.Error("GetTransactionHistory (daily_stats)", "error", err)
		return nil, internalErr(err, "GetTransactionHistory")
	}

	pts := make([]types.TxHistoryPoint, 0, len(rows))
	if bucketSec == 86400 {
		for _, r := range rows {
			t, perr := time.Parse("2006-01-02", r.Date)
			if perr != nil {
				continue
			}
			pts = append(pts, types.TxHistoryPoint{
				Timestamp: uint64(t.Unix()),
				Count:     int64(r.TotalTransactions),
			})
		}
	} else {
		type acc struct {
			start time.Time
			sum   int64
		}
		buckets := map[int64]*acc{}
		var order []int64
		for _, r := range rows {
			t, perr := time.Parse("2006-01-02", r.Date)
			if perr != nil {
				continue
			}
			wd := int(t.Weekday())
			if wd == 0 {
				wd = 7
			}
			weekStart := t.AddDate(0, 0, -(wd - 1))
			k := weekStart.Unix()
			a, ok := buckets[k]
			if !ok {
				a = &acc{start: weekStart}
				buckets[k] = a
				order = append(order, k)
			}
			a.sum += int64(r.TotalTransactions)
		}
		for _, k := range order {
			a := buckets[k]
			pts = append(pts, types.TxHistoryPoint{
				Timestamp: uint64(a.start.Unix()),
				Count:     a.sum,
			})
		}
	}
	return mapTxHistory(pts), nil
}

func (s *Server) GetGasPrices(ctx context.Context, req *indexerv1.GetGasPricesRequest) (*indexerv1.GasPrices, error) {
	numBlocks := int(req.GetSampleBlockCount())
	if numBlocks <= 0 {
		numBlocks = 20
	}
	// percentile_cont expects 0..1 fractions, not percentile integers.
	pcts, err := s.db.GetGasPercentiles(ctx, numBlocks, 0.25, 0.50, 0.75)
	if err != nil {
		slog.Error("GetGasPrices", "error", err)
		return nil, internalErr(err, "GetGasPrices")
	}
	if pcts == nil {
		return &indexerv1.GasPrices{}, nil
	}
	return &indexerv1.GasPrices{
		Slow:             gasPtrToBig(pcts.SlowWei),
		Normal:           gasPtrToBig(pcts.NormalWei),
		Fast:             gasPtrToBig(pcts.FastWei),
		BaseFee:          gasPtrToBig(pcts.BaseFee),
		SampleBlockCount: uint64(numBlocks),
	}, nil
}

func gasPtrToBig(u *uint64) *indexerv1.BigInt {
	if u == nil {
		return &indexerv1.BigInt{}
	}
	return bigIntFromString(strconv.FormatUint(*u, 10))
}

func (s *Server) GetDailyStats(ctx context.Context, req *indexerv1.GetDailyStatsRequest) (*indexerv1.GetDailyStatsResponse, error) {
	from, err := parseDate(req.GetFromDate())
	if err != nil {
		return nil, invalidArgument("from_date: %v", err)
	}
	to, err := parseDate(req.GetToDate())
	if err != nil {
		return nil, invalidArgument("to_date: %v", err)
	}
	rows, err := s.db.GetDailyStats(ctx, from, to)
	if err != nil {
		slog.Error("GetDailyStats", "error", err)
		return nil, internalErr(err, "GetDailyStats")
	}
	return &indexerv1.GetDailyStatsResponse{Days: mapDailyStats(rows)}, nil
}
