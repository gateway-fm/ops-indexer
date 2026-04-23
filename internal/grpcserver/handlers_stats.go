package grpcserver

import (
	"context"
	"log/slog"
	"strconv"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
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
	// Map bucket enum -> seconds.
	intervalSec := 3600 // default HOUR
	switch req.GetBucket() {
	case indexerv1.TimeBucket_TIME_BUCKET_DAY:
		intervalSec = 86400
	case indexerv1.TimeBucket_TIME_BUCKET_WEEK:
		intervalSec = 86400 * 7
	}

	// TODO: honor req.GetRange(). Current db.GetTransactionHistory uses limit
	// (recent N buckets), not an absolute range. Fold this into a follow-up
	// DB method that takes (from, to).
	limit := 168 // ~1 week of hourly buckets by default

	points, err := s.db.GetTransactionHistory(ctx, intervalSec, limit)
	if err != nil {
		slog.Error("GetTransactionHistory", "error", err)
		return nil, internalErr(err, "GetTransactionHistory")
	}
	return mapTxHistory(points), nil
}

func (s *Server) GetGasPrices(ctx context.Context, req *indexerv1.GetGasPricesRequest) (*indexerv1.GasPrices, error) {
	numBlocks := int(req.GetSampleBlockCount())
	if numBlocks <= 0 {
		numBlocks = 20
	}
	pcts, err := s.db.GetGasPercentiles(ctx, numBlocks, 25, 50, 75)
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
