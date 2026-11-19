package indexerv2

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxRows       = 20000
	maxField      = 2 << 20
	maxSend       = 16 << 20
	maxSet        = 4096
	maxPredicates = 16384
)

func failure(code codes.Code, reason string, metadata ...map[string]string) error {
	e := &errdetails.ErrorInfo{Domain: "indexer.ops", Reason: reason}
	if len(metadata) > 0 {
		e.Metadata = metadata[0]
	}
	s, _ := status.New(code, "indexed read failed").WithDetails(e)
	return s.Err()
}
func invalid(reason string) error { return failure(codes.InvalidArgument, reason) }
func dataError() error            { return failure(codes.DataLoss, "INCONSISTENT_DATA") }
func budget() error               { return failure(codes.ResourceExhausted, "READ_BUDGET_EXCEEDED") }
func mapError(ctx context.Context, e error) error {
	if e == nil {
		return nil
	}
	if _, ok := status.FromError(e); ok {
		return e
	}
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return failure(codes.DeadlineExceeded, "READ_DEADLINE_EXCEEDED")
		}
		return status.Error(codes.Canceled, "indexed read canceled")
	}
	return failure(codes.Unavailable, "DATASET_UNAVAILABLE")
}
func decodeHex(s string, want int) ([]byte, error) {
	if len(s) > 2*maxField+2 {
		return nil, budget()
	}
	payload := s
	if strings.HasPrefix(s, "0x") {
		payload = s[2:]
	} else if want > 0 {
		return nil, dataError()
	}
	b, e := hex.DecodeString(payload)
	if e != nil || want > 0 && len(b) != want {
		return nil, dataError()
	}
	if len(b) > maxField {
		return nil, budget()
	}
	return b, nil
}
func hexString(b []byte) string { return "0x" + hex.EncodeToString(b) }
func hasUnknown(m protoreflect.Message) bool {
	if len(m.GetUnknown()) != 0 {
		return true
	}
	bad := false
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.IsMap() {
			if f.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool { bad = hasUnknown(v.Message()); return !bad })
			}
		} else if f.IsList() {
			if f.Kind() == protoreflect.MessageKind {
				for i := 0; i < v.List().Len(); i++ {
					if hasUnknown(v.List().Get(i).Message()) {
						bad = true
						break
					}
				}
			}
		} else if f.Kind() == protoreflect.MessageKind {
			bad = hasUnknown(v.Message())
		}
		return !bad
	})
	return bad
}
func validateMessage(m proto.Message) error {
	if m == nil || !m.ProtoReflect().IsValid() || hasUnknown(m.ProtoReflect()) {
		return invalid("INVALID_FILTER")
	}
	if proto.Size(m) > 4<<20 {
		return budget()
	}
	return nil
}

type snapshot struct {
	ctx                                                     context.Context
	tx                                                      pgx.Tx
	cleanup                                                 func()
	cancel                                                  context.CancelFunc
	epoch, chainID                                          string
	genesis                                                 []byte
	anchor                                                  *pb.ReadAt
	head, lowest, history, runFloor, prunedTime, anchorTime uint64
	rows, charged, scanned                                  int
	effectiveRange                                          *pb.BlockRange
	emptyScope                                              bool
}

func (s *snapshot) close() { s.cleanup(); s.cancel() }
func (s *snapshot) commit() error {
	if e := s.tx.Commit(s.ctx); e != nil {
		return mapError(s.ctx, e)
	}
	if s.ctx.Err() != nil {
		return mapError(s.ctx, s.ctx.Err())
	}
	return nil
}
func (s *snapshot) page(c pb.Consistency) *pb.PageInfo {
	return &pb.PageInfo{ReadAt: proto.Clone(s.anchor).(*pb.ReadAt), DatasetEpoch: s.epoch, HistoryFromBlock: s.history, RunFloorBlock: s.runFloor, Consistency: c, EndedAt: pb.EndedAt_ENDED_AT_END_OF_RANGE}
}
func (s *snapshot) charge(m proto.Message) error {
	s.rows++
	s.charged += proto.Size(m)
	if s.rows > maxRows || s.charged > maxSend-4096 {
		return budget()
	}
	return nil
}
func (s *Service) begin(ctx context.Context, request proto.Message, read *pb.ReadContext, page *pb.PageRequest) (*snapshot, error) {
	if e := validateMessage(request); e != nil {
		return nil, e
	}
	if s.db == nil || s.ready != nil && !s.ready() {
		return nil, failure(codes.Unavailable, "DATASET_UNAVAILABLE")
	}
	if page.GetPageSize() < 0 || page.GetOrder() < 0 || page.GetOrder() > pb.Order_ORDER_ASCENDING {
		return nil, invalid("INVALID_FILTER")
	}
	if page.GetStartAfter() != nil && (read.GetReadAt() == nil || read.GetExpectDatasetEpoch() == "" || read.ExpectedRunFloor == nil) {
		return nil, invalid("INVALID_READ_CONTEXT")
	}
	if read.GetReadAt() != nil && (read.ReadAt.BlockNumber > math.MaxInt64 || len(read.ReadAt.BlockHash) != 32) {
		return nil, invalid("INVALID_READ_CONTEXT")
	}
	if read != nil && read.ExpectedRunFloor != nil && (read.ReadAt == nil || *read.ExpectedRunFloor > read.ReadAt.BlockNumber) {
		return nil, invalid("INVALID_READ_CONTEXT")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	tx, cleanup, e := s.db.BeginHistorySnapshotWithLimits(bounded, time.Second, 3*time.Second)
	if e != nil {
		mapped := mapError(bounded, e)
		cancel()
		if errors.Is(e, db.ErrHistoryWipeInProgress) {
			return nil, failure(codes.Unavailable, "DATASET_UNAVAILABLE")
		}
		return nil, mapped
	}
	r := &snapshot{ctx: bounded, tx: tx, cleanup: cleanup, cancel: cancel}
	fail := func(e error) (*snapshot, error) { mapped := mapError(bounded, e); r.close(); return nil, mapped }
	var chainID *string
	var low, high *int64
	var pruned, prunedTime int64
	e = tx.QueryRow(bounded, `SELECT m.dataset_epoch::text,CASE WHEN octet_length(m.chain_id)<=78 THEN m.chain_id END,m.genesis_hash,(SELECT min(number) FROM blocks),(SELECT max(number) FROM blocks),p.pruned_through,p.pruned_through_ts FROM indexer_dataset_metadata m CROSS JOIN prune_state p WHERE m.id=1 AND p.id=1`).Scan(&r.epoch, &chainID, &r.genesis, &low, &high, &pruned, &prunedTime)
	if e != nil {
		return fail(e)
	}
	if read.GetExpectDatasetEpoch() != "" && read.ExpectDatasetEpoch != r.epoch {
		return fail(failure(codes.Aborted, "DATASET_EPOCH_CHANGED"))
	}
	if chainID == nil || len(r.genesis) != 32 || low == nil || high == nil {
		return fail(failure(codes.Unavailable, "INDEXER_BEHIND"))
	}
	if *low < 0 || *high < 0 || pruned < 0 || prunedTime < 0 {
		return fail(dataError())
	}
	r.chainID = *chainID
	r.lowest = uint64(*low)
	r.head = uint64(*high)
	r.prunedTime = uint64(prunedTime)
	if pruned > 0 {
		r.history = uint64(pruned) + 1
	}
	number := r.head
	if read.GetReadAt() != nil {
		number = read.ReadAt.BlockNumber
	}
	if number < r.history {
		return fail(r.pruned())
	}
	if number > r.head {
		return fail(failure(codes.Unavailable, "INDEXER_BEHIND"))
	}
	var hash *string
	var timestamp int64
	if e = tx.QueryRow(bounded, `SELECT CASE WHEN octet_length(hash)=66 THEN hash END,timestamp FROM blocks WHERE number=$1`, int64(number)).Scan(&hash, &timestamp); e != nil {
		if errors.Is(e, pgx.ErrNoRows) {
			return fail(failure(codes.Aborted, "CHAIN_REORGED"))
		}
		return fail(e)
	}
	if hash == nil {
		return fail(dataError())
	}
	b, e := decodeHex(*hash, 32)
	if e != nil {
		return fail(e)
	}
	if timestamp < 0 {
		return fail(dataError())
	}
	r.anchor = &pb.ReadAt{BlockNumber: number, BlockHash: b}
	r.anchorTime = uint64(timestamp)
	if read.GetReadAt() != nil && !proto.Equal(read.ReadAt, r.anchor) {
		return fail(failure(codes.Aborted, "CHAIN_REORGED"))
	}

	if key := page.GetStartAfter(); key != nil {
		if n, ok := historicalKeyBlock(key); ok && n < r.history {
			return fail(r.pruned())
		}
	}
	br, tr, aggregate, exact := requestScope(request)
	if e := validateRange(br, tr); e != nil {
		return fail(e)
	}
	if e := r.refineRequestScope(br, exact, requestTransactionHash(request)); e != nil {
		return fail(e)
	}
	if r.effectiveRange != nil {
		br = r.effectiveRange
	}
	if _, _, _, e := r.scope(br, tr, aggregate); e != nil {
		return fail(e)
	}
	floor := max(r.lowest, r.history)
	pinned := read != nil && read.ExpectedRunFloor != nil
	if pinned {
		floor = *read.ExpectedRunFloor
	}
	retained := max(floor, r.history)
	var count int64
	if retained > number {
		return fail(r.pruned())
	}
	if e = tx.QueryRow(bounded, `SELECT count(*) FROM blocks WHERE number BETWEEN $1 AND $2`, int64(retained), int64(number)).Scan(&count); e != nil {
		return fail(e)
	}
	if uint64(count) != number-retained+1 {
		if pinned {
			return fail(failure(codes.Aborted, "INDEX_RUN_CHANGED"))
		}
		var next int64
		e = tx.QueryRow(bounded, `WITH ordered AS (SELECT number,lag(number) OVER (ORDER BY number) AS previous FROM blocks WHERE number BETWEEN $1 AND $2) SELECT max(number) FILTER (WHERE previous IS NULL OR number>previous+1) FROM ordered`, int64(retained), int64(number)).Scan(&next)
		if e != nil {
			return fail(e)
		}
		floor = uint64(next)
	}
	r.runFloor = floor
	if key := page.GetStartAfter(); key != nil {
		if n, ok := historicalKeyBlock(key); ok && n < r.history {
			return fail(r.pruned())
		}
	}
	return r, nil
}
func (s *snapshot) pruned() error {
	return failure(codes.OutOfRange, "HISTORY_PRUNED", map[string]string{"history_from_block": uintText(s.history)})
}
func uintText(n uint64) string { return fmtUint(n) }
func (s *snapshot) scope(r *pb.BlockRange, t *pb.TimeRange, aggregate bool) (uint64, uint64, pb.EndedAt, error) {
	from, to := uint64(0), s.anchor.BlockNumber
	if r != nil {
		if r.FromBlock != nil {
			from = *r.FromBlock
		}
		if r.ToBlock != nil {
			to = *r.ToBlock
		}
		if from > math.MaxInt64 || to > math.MaxInt64 || from > to {
			return 0, 0, 0, invalid("INVALID_FILTER")
		}
	}
	if t != nil {
		if t.GetFromTimestamp() > math.MaxInt64 || t.GetToTimestamp() > math.MaxInt64 || t.FromTimestamp != nil && t.ToTimestamp != nil && *t.FromTimestamp > *t.ToTimestamp {
			return 0, 0, 0, invalid("INVALID_FILTER")
		}
	}
	explicit := r != nil && r.FromBlock != nil
	if to < s.history {
		return 0, 0, 0, s.pruned()
	}
	if aggregate && s.history > 0 && (!explicit || from < s.history) {
		return 0, 0, 0, s.pruned()
	}

	if aggregate && (explicit && from < s.runFloor || !explicit && s.runFloor > 0) {
		return 0, 0, 0, failure(codes.Unavailable, "INDEXER_BEHIND")
	}
	end := pb.EndedAt_ENDED_AT_END_OF_RANGE
	if from < s.history && s.history >= s.runFloor {
		end = pb.EndedAt_ENDED_AT_RETENTION_FLOOR
	} else if from < s.runFloor {
		end = pb.EndedAt_ENDED_AT_INDEX_FLOOR
	}
	return max(from, s.history, s.runFloor), min(to, s.anchor.BlockNumber), end, nil
}
func historicalKeyBlock(k *pb.Key) (uint64, bool) {
	switch x := k.GetValue().(type) {
	case *pb.Key_Block:
		return x.Block.GetNumber(), true
	case *pb.Key_Transaction:
		return x.Transaction.GetBlockNumber(), true
	case *pb.Key_Log:
		return x.Log.GetBlockNumber(), true
	case *pb.Key_Transfer:
		return x.Transfer.GetBlockNumber(), true
	case *pb.Key_Internal:
		return x.Internal.GetBlockNumber(), true
	case *pb.Key_Creation:
		return x.Creation.GetBlockNumber(), true
	case *pb.Key_Search:
		if x.Search != nil && x.Search.Kind != pb.SearchKind_SEARCH_KIND_ADDRESS {
			return x.Search.BlockNumber, true
		}
	}
	return 0, false
}

// A count timeout is a count failure, never an approximate successful answer.
func countError(ctx context.Context, e error) error {
	var pe *pgconn.PgError
	if errors.As(e, &pe) && pe.Code == "57014" && ctx.Err() == nil {
		return failure(codes.ResourceExhausted, "COUNT_BUDGET_EXCEEDED")
	}
	return mapError(ctx, e)
}
func statusResource(e error) bool { return status.Code(e) == codes.ResourceExhausted }

// Retention is checked from the same RR watermark before proving C1. A pruned
// cursor or aggregate cannot become an INDEX_RUN_CHANGED error merely because
// another, retained block also disappeared.
func requestScope(request proto.Message) (*pb.BlockRange, *pb.TimeRange, bool, *uint64) {
	switch r := request.(type) {
	case *pb.ListBlocksRequest:
		return r.BlockRange, nil, false, nil
	case *pb.ListTransactionsRequest:
		return r.GetFilter().GetBlockRange(), r.GetFilter().GetTimeRange(), false, nil
	case *pb.ListLogsRequest:
		return r.GetFilter().GetBlockRange(), nil, false, optionalExactBlock(r.GetFilter())
	case *pb.ListTokenTransfersRequest:
		return r.GetFilter().GetBlockRange(), nil, false, optionalExactBlock(r.GetFilter())
	case *pb.ListInternalTransactionsRequest:
		return r.GetFilter().GetBlockRange(), nil, false, optionalExactBlock(r.GetFilter())
	case *pb.ListContractsRequest:
		return r.BlockRange, nil, false, nil
	case *pb.ListTokensRequest:
		return r.BlockRange, nil, false, nil
	case *pb.CountTransactionsRequest:
		return r.GetFilter().GetBlockRange(), r.GetFilter().GetTimeRange(), true, nil
	case *pb.CountLogsRequest:
		return r.GetFilter().GetBlockRange(), nil, true, optionalExactBlock(r.GetFilter())
	case *pb.CountTokenTransfersRequest:
		return r.GetFilter().GetBlockRange(), nil, true, optionalExactBlock(r.GetFilter())
	case *pb.BatchFirstActivityRequest:
		return r.GetFilter().GetBlockRange(), r.GetFilter().GetTimeRange(), true, nil
	case *pb.ListCounterpartiesRequest:
		return r.BlockRange, r.TimeRange, true, nil
	}
	return nil, nil, false, nil
}

func optionalExactBlock(filter any) *uint64 {
	switch f := filter.(type) {
	case *pb.LogFilter:
		if f != nil {
			return f.BlockNumber
		}
	case *pb.TransferFilter:
		if f != nil {
			return f.BlockNumber
		}
	case *pb.InternalFilter:
		if f != nil {
			return f.BlockNumber
		}
	}
	return nil
}

// Intersect literal and proven-parent selectors before applying retention.
// Unknown hashes leave the original scope unchanged: they never imply an
// invented complete empty history. The lookup and all subsequent proof/query
// work share this snapshot and the original call deadline.
func (s *snapshot) refineRequestScope(base *pb.BlockRange, exact *uint64, hash []byte) error {
	var parent *uint64
	if hash != nil {
		rows, e := s.tx.Query(s.ctx, `SELECT block_number FROM transactions WHERE hash=$1 AND block_number<=$2 LIMIT 2`, hexString(hash), int64(s.anchor.BlockNumber))
		if e != nil {
			return mapError(s.ctx, e)
		}
		found := 0
		for rows.Next() {
			var n int64
			if e = rows.Scan(&n); e != nil {
				rows.Close()
				return mapError(s.ctx, e)
			}
			if n < 0 {
				rows.Close()
				return dataError()
			}
			found++
			v := uint64(n)
			parent = &v
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return mapError(s.ctx, e)
		}
		if found > 1 {
			return dataError()
		}
	}
	if exact == nil && parent == nil {
		return nil
	}
	from, to := uint64(0), s.anchor.BlockNumber
	if base != nil {
		if base.FromBlock != nil {
			from = *base.FromBlock
		}
		if base.ToBlock != nil {
			to = *base.ToBlock
		}
	}
	for _, n := range []*uint64{exact, parent} {
		if n != nil {
			from = max(from, *n)
			to = min(to, *n)
		}
	}
	if from > to {
		s.emptyScope = true
		from = s.anchor.BlockNumber
		to = from
	}
	s.effectiveRange = &pb.BlockRange{FromBlock: &from, ToBlock: &to}
	return nil
}
func requestTransactionHash(request proto.Message) []byte {
	switch r := request.(type) {
	case *pb.ListTransactionsRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.ListLogsRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.ListTokenTransfersRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.ListInternalTransactionsRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.CountTransactionsRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.CountLogsRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.CountTokenTransfersRequest:
		return r.GetFilter().GetTransactionHash()
	case *pb.BatchFirstActivityRequest:
		return r.GetFilter().GetTransactionHash()
	}
	return nil
}
