package grpcserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type v1BoundCursor struct {
	Version  int               `json:"v"`
	Query    string            `json:"q"`
	Position db.V1PagePosition `json:"p"`
}

func (s *Server) v1Input(method string, filter any, page *indexerv1.PageRequest, offset *indexerv1.OffsetPageRequest) (db.V1PageInput, string, error) {
	binding, _ := json.Marshal(struct {
		Method string
		Filter any
		Hidden []int
	}{method, filter, s.db.HiddenTxTypes})
	digest := sha256.Sum256(binding)
	query := hex.EncodeToString(digest[:])
	size := int32(0)
	raw := ""
	number := 1
	if page != nil {
		size, raw = page.PageSize, page.Cursor
	}
	if offset != nil {
		size, raw = offset.PageSize, offset.Cursor
		number = int(offset.Page)
	}
	if size < 0 || number < 0 {
		return db.V1PageInput{}, "", invalidArgument("page_size and page must not be negative")
	}
	if number < 1 {
		number = 1
	}
	in := db.V1PageInput{Limit: int(s.clampPageSize(size)), Page: number}
	if raw != "" {
		if len(raw) > 4096 {
			return in, "", invalidArgument("cursor malformed")
		}
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return in, "", invalidArgument("cursor malformed")
		}
		var cur v1BoundCursor
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&cur); err != nil {
			return in, "", invalidArgument("cursor malformed")
		}
		if decoder.Decode(new(any)) != io.EOF || cur.Version != 1 || cur.Query != query || cur.Position.Source == "" || cur.Position.Page < 1 || cur.Position.Page >= 2147483647 || len(cur.Position.Key) == 0 {
			return in, "", invalidArgument("cursor does not match this query; restart pagination")
		}
		in.Position = cur.Position
	}
	return in, query, nil
}
func v1Next(query string, pos *db.V1PagePosition) string {
	if pos == nil {
		return ""
	}
	data, _ := json.Marshal(v1BoundCursor{1, query, *pos})
	return base64.RawURLEncoding.EncodeToString(data)
}
func v1Offset(query string, pos *db.V1PagePosition, page, size int, total int64) *indexerv1.OffsetPageResponse {
	return &indexerv1.OffsetPageResponse{Page: int32(page), PageSize: int32(size), TotalItems: total, NextCursor: v1Next(query, pos)}
}
func v1PagingError(err error, where string) error {
	if err == nil {
		return nil
	}
	var prunedErr *db.V1HistoryPruned
	if errors.As(err, &prunedErr) {
		return v1HistoryPruned(prunedErr.Floor)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "history query deadline exceeded")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "history query canceled")
	}
	if errors.Is(err, db.ErrHistoryWipeInProgress) {
		return status.Error(codes.Unavailable, "indexed dataset is being replaced")
	}
	if errors.Is(err, db.ErrV1PageBudget) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	if errors.Is(err, db.ErrV1BlockNotFound) {
		return notFound("block", "")
	}
	if errors.Is(err, db.ErrV1Cursor) {
		return invalidArgument("cursor does not match this source or ordering; restart pagination")
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && (pg.Code == "22P02" || pg.Code == "22003") {
		return invalidArgument("cursor or numeric filter malformed")
	}
	if errors.As(err, &pg) && pg.Code == "55P03" {
		return status.Error(codes.Unavailable, "history is being changed; retry later")
	}
	if errors.As(err, &pg) && pg.Code == "57014" {
		return status.Error(codes.ResourceExhausted, "query exceeded its work budget")
	}
	return internalErr(err, where)
}
func v1HistoryPruned(floor uint64) error {
	st := status.New(codes.OutOfRange, "history pruned before block "+strconv.FormatUint(floor, 10))
	d, err := st.WithDetails(&errdetails.ErrorInfo{Reason: "HISTORY_PRUNED", Domain: "indexer.ops", Metadata: map[string]string{"history_from_block": strconv.FormatUint(floor, 10)}})
	if err != nil {
		return st.Err()
	}
	return d.Err()
}
func v1BlockRange(br *indexerv1.BlockRange) (uint64, uint64, error) {
	from, to := br.GetFromBlock(), br.GetToBlock()
	if to > 0 && from >= to {
		return 0, 0, invalidArgument("block_range must be a nonempty half-open interval")
	}
	if from > uint64(^uint64(0)>>1) || to > uint64(^uint64(0)>>1) {
		return 0, 0, invalidArgument("block range exceeds supported integer range")
	}
	return from, to, nil
}
