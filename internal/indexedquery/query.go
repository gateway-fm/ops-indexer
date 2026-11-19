package indexedquery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var (
	ErrInvalid     = errors.New("invalid indexed read")
	ErrUnavailable = errors.New("indexed source unavailable")
	ErrTooBroad    = errors.New("indexed read exceeds limit")
	ErrConflict    = errors.New("indexed read changed")
)

type Family string

const (
	Blocks       Family = "blocks"
	Transactions Family = "transactions"
	Transfers    Family = "transfers"
	Internals    Family = "internals"
	Logs         Family = "logs"
	Tokens       Family = "tokens"
	Balances     Family = "balances"
	Holders      Family = "holders"
	Contracts    Family = "contracts"
	Addresses    Family = "addresses"
	Sync         Family = "sync"
	Progress     Family = "progress"
	Search       Family = "search"
)

type Anchor struct {
	Number uint64
	Hash   string
}
type Position struct {
	Block                uint64
	Index                uint32
	Hash, Subkey, Amount string
}
type Query struct {
	Family                               Family
	Address, Hash, Token, Topic0, Search string
	Block, From, Through                 *uint64
	Upper                                *Anchor
	After                                *Position
	RawLimit                             int
	Fence                                Fence `json:"-"`
}

var addressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
var hashPattern = regexp.MustCompile(`^0x[0-9a-f]{64}$`)

func ValidAddress(s string) bool { return addressPattern.MatchString(s) }
func ValidHash(s string) bool    { return hashPattern.MatchString(s) }
func Normalize(q Query) (Query, error) {
	switch q.Family {
	case Blocks, Transactions, Transfers, Internals, Logs, Tokens, Balances, Holders, Contracts, Addresses, Sync, Progress, Search:
	default:
		return Query{}, ErrInvalid
	}
	if q.RawLimit < 1 || q.RawLimit > 1024 || q.Address != "" && !ValidAddress(q.Address) || q.Token != "" && !ValidAddress(q.Token) || q.Hash != "" && !ValidHash(q.Hash) || q.Topic0 != "" && !ValidHash(q.Topic0) || len(q.Search) > 128 {
		return Query{}, ErrInvalid
	}
	for _, n := range []*uint64{q.Block, q.From, q.Through} {
		if n != nil && *n > 1<<63-1 {
			return Query{}, ErrInvalid
		}
	}
	if q.From != nil && q.Through != nil && *q.From > *q.Through {
		return Query{}, ErrInvalid
	}
	if q.Upper != nil {
		if q.Upper.Number > 1<<63-1 || !ValidHash(q.Upper.Hash) {
			return Query{}, ErrInvalid
		}
		c := *q.Upper
		q.Upper = &c
	}
	if q.After != nil {
		c := *q.After
		if c.Block > 1<<63-1 || len(c.Hash) > 66 || len(c.Subkey) > 256 || len(c.Amount) > 78 {
			return Query{}, ErrInvalid
		}
		q.After = &c
	}
	for _, n := range []**uint64{&q.Block, &q.From, &q.Through} {
		if *n != nil {
			v := **n
			*n = &v
		}
	}
	return q, nil
}
func (q Query) Digest() [32]byte { raw, _ := json.Marshal(q); return sha256.Sum256(raw) }
func (q Query) Bind(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return q.Fence.Bind(ctx)
}
func (q Query) Deadline() time.Time { d, _ := q.Fence.Original(); return d }
