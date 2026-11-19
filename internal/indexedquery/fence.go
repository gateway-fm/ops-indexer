package indexedquery

import (
	"context"
	"time"
)

type Fence struct{ ctx context.Context }

func NewFence(ctx context.Context) Fence { return Fence{ctx} }
func (f Fence) Check() error {
	if f.ctx == nil {
		return ErrUnavailable
	}
	return f.ctx.Err()
}
func (f Fence) Original() (time.Time, bool) {
	if f.ctx == nil {
		return time.Time{}, false
	}
	return f.ctx.Deadline()
}
func (f Fence) Bind(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if f.Check() != nil || ctx == nil {
		return nil, nil, ErrUnavailable
	}
	d, ok := f.Original()
	if !ok {
		return nil, nil, ErrUnavailable
	}
	out, cancel := context.WithDeadline(ctx, d)
	stop := context.AfterFunc(f.ctx, cancel)
	return out, func() { stop(); cancel() }, nil
}
