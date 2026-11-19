package indexedread

import "context"

func (r *Reader) CheckReady(ctx context.Context) error {
	if r == nil || ctx == nil {
		return ErrUnavailable
	}
	return r.withSnapshot(ctx, newBudget(r.limits), func(*snapshot) error { return nil })
}
