package signal

import (
	"context"
	"sync"
)

type signalWriteBudgetKey string

const signalWriteBudgetCtxKey signalWriteBudgetKey = "signal_write_budget"

type signalWriteBudget struct {
	max  int
	used int
	mu   sync.Mutex
}

func WithSignalWriteLimit(ctx context.Context, max int) context.Context {
	if max <= 0 {
		return ctx
	}
	b := &signalWriteBudget{max: max}
	return context.WithValue(ctx, signalWriteBudgetCtxKey, b)
}

func consumeSignalWriteBudget(ctx context.Context) bool {
	v, ok := ctx.Value(signalWriteBudgetCtxKey).(*signalWriteBudget)
	if !ok || v == nil || v.max <= 0 {
		return true
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.used >= v.max {
		return false
	}
	v.used++
	return true
}
