package signal

import (
	"context"
	"strings"
)

type candidateIDsCtxKey string

const candidateMarketIDsKey candidateIDsCtxKey = "candidate_market_ids"

func WithCandidateMarketIDs(ctx context.Context, ids []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		set[id] = struct{}{}
	}
	if len(set) == 0 {
		return ctx
	}
	return context.WithValue(ctx, candidateMarketIDsKey, set)
}

func candidateMarketIDsFromCtx(ctx context.Context) map[string]struct{} {
	if ctx == nil {
		return nil
	}
	v, ok := ctx.Value(candidateMarketIDsKey).(map[string]struct{})
	if !ok || len(v) == 0 {
		return nil
	}
	return v
}
