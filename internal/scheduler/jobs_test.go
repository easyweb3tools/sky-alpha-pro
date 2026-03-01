package scheduler

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/pkg/config"
)

func TestRegisterDefaultJobsRegistersChainScanWithSkipFallbackWhenRPCEmpty(t *testing.T) {
	cfg := &config.Config{}
	cfg.Scheduler.Enabled = true
	cfg.Scheduler.DefaultTimeout = time.Second
	cfg.Scheduler.Jobs.ChainScan.Enabled = true
	cfg.Scheduler.Jobs.ChainScan.Interval = time.Minute
	cfg.Scheduler.Jobs.ChainScan.Timeout = time.Second
	cfg.Chain.RPCURL = ""

	mgr := NewManager(cfg.Scheduler, zap.NewNop(), nil)
	RegisterDefaultJobs(mgr, cfg, nil, nil, nil, &chain.Service{}, nil, zap.NewNop())
	found := false
	for _, job := range mgr.jobs {
		if job.name == "chain_scan" {
			found = true
			result, err := job.run(context.Background())
			if err != nil {
				t.Fatalf("chain_scan run should skip without error when rpc empty: %v", err)
			}
			if result.SkipReason == "" {
				t.Fatalf("expected chain_scan skip reason when rpc empty")
			}
			if len(result.Errors) == 0 || result.Errors[0].Code != errCodeChainRPCEmpty {
				t.Fatalf("expected chain_rpc_empty issue, got %+v", result.Errors)
			}
		}
	}
	if !found {
		t.Fatalf("chain_scan should be registered")
	}
}
