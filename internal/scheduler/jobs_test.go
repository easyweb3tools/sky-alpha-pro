package scheduler

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"sky-alpha-pro/pkg/config"
)

func TestRegisterDefaultJobsSkipsChainScanWhenRPCEmpty(t *testing.T) {
	cfg := &config.Config{}
	cfg.Scheduler.Enabled = true
	cfg.Scheduler.DefaultTimeout = time.Second
	cfg.Scheduler.Jobs.ChainScan.Enabled = true
	cfg.Scheduler.Jobs.ChainScan.Interval = time.Minute
	cfg.Scheduler.Jobs.ChainScan.Timeout = time.Second
	cfg.Chain.RPCURL = ""

	mgr := NewManager(cfg.Scheduler, zap.NewNop(), nil)
	RegisterDefaultJobs(mgr, cfg, nil, nil, nil, nil, nil, zap.NewNop())
	for _, job := range mgr.jobs {
		if job.name == "chain_scan" {
			t.Fatalf("chain_scan should not be registered when rpc url is empty")
		}
	}
}
