package scheduler

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/pkg/config"
)

func TestRegisterDefaultJobsRegistersChainScanWithSkipFallbackWhenRPCEmpty(t *testing.T) {
	cfg := &config.Config{}
	cfg.Scheduler.Enabled = true
	cfg.Scheduler.DefaultTimeout = time.Second
	cfg.Scheduler.Jobs.ChainScan.Enabled = true
	cfg.Scheduler.Jobs.ChainScan.Interval = 10 * time.Second
	cfg.Scheduler.Jobs.ChainScan.Timeout = time.Second
	cfg.Chain.RPCURL = ""

	mgr := NewManager(cfg.Scheduler, zap.NewNop(), nil)
	RegisterDefaultJobs(mgr, cfg, nil, nil, nil, &chain.Service{}, nil, zap.NewNop())
	found := false
	for _, job := range mgr.jobs {
		if job.name == "chain_scan" {
			found = true
			if job.interval != 30*time.Second {
				t.Fatalf("expected chain_scan interval clamped to 30s, got %s", job.interval)
			}
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

func TestLoadActiveCitiesFallsBackToQuestionParsing(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			city TEXT,
			question TEXT,
			market_type TEXT,
			is_active BOOLEAN,
			updated_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create markets: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE weather_stations (
			id TEXT PRIMARY KEY,
			city TEXT
		)
	`).Error; err != nil {
		t.Fatalf("create weather_stations: %v", err)
	}
	if err := db.Exec(`INSERT INTO weather_stations(id, city) VALUES ('s1', 'new york'), ('s2', 'chicago')`).Error; err != nil {
		t.Fatalf("seed weather_stations: %v", err)
	}
	if err := db.Exec(`
		INSERT INTO markets(id, city, question, market_type, is_active, updated_at) VALUES
		('m1', '', 'Will NYC high temperature exceed 55F tomorrow?', 'unknown', 1, CURRENT_TIMESTAMP),
		('m2', '', 'Will Chicago be above 20F?', 'unknown', 1, CURRENT_TIMESTAMP)
	`).Error; err != nil {
		t.Fatalf("seed markets: %v", err)
	}

	cities, err := loadActiveCities(context.Background(), db)
	if err != nil {
		t.Fatalf("load active cities: %v", err)
	}
	if !slices.Contains(cities, "new york") {
		t.Fatalf("expected fallback extracted city new york, got %v", cities)
	}
	if !slices.Contains(cities, "chicago") {
		t.Fatalf("expected fallback extracted city chicago, got %v", cities)
	}
}
