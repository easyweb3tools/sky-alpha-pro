package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

func TestOpsStatusHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	cfg := &config.Config{}
	cfg.Metrics.Enabled = true
	cfg.Metrics.Path = "/metrics"
	cfg.Chain.RPCURL = "https://example.com/rpc"
	cfg.Chain.ScanLookbackBlocks = 20
	cfg.Agent.VertexProject = "project-1"
	cfg.Weather.VisualCrossingAPIKey = "k1"
	cfg.Scheduler.Enabled = true
	cfg.Scheduler.Jobs.ChainScan.Interval = 60 * time.Second

	reg := metrics.New(cfg.Metrics)
	reg.SetDataFreshness("markets", 12)

	mgr := scheduler.NewManager(cfg.Scheduler, zap.NewNop(), reg)

	r := gin.New()
	r.GET("/ops/status", OpsStatusHandler(cfg, reg, mgr))

	req := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var body struct {
		Summary struct {
			Degraded      bool `json:"degraded"`
			UnhealthyJobs int  `json:"unhealthy_jobs"`
		} `json:"summary"`
		ConfigCheck struct {
			ChainRPCConfigured           bool   `json:"chain_rpc_configured"`
			AgentVertexProjectConfigured bool   `json:"agent_vertex_project_configured"`
			VisualCrossingKeyConfigured  bool   `json:"visualcrossing_key_configured"`
			ChainScanLookbackBlocks      uint64 `json:"chain_scan_lookback_blocks"`
			ChainScanConfiguredIntervalS int64  `json:"chain_scan_configured_interval_seconds"`
			ChainScanEffectiveIntervalS  int64  `json:"chain_scan_effective_interval_seconds"`
		} `json:"config_check"`
		Freshness map[string]float64 `json:"freshness"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !body.ConfigCheck.ChainRPCConfigured {
		t.Fatalf("expected chain rpc configured")
	}
	if !body.ConfigCheck.AgentVertexProjectConfigured {
		t.Fatalf("expected vertex project configured")
	}
	if !body.ConfigCheck.VisualCrossingKeyConfigured {
		t.Fatalf("expected visual crossing key configured")
	}
	if body.ConfigCheck.ChainScanLookbackBlocks != 20 {
		t.Fatalf("expected chain lookback blocks 20, got %d", body.ConfigCheck.ChainScanLookbackBlocks)
	}
	if body.ConfigCheck.ChainScanConfiguredIntervalS != 60 {
		t.Fatalf("expected configured chain scan interval 60s, got %d", body.ConfigCheck.ChainScanConfiguredIntervalS)
	}
	if body.ConfigCheck.ChainScanEffectiveIntervalS != 0 {
		t.Fatalf("expected effective interval 0 without registered chain job, got %d", body.ConfigCheck.ChainScanEffectiveIntervalS)
	}
	if body.Freshness["markets"] != 12 {
		t.Fatalf("expected markets freshness 12, got %v", body.Freshness["markets"])
	}
	if body.Summary.Degraded {
		t.Fatalf("expected not degraded with empty scheduler jobs")
	}
	if body.Summary.UnhealthyJobs != 0 {
		t.Fatalf("expected 0 unhealthy jobs, got %d", body.Summary.UnhealthyJobs)
	}
}

func TestOpsStatusHandlerAuth(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	t.Setenv("SKY_ALPHA_OPS_TOKEN", "ops-token")

	r := gin.New()
	r.GET("/ops/status", OpsStatusHandler(&config.Config{}, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", w.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	req2.Header.Set("Authorization", "Bearer ops-token")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d", w2.Code)
	}
}

func TestSummarizeSchedulerSnapshot(t *testing.T) {
	now := time.Now().UTC()
	s := scheduler.ManagerSnapshot{
		Enabled: true,
		Jobs: []scheduler.JobRuntimeSnapshot{
			{Name: "market_sync", LastStatus: "success"},
			{
				Name:                "chain_scan",
				LastStatus:          "error",
				LastErrorCode:       "upstream_429",
				LastErrorMessage:    "429 Too Many Requests",
				ConsecutiveFailures: 3,
				LastErrorAt:         &now,
			},
			{
				Name:                "weather_forecast",
				LastStatus:          "skipped_no_input",
				LastErrorCode:       "empty_city_set",
				LastErrorMessage:    "no active cities",
				ConsecutiveFailures: 2,
			},
		},
	}

	summary := summarizeSchedulerSnapshot(s)
	if !summary.Degraded {
		t.Fatalf("expected degraded=true")
	}
	if summary.TotalJobs != 3 || summary.HealthyJobs != 1 || summary.UnhealthyJobs != 2 {
		t.Fatalf("unexpected summary counters: %+v", summary)
	}
	if len(summary.Blockers) != 2 {
		t.Fatalf("expected 2 blockers, got %d", len(summary.Blockers))
	}
	if summary.Blockers[0].Severity == "" || summary.Blockers[1].Severity == "" {
		t.Fatalf("expected blockers with severity, got %+v", summary.Blockers)
	}
}

func TestOpsAgentValidationsHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentValidation{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	now := time.Now().UTC()
	rows := []model.AgentValidation{
		{
			SessionID:      "00000000-0000-0000-0000-000000000001",
			CycleID:        "cycle-1",
			ValidatorModel: "gemini-2.5-pro",
			Verdict:        "pass",
			Score:          91,
			Summary:        "cycle is healthy",
			CreatedAt:      now,
		},
		{
			SessionID:      "00000000-0000-0000-0000-000000000002",
			CycleID:        "cycle-2",
			ValidatorModel: "gemini-2.5-pro",
			Verdict:        "warning",
			Score:          68,
			Summary:        "partial data",
			CreatedAt:      now.Add(-time.Hour),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed validations: %v", err)
	}

	svc := agent.NewService(config.AgentConfig{}, db, zap.NewNop(), nil, nil)
	r := gin.New()
	r.GET("/ops/agent/validations", OpsAgentValidationsHandler(svc))

	req := httptest.NewRequest(http.MethodGet, "/ops/agent/validations?limit=1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var body struct {
		Items []struct {
			CycleID string `json:"cycle_id"`
			Verdict string `json:"verdict"`
		} `json:"items"`
		Count   int `json:"count"`
		Summary struct {
			Total     int            `json:"total"`
			ByVerdict map[string]int `json:"by_verdict"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Count != 1 || len(body.Items) != 1 {
		t.Fatalf("expected one item, got count=%d len=%d", body.Count, len(body.Items))
	}
	if body.Items[0].CycleID != "cycle-2" && body.Items[0].CycleID != "cycle-1" {
		t.Fatalf("unexpected cycle id: %s", body.Items[0].CycleID)
	}
	if body.Summary.Total != 2 {
		t.Fatalf("expected summary total 2, got %d", body.Summary.Total)
	}
	if body.Summary.ByVerdict["pass"] != 1 || body.Summary.ByVerdict["warning"] != 1 {
		t.Fatalf("unexpected verdict stats: %+v", body.Summary.ByVerdict)
	}
}

func TestOpsAgentValidationsHandlerAuth(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	t.Setenv("SKY_ALPHA_OPS_TOKEN", "ops-token")

	r := gin.New()
	r.GET("/ops/agent/validations", OpsAgentValidationsHandler(nil))

	req := httptest.NewRequest(http.MethodGet, "/ops/agent/validations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", w.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/ops/agent/validations", nil)
	req2.Header.Set("Authorization", "Bearer ops-token")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d", w2.Code)
	}
}

func TestOpsInspectionHandlerWithoutDB(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.GET("/ops/inspection", OpsInspectionHandler(nil, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/ops/inspection", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without db, got %d", w.Code)
	}
}

func TestFillSpecFillTrend(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.SchedulerRun{}); err != nil {
		t.Fatalf("migrate scheduler_runs: %v", err)
	}

	now := time.Now().UTC()
	rows := []model.SchedulerRun{
		{
			JobName:    "market_spec_fill",
			Status:     "success",
			StartedAt:  now.Add(-2 * time.Hour),
			FinishedAt: now.Add(-2*time.Hour + 10*time.Second),
		},
		{
			JobName:    "market_spec_fill",
			Status:     "error",
			StartedAt:  now.Add(-90 * time.Minute),
			FinishedAt: now.Add(-90*time.Minute + 10*time.Second),
		},
		{
			JobName:    "market_spec_fill",
			Status:     "skipped_no_input",
			StartedAt:  now.Add(-30 * time.Minute),
			FinishedAt: now.Add(-30*time.Minute + 10*time.Second),
		},
		{
			JobName:    "market_sync",
			Status:     "success",
			StartedAt:  now.Add(-30 * time.Minute),
			FinishedAt: now.Add(-30*time.Minute + 10*time.Second),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed scheduler_runs: %v", err)
	}

	out := inspectionSpecFillTrend{
		WindowHours: 24,
		Hourly:      make([]inspectionSpecFillHourAgg, 0),
	}
	if err := fillSpecFillTrend(context.Background(), db, &out); err != nil {
		t.Fatalf("fill trend: %v", err)
	}

	if out.Runs24H != 3 {
		t.Fatalf("expected runs_24h=3, got %d", out.Runs24H)
	}
	if out.Success24H != 1 || out.Error24H != 1 || out.Skipped24H != 1 {
		t.Fatalf("unexpected counters: %+v", out)
	}
	if out.SuccessRate24H != 0.3333 {
		t.Fatalf("expected success_rate_24h=0.3333, got %.4f", out.SuccessRate24H)
	}
	if len(out.Hourly) == 0 {
		t.Fatalf("expected non-empty hourly trend")
	}
}
