package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/datatypes"
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
	if summary.TotalJobs != 3 || summary.HealthyJobs != 2 || summary.UnhealthyJobs != 1 {
		t.Fatalf("unexpected summary counters: %+v", summary)
	}
	if len(summary.Blockers) != 1 {
		t.Fatalf("expected 1 blocker, got %d", len(summary.Blockers))
	}
	if summary.Blockers[0].Severity == "" {
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

func TestOpsAgentPromptPerformanceHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentSession{}); err != nil {
		t.Fatalf("migrate agent_sessions: %v", err)
	}

	now := time.Now().UTC()
	rows := []model.AgentSession{
		{
			ID:            "00000000-0000-0000-0000-000000000011",
			CycleID:       "c-1",
			PromptVersion: "v1.2.3",
			PromptVariant: "active",
			RunMode:       "vertex_brain",
			Status:        "success",
			ToolCalls:     5,
			LLMCalls:      1,
			DurationMS:    1200,
			SummaryJSON:   datatypes.JSON([]byte(`{"funnel":{"signals_generated":2}}`)),
			StartedAt:     now.Add(-10 * time.Minute),
			FinishedAt:    now.Add(-9 * time.Minute),
			CreatedAt:     now.Add(-9 * time.Minute),
			UpdatedAt:     now.Add(-9 * time.Minute),
		},
		{
			ID:            "00000000-0000-0000-0000-000000000012",
			CycleID:       "c-2",
			PromptVersion: "v1.3.0-canary",
			PromptVariant: "candidate",
			RunMode:       "vertex_brain",
			Status:        "degraded",
			ToolCalls:     7,
			LLMCalls:      1,
			DurationMS:    2000,
			SummaryJSON:   datatypes.JSON([]byte(`{"funnel":{"signals_generated":0}}`)),
			StartedAt:     now.Add(-30 * time.Minute),
			FinishedAt:    now.Add(-29 * time.Minute),
			CreatedAt:     now.Add(-29 * time.Minute),
			UpdatedAt:     now.Add(-29 * time.Minute),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed agent_sessions: %v", err)
	}

	r := gin.New()
	r.GET("/ops/agent/prompt-performance", OpsAgentPromptPerformanceHandler(db))

	req := httptest.NewRequest(http.MethodGet, "/ops/agent/prompt-performance?window_hours=24", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var body struct {
		Count int `json:"count"`
		Items []struct {
			PromptVersion       string  `json:"prompt_version"`
			PromptVariant       string  `json:"prompt_variant"`
			Runs                int64   `json:"runs"`
			SignalsGeneratedSum int64   `json:"signals_generated_sum"`
			SuccessRate         float64 `json:"success_rate"`
			DegradedRate        float64 `json:"degraded_rate"`
		} `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Count != 2 {
		t.Fatalf("expected 2 groups, got=%d", body.Count)
	}
	foundActive := false
	foundCandidate := false
	for _, item := range body.Items {
		if item.PromptVersion == "v1.2.3" && item.PromptVariant == "active" {
			foundActive = true
			if item.Runs != 1 || item.SignalsGeneratedSum != 2 || item.SuccessRate <= 0 {
				t.Fatalf("unexpected active stats: %+v", item)
			}
		}
		if item.PromptVersion == "v1.3.0-canary" && item.PromptVariant == "candidate" {
			foundCandidate = true
			if item.Runs != 1 || item.DegradedRate <= 0 {
				t.Fatalf("unexpected candidate stats: %+v", item)
			}
		}
	}
	if !foundActive || !foundCandidate {
		t.Fatalf("missing prompt performance groups: active=%v candidate=%v", foundActive, foundCandidate)
	}
}

func TestOpsAgentSessionReplayHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentSession{}, &model.AgentStep{}); err != nil {
		t.Fatalf("migrate replay tables: %v", err)
	}

	now := time.Now().UTC()
	session := model.AgentSession{
		ID:            "00000000-0000-0000-0000-000000000099",
		CycleID:       "cycle-replay-1",
		PromptVersion: "v1.0.0",
		PromptVariant: "active",
		RunMode:       "vertex_brain",
		Status:        "success",
		Decision:      "run",
		LLMCalls:      1,
		LLMTokens:     321,
		ToolCalls:     2,
		SummaryJSON: datatypes.JSON([]byte(`{
			"brain_trace":[
				{"state":"cycle_started","at":"2026-03-05T12:00:00Z"},
				{"state":"vertex_next_action","step_no":1}
			]
		}`)),
		StartedAt:  now.Add(-2 * time.Minute),
		FinishedAt: now.Add(-1 * time.Minute),
		DurationMS: 60000,
		CreatedAt:  now.Add(-1 * time.Minute),
		UpdatedAt:  now.Add(-1 * time.Minute),
	}
	if err := db.Create(&session).Error; err != nil {
		t.Fatalf("seed session: %v", err)
	}
	steps := []model.AgentStep{
		{
			SessionID:  session.ID,
			StepNo:     1,
			Tool:       "report.generate",
			Status:     "success",
			OnFail:     "continue",
			ArgsJSON:   datatypes.JSON([]byte(`{}`)),
			ResultJSON: datatypes.JSON([]byte(`{"ok":true}`)),
			StartedAt:  now.Add(-90 * time.Second),
			FinishedAt: now.Add(-89 * time.Second),
			DurationMS: 1000,
			CreatedAt:  now.Add(-89 * time.Second),
		},
	}
	if err := db.Create(&steps).Error; err != nil {
		t.Fatalf("seed steps: %v", err)
	}

	r := gin.New()
	r.GET("/ops/agent/sessions/:id/replay", OpsAgentSessionReplayHandler(db))

	req := httptest.NewRequest(http.MethodGet, "/ops/agent/sessions/"+session.ID+"/replay", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", w.Code, w.Body.String())
	}

	var body struct {
		SessionID  string           `json:"session_id"`
		BrainTrace []map[string]any `json:"brain_trace"`
		Steps      []struct {
			StepNo int `json:"step_no"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.SessionID != session.ID {
		t.Fatalf("unexpected session id: %s", body.SessionID)
	}
	if len(body.BrainTrace) != 2 {
		t.Fatalf("expected 2 brain trace events, got %d", len(body.BrainTrace))
	}
	if len(body.Steps) != 1 || body.Steps[0].StepNo != 1 {
		t.Fatalf("unexpected steps: %+v", body.Steps)
	}
}

func TestOpsAgentStrategyChangesHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentStrategyChange{}); err != nil {
		t.Fatalf("migrate agent_strategy_changes: %v", err)
	}

	now := time.Now().UTC()
	rows := []model.AgentStrategyChange{
		{
			Scope:         "prompt_rollout",
			Subject:       "v1.3.0-canary",
			ParamName:     "rollout_pct",
			OldValue:      0,
			NewValue:      20,
			TargetMetric:  "signals_per_run",
			BaselineValue: 0.2,
			LatestValue:   0.1,
			ObservedRuns:  3,
			Status:        "rolled_back",
			Decision:      "rollback",
			Reason:        "no improvement for 3 consecutive candidate cycles",
			CreatedAt:     now.Add(-10 * time.Minute),
			UpdatedAt:     now.Add(-9 * time.Minute),
			FinalizedAt:   ptrTime(now.Add(-9 * time.Minute)),
		},
		{
			Scope:         "prompt_rollout",
			Subject:       "v1.2.3",
			ParamName:     "rollout_pct",
			OldValue:      0,
			NewValue:      10,
			TargetMetric:  "signals_per_run",
			BaselineValue: 0.2,
			LatestValue:   0.3,
			ObservedRuns:  2,
			Status:        "monitoring",
			Decision:      "pending",
			CreatedAt:     now.Add(-5 * time.Minute),
			UpdatedAt:     now.Add(-4 * time.Minute),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed strategy changes: %v", err)
	}

	r := gin.New()
	r.GET("/ops/agent/strategy-changes", OpsAgentStrategyChangesHandler(db))

	req := httptest.NewRequest(http.MethodGet, "/ops/agent/strategy-changes?limit=20", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var body struct {
		Count   int `json:"count"`
		Summary struct {
			Monitoring int `json:"monitoring"`
			Kept       int `json:"kept"`
			RolledBack int `json:"rolled_back"`
		} `json:"summary"`
		Items []struct {
			Subject  string `json:"subject"`
			Status   string `json:"status"`
			Decision string `json:"decision"`
		} `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Count != 2 {
		t.Fatalf("expected 2 items got=%d", body.Count)
	}
	if body.Summary.RolledBack != 1 || body.Summary.Monitoring != 1 {
		t.Fatalf("unexpected summary: %+v", body.Summary)
	}
}

func TestOpsAgentStrategyParamsHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentStrategyParam{}); err != nil {
		t.Fatalf("migrate agent_strategy_params: %v", err)
	}

	now := time.Now().UTC()
	rows := []map[string]any{
		{
			"key":        "max_tool_calls",
			"scope":      "agent_cycle",
			"value":      10,
			"enabled":    true,
			"reason":     "test",
			"created_at": now.Add(-10 * time.Minute),
			"updated_at": now.Add(-9 * time.Minute),
		},
		{
			"key":        "max_external_requests",
			"scope":      "agent_cycle",
			"value":      90,
			"enabled":    false,
			"reason":     "test-disabled",
			"created_at": now.Add(-8 * time.Minute),
			"updated_at": now.Add(-7 * time.Minute),
		},
	}
	if err := db.Table("agent_strategy_params").Create(&rows).Error; err != nil {
		t.Fatalf("seed strategy params: %v", err)
	}

	r := gin.New()
	r.GET("/ops/agent/strategy-params", OpsAgentStrategyParamsHandler(db))
	req := httptest.NewRequest(http.MethodGet, "/ops/agent/strategy-params?scope=agent_cycle&limit=20", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	var body struct {
		Count   int `json:"count"`
		Summary struct {
			Enabled int `json:"enabled"`
		} `json:"summary"`
		Items []struct {
			Key     string  `json:"key"`
			Enabled bool    `json:"enabled"`
			Value   float64 `json:"value"`
		} `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Count != 2 {
		t.Fatalf("expected 2 items got=%d", body.Count)
	}
	if body.Summary.Enabled != 1 {
		t.Fatalf("expected enabled=1 got=%d", body.Summary.Enabled)
	}
}

func ptrTime(v time.Time) *time.Time {
	return &v
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
	if out.EffectiveSuccess24H != 1 || out.NoProgress24H != 1 {
		t.Fatalf("unexpected effective counters: %+v", out)
	}
	if out.SuccessRate24H != 0.3333 {
		t.Fatalf("expected success_rate_24h=0.3333, got %.4f", out.SuccessRate24H)
	}
	if out.EffectiveSuccessRate24H != 0.3333 {
		t.Fatalf("expected effective_success_rate_24h=0.3333, got %.4f", out.EffectiveSuccessRate24H)
	}
	if len(out.Hourly) == 0 {
		t.Fatalf("expected non-empty hourly trend")
	}
}

func TestFillSpecFillDiagnostics(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	schema := `
CREATE TABLE markets (
	id TEXT PRIMARY KEY,
	polymarket_id TEXT NOT NULL,
	question TEXT NOT NULL,
	city TEXT,
	market_type TEXT NOT NULL,
	end_date DATETIME NOT NULL,
	is_active BOOLEAN NOT NULL DEFAULT 1,
	threshold_f DECIMAL(6,2),
	comparator TEXT,
	weather_target_date DATETIME,
	spec_status TEXT,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL
);`
	if err := db.Exec(schema).Error; err != nil {
		t.Fatalf("create markets table: %v", err)
	}

	now := time.Now().UTC()
	target := now.Add(24 * time.Hour)
	rows := []map[string]any{
		{
			"id":                  "m1",
			"polymarket_id":       "pm1",
			"question":            "Will NYC exceed 70F?",
			"city":                "new york",
			"market_type":         "temperature_high",
			"end_date":            now.Add(48 * time.Hour),
			"is_active":           true,
			"threshold_f":         decimal.NewFromFloat(70),
			"comparator":          "ge",
			"weather_target_date": target,
			"spec_status":         "ready",
			"created_at":          now,
			"updated_at":          now,
		},
		{
			"id":            "m2",
			"polymarket_id": "pm2",
			"question":      "Will temp be above 60F?",
			"city":          "",
			"market_type":   "unknown",
			"end_date":      now.Add(48 * time.Hour),
			"is_active":     true,
			"spec_status":   "missing_city",
			"created_at":    now,
			"updated_at":    now,
		},
	}
	if err := db.Table("markets").Create(&rows).Error; err != nil {
		t.Fatalf("seed markets: %v", err)
	}

	out := inspectionSpecFillDiag{BySpecStatus: map[string]int64{}}
	if err := fillSpecFillDiagnostics(context.Background(), db, &out); err != nil {
		t.Fatalf("fill diag: %v", err)
	}

	if out.ActiveMarkets != 2 || out.SpecReady != 1 || out.CityMissing != 1 {
		t.Fatalf("unexpected counters: %+v", out)
	}
	if out.BySpecStatus["ready"] != 1 || out.BySpecStatus["missing_city"] != 1 {
		t.Fatalf("unexpected by_spec_status: %+v", out.BySpecStatus)
	}
}

func TestFillInspectionStrategyEvolution(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.PromptVersion{}, &model.AgentStrategyChange{}); err != nil {
		t.Fatalf("migrate strategy tables: %v", err)
	}

	now := time.Now().UTC()
	pv := model.PromptVersion{
		Name:         "agent_cycle",
		Version:      "v1.3.0-canary",
		SystemPrompt: strings.Repeat("A", 40),
		Stage:        "candidate",
		RolloutPct:   20,
		IsActive:     false,
		CreatedAt:    now.Add(-2 * time.Hour),
		UpdatedAt:    now.Add(-90 * time.Minute),
	}
	if err := db.Create(&pv).Error; err != nil {
		t.Fatalf("seed prompt version: %v", err)
	}
	rows := []model.AgentStrategyChange{
		{
			Scope:         "prompt_rollout",
			Subject:       "v1.3.0-canary",
			ParamName:     "rollout_pct",
			OldValue:      0,
			NewValue:      20,
			TargetMetric:  "signals_per_run",
			BaselineValue: 0.2,
			LatestValue:   0.1,
			ObservedRuns:  3,
			Status:        "rolled_back",
			Decision:      "rollback",
			Reason:        "no improvement for 3 consecutive candidate cycles",
			CreatedAt:     now.Add(-50 * time.Minute),
			UpdatedAt:     now.Add(-49 * time.Minute),
		},
		{
			Scope:         "prompt_rollout",
			Subject:       "v1.2.3",
			ParamName:     "rollout_pct",
			OldValue:      0,
			NewValue:      10,
			TargetMetric:  "signals_per_run",
			BaselineValue: 0.2,
			LatestValue:   0.3,
			ObservedRuns:  2,
			Status:        "monitoring",
			Decision:      "pending",
			CreatedAt:     now.Add(-30 * time.Minute),
			UpdatedAt:     now.Add(-29 * time.Minute),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed strategy changes: %v", err)
	}

	out := inspectionStrategyEvolution{WindowHours: 24}
	if err := fillInspectionStrategyEvolution(context.Background(), db, &out); err != nil {
		t.Fatalf("fill strategy evolution: %v", err)
	}

	if out.CandidateVersion != "v1.3.0-canary" || out.CandidateRolloutPct != 20 {
		t.Fatalf("unexpected candidate rollout: %+v", out)
	}
	if out.Changes24H != 2 || out.RolledBack24H != 1 || out.Monitoring24H != 1 {
		t.Fatalf("unexpected strategy summary: %+v", out)
	}
	if out.LastSubject == "" || out.LastUpdatedAt == nil {
		t.Fatalf("expected last strategy decision metadata, got %+v", out)
	}
}

func TestFillInspectionParamTuning(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentStrategyParam{}, &model.AgentStrategyChange{}); err != nil {
		t.Fatalf("migrate param tuning tables: %v", err)
	}

	now := time.Now().UTC()
	params := []map[string]any{
		{
			"key":        "max_tool_calls",
			"scope":      "agent_cycle",
			"value":      10,
			"enabled":    true,
			"reason":     "test",
			"created_at": now.Add(-2 * time.Hour),
			"updated_at": now.Add(-90 * time.Minute),
		},
		{
			"key":        "market_limit",
			"scope":      "agent_cycle",
			"value":      220,
			"enabled":    false,
			"reason":     "test",
			"created_at": now.Add(-2 * time.Hour),
			"updated_at": now.Add(-80 * time.Minute),
		},
	}
	if err := db.Table("agent_strategy_params").Create(&params).Error; err != nil {
		t.Fatalf("seed strategy params: %v", err)
	}
	changes := []model.AgentStrategyChange{
		{
			Scope:         "agent_param_tuning",
			Subject:       "agent_cycle",
			ParamName:     "max_tool_calls",
			OldValue:      12,
			NewValue:      10,
			TargetMetric:  "error_rate",
			BaselineValue: 0.3,
			LatestValue:   0.2,
			ObservedRuns:  2,
			Status:        "monitoring",
			Decision:      "pending",
			CreatedAt:     now.Add(-50 * time.Minute),
			UpdatedAt:     now.Add(-49 * time.Minute),
		},
		{
			Scope:         "agent_param_tuning",
			Subject:       "agent_cycle",
			ParamName:     "market_limit",
			OldValue:      300,
			NewValue:      220,
			TargetMetric:  "signals_per_run",
			BaselineValue: 0,
			LatestValue:   0,
			ObservedRuns:  3,
			Status:        "rolled_back",
			Decision:      "rollback",
			Reason:        "no improvement",
			CreatedAt:     now.Add(-20 * time.Minute),
			UpdatedAt:     now.Add(-19 * time.Minute),
		},
	}
	if err := db.Create(&changes).Error; err != nil {
		t.Fatalf("seed strategy changes: %v", err)
	}

	out := inspectionParamTuning{WindowHours: 24}
	if err := fillInspectionParamTuning(context.Background(), db, &out); err != nil {
		t.Fatalf("fill param tuning: %v", err)
	}
	if out.ParamsCount != 2 || out.EnabledParams != 1 {
		t.Fatalf("unexpected params counters: %+v", out)
	}
	if out.Monitoring24H != 1 || out.RolledBack24H != 1 {
		t.Fatalf("unexpected change counters: %+v", out)
	}
	if out.CurrentParams["max_tool_calls"] != 10 {
		t.Fatalf("expected current max_tool_calls=10 got=%v", out.CurrentParams["max_tool_calls"])
	}
	if out.LastUpdatedAt == nil {
		t.Fatalf("expected last_updated_at present")
	}
}

func TestFillInspectionPromptRollout(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.AgentSession{}); err != nil {
		t.Fatalf("migrate agent_sessions: %v", err)
	}
	now := time.Now().UTC()
	rows := []model.AgentSession{
		{
			ID:            "00000000-0000-0000-0000-000000000021",
			CycleID:       "pr-1",
			PromptVersion: "v1.2.3",
			PromptVariant: "active",
			Status:        "success",
			SummaryJSON:   datatypes.JSON([]byte(`{"funnel":{"signals_generated":2}}`)),
			StartedAt:     now.Add(-30 * time.Minute),
			FinishedAt:    now.Add(-29 * time.Minute),
			CreatedAt:     now.Add(-29 * time.Minute),
		},
		{
			ID:            "00000000-0000-0000-0000-000000000022",
			CycleID:       "pr-2",
			PromptVersion: "v1.3.0-canary",
			PromptVariant: "candidate",
			Status:        "success",
			SummaryJSON:   datatypes.JSON([]byte(`{"funnel":{"signals_generated":1}}`)),
			StartedAt:     now.Add(-20 * time.Minute),
			FinishedAt:    now.Add(-19 * time.Minute),
			CreatedAt:     now.Add(-19 * time.Minute),
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed agent_sessions: %v", err)
	}

	out := inspectionPromptRollout{WindowHours: 24}
	if err := fillInspectionPromptRollout(context.Background(), db, &out); err != nil {
		t.Fatalf("fill prompt rollout: %v", err)
	}
	if out.ActiveRuns != 1 || out.CandidateRuns != 1 {
		t.Fatalf("unexpected run counts: %+v", out)
	}
	if out.ActiveSignalsPerRun != 2 || out.CandidateSignalsPerRun != 1 {
		t.Fatalf("unexpected signals per run: %+v", out)
	}
}

func TestBuildInspectionActionPlan(t *testing.T) {
	resp := opsInspectionResponse{
		SpecFillDiag: inspectionSpecFillDiag{
			SupportedMarkets: 10,
			SpecReady:        0,
		},
		ValueDashboard: inspectionValueDashboard{
			Signals7D: 0,
			Trades7D:  0,
		},
		StrategyEvolution: inspectionStrategyEvolution{
			RolledBack24H: 1,
		},
	}
	immediate, h48 := buildInspectionActionPlan(resp)
	if len(immediate) == 0 || len(h48) == 0 {
		t.Fatalf("expected non-empty action plan, immediate=%v h48=%v", immediate, h48)
	}
}
