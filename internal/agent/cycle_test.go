package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
)

func setupCycleTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	ddls := []string{
		`CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			polymarket_id TEXT,
			question TEXT,
			city TEXT,
			market_type TEXT,
			spec_status TEXT,
			end_date DATETIME,
			is_active BOOLEAN,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE forecasts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			fetched_at DATETIME
		)`,
		`CREATE TABLE signal_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at DATETIME,
			finished_at DATETIME,
			duration_ms INTEGER,
			markets_total INTEGER,
			spec_ready INTEGER,
			forecast_ready INTEGER,
			raw_edge_pass INTEGER,
			exec_edge_pass INTEGER,
			signals_generated INTEGER,
			skipped INTEGER,
			skip_reasons_json TEXT,
			created_at DATETIME
		)`,
		`CREATE TABLE agent_sessions (
			id TEXT PRIMARY KEY,
			cycle_id TEXT,
			prompt_version TEXT,
			prompt_variant TEXT,
			run_mode TEXT,
			model TEXT,
			status TEXT,
			decision TEXT,
			llm_calls INTEGER,
			llm_tokens INTEGER,
			tool_calls INTEGER,
			records_success INTEGER,
			records_error INTEGER,
			records_skipped INTEGER,
			error_code TEXT,
			error_message TEXT,
			input_context_json TEXT,
			output_plan_json TEXT,
			summary_json TEXT,
			started_at DATETIME,
			finished_at DATETIME,
			duration_ms INTEGER,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE agent_steps (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT,
			step_no INTEGER,
			tool TEXT,
			status TEXT,
			on_fail TEXT,
			error_code TEXT,
			error_detail TEXT,
			args_json TEXT,
			result_json TEXT,
			started_at DATETIME,
			finished_at DATETIME,
			duration_ms INTEGER,
			created_at DATETIME
		)`,
		`CREATE TABLE agent_memories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT,
			cycle_id TEXT,
			outcome TEXT,
			key_failures_json TEXT,
			funnel_summary_json TEXT,
			action_suggestions_json TEXT,
			execution_outcome_json TEXT,
			created_at DATETIME
		)`,
		`CREATE TABLE agent_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT,
			cycle_id TEXT,
			run_mode TEXT,
			decision TEXT,
			status TEXT,
			summary_json TEXT,
			funnel_json TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE prompt_versions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			version TEXT,
			system_prompt TEXT,
			runtime_template TEXT,
			context_template TEXT,
			schema_json TEXT,
			stage TEXT,
			rollout_pct REAL,
			is_active BOOLEAN,
			created_at DATETIME,
			updated_at DATETIME
		)`,
		`CREATE TABLE agent_strategy_changes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scope TEXT,
			subject TEXT,
			param_name TEXT,
			old_value REAL,
			new_value REAL,
			target_metric TEXT,
			baseline_value REAL,
			latest_value REAL,
			observed_runs INTEGER,
			no_improve_streak INTEGER,
			status TEXT,
			decision TEXT,
			reason TEXT,
			meta_json TEXT,
			created_at DATETIME,
			updated_at DATETIME,
			finalized_at DATETIME
		)`,
		`CREATE TABLE agent_strategy_params (
			key TEXT PRIMARY KEY,
			scope TEXT,
			value REAL,
			enabled BOOLEAN,
			reason TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}
	return db
}

func TestRunCycleFallbackPersistsSessionAndMemory(t *testing.T) {
	db := setupCycleTestDB(t)

	now := time.Now().UTC()
	if err := db.Exec(
		`INSERT INTO markets(id, polymarket_id, question, city, market_type, spec_status, end_date, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"00000000-0000-0000-0000-000000000001",
		"poly-test-1",
		"Will New York high exceed 60F tomorrow?",
		"",
		"temperature_high",
		"incomplete",
		now.Add(24*time.Hour),
		true,
		now,
		now,
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}

	svc := &Service{
		db:  db,
		log: zap.NewNop(),
	}

	res, err := svc.RunCycle(context.Background(), CycleOptions{RunMode: "observe"})
	if err != nil {
		t.Fatalf("run cycle: %v", err)
	}
	if res.SessionID == "" {
		t.Fatalf("expected session_id")
	}
	if res.ToolCalls == 0 {
		t.Fatalf("expected at least one step in fallback plan")
	}
	if res.LLMCalls != 0 {
		t.Fatalf("expected fallback llm_calls=0, got=%d", res.LLMCalls)
	}

	var sessions int64
	if err := db.Model(&model.AgentSession{}).Count(&sessions).Error; err != nil {
		t.Fatalf("count agent_sessions: %v", err)
	}
	if sessions != 1 {
		t.Fatalf("expected 1 agent_session, got=%d", sessions)
	}
	var steps int64
	if err := db.Model(&model.AgentStep{}).Count(&steps).Error; err != nil {
		t.Fatalf("count agent_steps: %v", err)
	}
	if steps < 1 {
		t.Fatalf("expected at least 1 agent_step, got=%d", steps)
	}
	var memories int64
	if err := db.Model(&model.AgentMemory{}).Count(&memories).Error; err != nil {
		t.Fatalf("count agent_memories: %v", err)
	}
	if memories != 1 {
		t.Fatalf("expected 1 agent_memory, got=%d", memories)
	}
	var reports int64
	if err := db.Model(&model.AgentReport{}).Count(&reports).Error; err != nil {
		t.Fatalf("count agent_reports: %v", err)
	}
	if reports != 1 {
		t.Fatalf("expected 1 agent_report, got=%d", reports)
	}
}

func TestRunCycleUsesActivePromptVersion(t *testing.T) {
	db := setupCycleTestDB(t)
	now := time.Now().UTC()

	if err := db.Exec(
		`INSERT INTO prompt_versions(name, version, system_prompt, runtime_template, context_template, schema_json, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_cycle", "v1.2.3", "custom system prompt", "{}", "{}", "{}", true, now, now,
	).Error; err != nil {
		t.Fatalf("insert prompt version: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO markets(id, polymarket_id, question, city, market_type, spec_status, end_date, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"00000000-0000-0000-0000-000000000002",
		"poly-test-2",
		"Will Chicago high exceed 40F tomorrow?",
		"chicago",
		"temperature_high",
		"ready",
		now.Add(24*time.Hour),
		true,
		now,
		now,
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}

	svc := &Service{
		db:  db,
		log: zap.NewNop(),
	}
	res, err := svc.RunCycle(context.Background(), CycleOptions{RunMode: "observe"})
	if err != nil {
		t.Fatalf("run cycle: %v", err)
	}

	var row model.AgentSession
	if err := db.Where("id = ?", res.SessionID).Take(&row).Error; err != nil {
		t.Fatalf("load agent_session: %v", err)
	}
	if row.PromptVersion != "v1.2.3" {
		t.Fatalf("expected prompt_version=v1.2.3 got=%s", row.PromptVersion)
	}
	if row.PromptVariant != "active" {
		t.Fatalf("expected prompt_variant=active got=%s", row.PromptVariant)
	}
}

func TestRunCycleCanaryPromptRollout(t *testing.T) {
	db := setupCycleTestDB(t)
	now := time.Now().UTC()

	if err := db.Exec(
		`INSERT INTO prompt_versions(name, version, system_prompt, runtime_template, context_template, schema_json, stage, rollout_pct, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_cycle", "v1.2.3", strings.Repeat("A", 40), "{}", "{}", "{}", "active", 0, true, now, now,
	).Error; err != nil {
		t.Fatalf("insert active prompt version: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO prompt_versions(name, version, system_prompt, runtime_template, context_template, schema_json, stage, rollout_pct, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_cycle", "v1.3.0-canary", strings.Repeat("C", 40), "{}", "{}", "{}", "candidate", 100.0, false, now, now.Add(time.Second),
	).Error; err != nil {
		t.Fatalf("insert candidate prompt version: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO markets(id, polymarket_id, question, city, market_type, spec_status, end_date, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"00000000-0000-0000-0000-000000000003",
		"poly-test-3",
		"Will Boston high exceed 45F tomorrow?",
		"boston",
		"temperature_high",
		"ready",
		now.Add(24*time.Hour),
		true,
		now,
		now,
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}

	svc := &Service{
		db:  db,
		log: zap.NewNop(),
	}
	res, err := svc.RunCycle(context.Background(), CycleOptions{
		RunMode: "observe",
		CycleID: "cycle-rollout-fixed",
	})
	if err != nil {
		t.Fatalf("run cycle: %v", err)
	}

	var row model.AgentSession
	if err := db.Where("id = ?", res.SessionID).Take(&row).Error; err != nil {
		t.Fatalf("load agent_session: %v", err)
	}
	if row.PromptVersion != "v1.3.0-canary" {
		t.Fatalf("expected prompt_version=v1.3.0-canary got=%s", row.PromptVersion)
	}
	if row.PromptVariant != "candidate" {
		t.Fatalf("expected prompt_variant=candidate got=%s", row.PromptVariant)
	}
}

func TestRunCycleCanaryPromptAutoRollbackAfterThreeNoImprove(t *testing.T) {
	db := setupCycleTestDB(t)
	now := time.Now().UTC()

	if err := db.Exec(
		`INSERT INTO prompt_versions(name, version, system_prompt, runtime_template, context_template, schema_json, stage, rollout_pct, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_cycle", "v1.2.3", strings.Repeat("A", 40), "{}", "{}", "{}", "active", 0, true, now, now,
	).Error; err != nil {
		t.Fatalf("insert active prompt version: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO prompt_versions(name, version, system_prompt, runtime_template, context_template, schema_json, stage, rollout_pct, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"agent_cycle", "v1.3.0-canary", strings.Repeat("C", 40), "{}", "{}", "{}", "candidate", 100.0, false, now, now.Add(time.Second),
	).Error; err != nil {
		t.Fatalf("insert candidate prompt version: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO markets(id, polymarket_id, question, city, market_type, spec_status, end_date, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"00000000-0000-0000-0000-000000000004",
		"poly-test-4",
		"Will Seattle high exceed 50F tomorrow?",
		"seattle",
		"temperature_high",
		"ready",
		now.Add(24*time.Hour),
		true,
		now,
		now,
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}

	svc := &Service{
		db:  db,
		log: zap.NewNop(),
	}
	for i := 0; i < 3; i++ {
		_, err := svc.RunCycle(context.Background(), CycleOptions{
			RunMode: "observe",
			CycleID: "cycle-canary-no-improve-" + time.Now().UTC().Add(time.Duration(i)*time.Second).Format("150405"),
		})
		if err != nil {
			t.Fatalf("run cycle #%d: %v", i+1, err)
		}
	}

	var candidate model.PromptVersion
	if err := db.Where("version = ?", "v1.3.0-canary").Take(&candidate).Error; err != nil {
		t.Fatalf("load candidate prompt version: %v", err)
	}
	if candidate.RolloutPct != 0 {
		t.Fatalf("expected candidate rollout_pct to be 0 after rollback, got=%v", candidate.RolloutPct)
	}

	var change model.AgentStrategyChange
	if err := db.Where("scope = ? AND subject = ?", strategyScopePromptRollout, "v1.3.0-canary").
		Order("id DESC").
		Take(&change).Error; err != nil {
		t.Fatalf("load strategy change: %v", err)
	}
	if change.Status != "rolled_back" {
		t.Fatalf("expected strategy change status=rolled_back got=%s", change.Status)
	}
	if change.NoImproveStreak < 3 {
		t.Fatalf("expected no_improve_streak >=3 got=%d", change.NoImproveStreak)
	}
}

func TestBuildParamAdjustmentProposals_MultiParamMatrix(t *testing.T) {
	svc := &Service{}
	base := CycleOptions{
		RunMode:             "vertex_brain",
		MaxToolCalls:        12,
		MaxExternalRequests: 120,
		MarketLimit:         200,
	}

	// 429 pressure should down-tune tool/external budgets.
	p429 := svc.buildParamAdjustmentProposals(base, paramTuningSnapshot{
		Had429:           true,
		SignalsGenerated: 0,
		SpecReady:        10,
		ForecastReady:    8,
	})
	if len(p429) == 0 {
		t.Fatalf("expected proposals for 429 pressure")
	}
	if p429[0].TargetMetric != "error_rate" {
		t.Fatalf("expected error_rate target, got=%s", p429[0].TargetMetric)
	}

	// ready funnel + zero signal should widen search knobs.
	pNoSignal := svc.buildParamAdjustmentProposals(base, paramTuningSnapshot{
		Had429:           false,
		SignalsGenerated: 0,
		SpecReady:        6,
		ForecastReady:    6,
	})
	if len(pNoSignal) == 0 {
		t.Fatalf("expected proposals for no-signal scenario")
	}
	if pNoSignal[0].TargetMetric != "signals_per_run" {
		t.Fatalf("expected signals_per_run target, got=%s", pNoSignal[0].TargetMetric)
	}
}

func TestApplyAgentCycleParamOverrides_UsesPersistedParams(t *testing.T) {
	db := setupCycleTestDB(t)
	now := time.Now().UTC()
	if err := db.Exec(
		`INSERT INTO agent_strategy_params(key, scope, value, enabled, reason, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		strategyParamMaxToolCalls, strategySubjectAgentCycle, 9, true, "test", now, now,
	).Error; err != nil {
		t.Fatalf("seed strategy param max_tool_calls: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO agent_strategy_params(key, scope, value, enabled, reason, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		strategyParamMaxExternalReqs, strategySubjectAgentCycle, 12, true, "test", now, now,
	).Error; err != nil {
		t.Fatalf("seed strategy param max_external_requests: %v", err)
	}
	svc := &Service{db: db, log: zap.NewNop()}
	out := svc.applyAgentCycleParamOverrides(context.Background(), CycleOptions{
		RunMode:             "vertex_brain",
		MaxToolCalls:        12,
		MaxExternalRequests: 200,
		MarketLimit:         300,
	})
	if out.MaxToolCalls != 9 {
		t.Fatalf("expected max_tool_calls override=9 got=%d", out.MaxToolCalls)
	}
	// constraint: max_external_requests must be >= max_tool_calls * 3
	if out.MaxExternalRequests != 27 {
		t.Fatalf("expected constrained max_external_requests=27 got=%d", out.MaxExternalRequests)
	}
}
