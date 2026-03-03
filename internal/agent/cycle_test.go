package agent

import (
	"context"
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
			run_mode TEXT,
			model TEXT,
			status TEXT,
			decision TEXT,
			llm_calls INTEGER,
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
		`CREATE TABLE prompt_versions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			version TEXT,
			system_prompt TEXT,
			runtime_template TEXT,
			context_template TEXT,
			schema_json TEXT,
			is_active BOOLEAN,
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
}
