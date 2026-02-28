package agent

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/config"
)

func TestAnalyzeSingleMarket(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	for _, ddl := range []string{
		`CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			polymarket_id TEXT,
			question TEXT,
			city TEXT,
			market_type TEXT,
			end_date DATETIME,
			is_active NUMERIC
		)`,
		`CREATE TABLE market_prices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			price_yes DECIMAL(10,4),
			price_no DECIMAL(10,4),
			captured_at DATETIME
		)`,
		`CREATE TABLE forecasts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			location TEXT,
			city TEXT,
			forecast_date DATE,
			source TEXT,
			temp_high_f DECIMAL(5,1),
			temp_low_f DECIMAL(5,1),
			fetched_at DATETIME
		)`,
		`CREATE TABLE weather_stations (
			id TEXT PRIMARY KEY,
			city TEXT
		)`,
		`CREATE TABLE signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			signal_date DATE,
			signal_type TEXT,
			direction TEXT,
			edge_pct DECIMAL(8,4),
			confidence DECIMAL(5,2),
			market_price DECIMAL(10,4),
			our_estimate DECIMAL(10,4),
			reasoning TEXT,
			ai_model TEXT,
			acted_on NUMERIC,
			created_at DATETIME
		)`,
		`CREATE TABLE agent_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT,
			market_id TEXT,
			action TEXT,
			model TEXT,
			prompt_tokens INTEGER,
			completion_tokens INTEGER,
			tool_calls JSON,
			reasoning TEXT,
			result JSON,
			duration_ms INTEGER,
			created_at DATETIME
		)`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	marketID := uuid.NewString()
	targetDate := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	endDate := targetDate.AddDate(0, 0, 1)

	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, city, market_type, end_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)",
		marketID,
		"poly-w6-1",
		"Will New York high exceed 45F?",
		"new york",
		"temperature_high",
		endDate,
		true,
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		marketID,
		decimal.NewFromFloat(0.41).String(),
		decimal.NewFromFloat(0.59).String(),
		time.Now().UTC(),
	).Error; err != nil {
		t.Fatalf("insert market price: %v", err)
	}
	for _, src := range []string{"openmeteo", "nws", "visualcrossing"} {
		if err := db.Exec(
			"INSERT INTO forecasts(location, city, forecast_date, source, temp_high_f, temp_low_f, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			"New York, United States",
			"new york",
			targetDate,
			src,
			48.0,
			39.0,
			time.Now().UTC(),
		).Error; err != nil {
			t.Fatalf("insert forecast: %v", err)
		}
	}
	if err := db.Exec("INSERT INTO weather_stations(id, city) VALUES (?, ?)", "KNYC", "new york").Error; err != nil {
		t.Fatalf("insert weather station: %v", err)
	}

	signalSvc := signal.NewService(config.SignalConfig{
		MinEdgePct:          1,
		MaxMarkets:          10,
		DefaultLimit:        10,
		Concurrency:         2,
		ForecastMaxAgeHours: 24,
		MinSigma:            0.5,
	}, db, zap.NewNop())

	svc := NewService(config.AgentConfig{
		Model:        "rule-based-agent-v1",
		AnalyzeLimit: 20,
	}, db, zap.NewNop(), nil, signalSvc)

	resp, err := svc.Analyze(context.Background(), AnalyzeRequest{
		MarketID: marketID,
		Depth:    "full",
	})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("expected 1 analysis item, got=%d", resp.Count)
	}
	if len(resp.Items[0].ToolCalls) == 0 {
		t.Fatalf("expected tool calls")
	}

	var logCount int64
	if err := db.Table("agent_logs").Count(&logCount).Error; err != nil {
		t.Fatalf("count agent logs: %v", err)
	}
	if logCount != 1 {
		t.Fatalf("expected 1 agent log row, got=%d", logCount)
	}
}
