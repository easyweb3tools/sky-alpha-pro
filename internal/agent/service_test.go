package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

func TestAnalyzeSingleMarket(t *testing.T) {
	db := setupAgentTestDB(t)
	marketID, targetDate := seedMarketWithPrice(t, db, "poly-w6-1", "new york")
	seedForecasts(t, db, "new york", targetDate, []float64{48, 47, 49})
	seedStation(t, db, "KNYC", "new york")

	svc := newTestAgentService(db, nil)
	resp, err := svc.Analyze(context.Background(), AnalyzeRequest{
		MarketID: marketID,
		Depth:    DepthFull,
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
	if resp.Items[0].Analysis.Status != "ok" {
		t.Fatalf("expected status=ok, got=%s", resp.Items[0].Analysis.Status)
	}

	assertAgentLogCount(t, db, 1)
}

func TestAnalyzeAllMarkets(t *testing.T) {
	db := setupAgentTestDB(t)

	m1, d1 := seedMarketWithPrice(t, db, "poly-w6-2a", "new york")
	m2, d2 := seedMarketWithPrice(t, db, "poly-w6-2b", "chicago")
	seedForecasts(t, db, "new york", d1, []float64{48, 47, 49})
	seedForecasts(t, db, "chicago", d2, []float64{35, 36, 34})
	seedStation(t, db, "KNYC", "new york")
	seedStation(t, db, "KORD", "chicago")

	_ = m1
	_ = m2

	svc := newTestAgentService(db, nil)
	resp, err := svc.Analyze(context.Background(), AnalyzeRequest{
		All:   true,
		Limit: 10,
		Depth: DepthSummary,
	})
	if err != nil {
		t.Fatalf("analyze all failed: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("expected 2 analysis items, got=%d errors=%v", resp.Count, resp.Errors)
	}

	assertAgentLogCount(t, db, 2)
}

func TestAnalyzeDegradedWithoutForecast(t *testing.T) {
	db := setupAgentTestDB(t)
	marketID, _ := seedMarketWithPrice(t, db, "poly-w6-3", "new york")
	seedStation(t, db, "KNYC", "new york")

	svc := newTestAgentService(db, nil)
	resp, err := svc.Analyze(context.Background(), AnalyzeRequest{
		MarketID: marketID,
		Depth:    DepthSummary,
	})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("expected 1 analysis item, got=%d", resp.Count)
	}
	item := resp.Items[0]
	if item.Analysis.Status != "degraded" {
		t.Fatalf("expected degraded status, got=%s", item.Analysis.Status)
	}
	if item.Analysis.Confidence > 25 {
		t.Fatalf("expected degraded confidence <= 25, got=%v", item.Analysis.Confidence)
	}
}

func TestValidateDepth(t *testing.T) {
	if _, err := ValidateDepth("invalid"); err == nil {
		t.Fatalf("expected invalid depth error")
	}
	if depth, err := ValidateDepth(""); err != nil || depth != DepthSummary {
		t.Fatalf("expected empty depth => summary, got=%s err=%v", depth, err)
	}
}

func setupAgentTestDB(t *testing.T) *gorm.DB {
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
	return db
}

func seedMarketWithPrice(t *testing.T, db *gorm.DB, polymarketID string, city string) (string, time.Time) {
	t.Helper()
	marketID := uuid.NewString()
	targetDate := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	endDate := targetDate.AddDate(0, 0, 1)

	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, city, market_type, end_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)",
		marketID,
		polymarketID,
		"Will New York high exceed 45F?",
		city,
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
	return marketID, targetDate
}

func seedForecasts(t *testing.T, db *gorm.DB, city string, targetDate time.Time, highs []float64) {
	t.Helper()
	sources := []string{"openmeteo", "nws", "visualcrossing"}
	for i, source := range sources {
		high := highs[i]
		if err := db.Exec(
			"INSERT INTO forecasts(location, city, forecast_date, source, temp_high_f, temp_low_f, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			stringsTitle(city)+", United States",
			city,
			targetDate,
			source,
			high,
			39.0,
			time.Now().UTC(),
		).Error; err != nil {
			t.Fatalf("insert forecast: %v", err)
		}
	}
}

func seedStation(t *testing.T, db *gorm.DB, id string, city string) {
	t.Helper()
	if err := db.Exec("INSERT INTO weather_stations(id, city) VALUES (?, ?)", id, city).Error; err != nil {
		t.Fatalf("insert weather station: %v", err)
	}
}

func assertAgentLogCount(t *testing.T, db *gorm.DB, expected int64) {
	t.Helper()
	var logCount int64
	if err := db.Table("agent_logs").Count(&logCount).Error; err != nil {
		t.Fatalf("count agent logs: %v", err)
	}
	if logCount != expected {
		t.Fatalf("expected %d agent logs, got=%d", expected, logCount)
	}
}

func stringsTitle(v string) string {
	if v == "" {
		return v
	}
	parts := strings.Split(v, " ")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

func newTestAgentService(db *gorm.DB, weatherSvc *weather.Service) *Service {
	signalSvc := signal.NewService(config.SignalConfig{
		MinEdgePct:          1,
		MaxMarkets:          10,
		DefaultLimit:        10,
		Concurrency:         2,
		ForecastMaxAgeHours: 24,
		MinSigma:            0.5,
	}, db, zap.NewNop())

	return NewService(config.AgentConfig{
		Model:           "rule-based-agent-v1",
		AnalyzeLimit:    20,
		Concurrency:     4,
		MarketTimeout:   10 * time.Second,
		MaxForecastDays: 10,
	}, db, zap.NewNop(), weatherSvc, signalSvc)
}
