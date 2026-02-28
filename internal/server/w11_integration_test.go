package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

func TestW11CriticalPathSignalAndAgentAPI(t *testing.T) {
	db := setupW11IntegrationDB(t)
	seedW11IntegrationData(t, db)

	cfg := &config.Config{
		App: config.AppConfig{
			Name:    "sky-alpha-pro",
			Env:     "test",
			Version: "w11-test",
		},
		Market: config.MarketConfig{
			CLOBBaseURL:    "http://127.0.0.1:65535",
			GammaBaseURL:   "http://127.0.0.1:65535",
			RequestTimeout: 2 * time.Second,
		},
		Weather: config.WeatherConfig{
			NWSBaseURL:                "http://127.0.0.1:65535",
			OpenMeteoBaseURL:          "http://127.0.0.1:65535",
			OpenMeteoGeocodingBaseURL: "http://127.0.0.1:65535",
			VisualCrossingBaseURL:     "http://127.0.0.1:65535",
			RequestTimeout:            2 * time.Second,
			UserAgent:                 "sky-alpha-pro/w11-test",
		},
		Signal: config.SignalConfig{
			MinEdgePct:          1,
			MaxMarkets:          100,
			DefaultLimit:        20,
			Concurrency:         4,
			ForecastMaxAgeHours: 24,
			MinSigma:            0.5,
		},
		Agent: config.AgentConfig{
			AnalyzeLimit:  20,
			Concurrency:   4,
			MarketTimeout: 3 * time.Second,
		},
	}

	router := NewRouter(cfg, zap.NewNop(), db)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/signals/generate?limit=10", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("generate signals status: got=%d body=%s", w.Code, w.Body.String())
	}

	type generateResp struct {
		Result struct {
			Processed int `json:"processed"`
			Generated int `json:"generated"`
		} `json:"result"`
	}
	var gr generateResp
	if err := json.Unmarshal(w.Body.Bytes(), &gr); err != nil {
		t.Fatalf("decode generate response: %v", err)
	}
	if gr.Result.Generated < 1 {
		t.Fatalf("expected generated >= 1, got %d (processed=%d)", gr.Result.Generated, gr.Result.Processed)
	}

	body := map[string]any{
		"market_id": "pm-nyc-high-2026-03-01",
		"depth":     "full",
	}
	payload, _ := json.Marshal(body)

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/analyze", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("agent analyze status: got=%d body=%s", w.Code, w.Body.String())
	}

	type analyzeResp struct {
		Count int `json:"count"`
		Items []struct {
			Analysis struct {
				Status string `json:"status"`
			} `json:"analysis"`
		} `json:"items"`
	}
	var ar analyzeResp
	if err := json.Unmarshal(w.Body.Bytes(), &ar); err != nil {
		t.Fatalf("decode analyze response: %v", err)
	}
	if ar.Count != 1 || len(ar.Items) != 1 {
		t.Fatalf("expected one analysis item, got count=%d len=%d", ar.Count, len(ar.Items))
	}
	if ar.Items[0].Analysis.Status != "ok" {
		t.Fatalf("expected analysis status ok, got %s", ar.Items[0].Analysis.Status)
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/signals?limit=10", nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list signals status: got=%d body=%s", w.Code, w.Body.String())
	}

	var signalCount int64
	if err := db.Model(&model.Signal{}).Count(&signalCount).Error; err != nil {
		t.Fatalf("count signals: %v", err)
	}
	if signalCount < 1 {
		t.Fatalf("expected at least one signal")
	}

	var logCount int64
	if err := db.Model(&model.AgentLog{}).Count(&logCount).Error; err != nil {
		t.Fatalf("count agent logs: %v", err)
	}
	if logCount != 1 {
		t.Fatalf("expected one agent log, got %d", logCount)
	}
}

func setupW11IntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	schema := []string{
		`CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			polymarket_id TEXT UNIQUE NOT NULL,
			question TEXT NOT NULL,
			city TEXT,
			market_type TEXT NOT NULL,
			end_date DATETIME NOT NULL,
			is_active BOOLEAN NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
		`CREATE TABLE market_prices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT NOT NULL,
			price_yes DECIMAL(10,4) NOT NULL,
			price_no DECIMAL(10,4) NOT NULL,
			source TEXT,
			captured_at DATETIME NOT NULL
		)`,
		`CREATE TABLE forecasts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			location TEXT NOT NULL,
			city TEXT,
			forecast_date DATE NOT NULL,
			source TEXT NOT NULL,
			temp_high_f REAL,
			temp_low_f REAL,
			fetched_at DATETIME NOT NULL
		)`,
		`CREATE TABLE weather_stations (
			id TEXT PRIMARY KEY,
			city TEXT NOT NULL
		)`,
		`CREATE TABLE signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT NOT NULL,
			signal_date DATE,
			signal_type TEXT NOT NULL,
			direction TEXT NOT NULL,
			edge_pct REAL NOT NULL,
			confidence REAL,
			market_price REAL,
			our_estimate REAL,
			reasoning TEXT,
			ai_model TEXT,
			acted_on BOOLEAN DEFAULT FALSE,
			created_at DATETIME NOT NULL
		)`,
		`CREATE TABLE agent_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT,
			market_id TEXT,
			action TEXT NOT NULL,
			model TEXT,
			prompt_tokens INTEGER,
			completion_tokens INTEGER,
			tool_calls TEXT,
			reasoning TEXT,
			result TEXT,
			duration_ms INTEGER,
			created_at DATETIME NOT NULL
		)`,
	}
	for _, stmt := range schema {
		if err := db.Exec(stmt).Error; err != nil {
			t.Fatalf("create schema failed: %v", err)
		}
	}
	return db
}

func seedW11IntegrationData(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC()
	marketID := "11111111-1111-1111-1111-111111111111"
	endDate := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	targetDate := endDate.AddDate(0, 0, -1)

	if err := db.Exec(`
		INSERT INTO markets (id, polymarket_id, question, city, market_type, end_date, is_active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, marketID, "pm-nyc-high-2026-03-01", "Will New York high temperature exceed 50F on March 1?", "new york", "temperature_high", endDate, true, now, now).Error; err != nil {
		t.Fatalf("seed market: %v", err)
	}

	if err := db.Exec(`
		INSERT INTO market_prices (market_id, price_yes, price_no, source, captured_at)
		VALUES (?, ?, ?, ?, ?)
	`, marketID, 0.40, 0.60, "clob", now).Error; err != nil {
		t.Fatalf("seed market price: %v", err)
	}

	if err := db.Exec(`
		INSERT INTO forecasts (location, city, forecast_date, source, temp_high_f, temp_low_f, fetched_at) VALUES
			('New York, NY', 'new york', ?, 'nws', 61, 47, ?),
			('New York, NY', 'new york', ?, 'openmeteo', 60, 46, ?),
			('New York, NY', 'new york', ?, 'visualcrossing', 62, 48, ?)
	`, targetDate, now, targetDate, now, targetDate, now).Error; err != nil {
		t.Fatalf("seed forecasts: %v", err)
	}

	if err := db.Exec(`
		INSERT INTO weather_stations (id, city) VALUES ('station-nyc', 'new york')
	`).Error; err != nil {
		t.Fatalf("seed weather stations: %v", err)
	}
}
