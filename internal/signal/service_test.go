package signal

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

func TestParseThresholdFahrenheit(t *testing.T) {
	tests := []struct {
		q    string
		want float64
		ok   bool
	}{
		{"Will NYC high exceed 45F?", 45, true},
		{"Will temp be above 10C tomorrow?", 50, true},
		{"No threshold here", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseThresholdFahrenheit(tt.q)
		if ok != tt.ok {
			t.Fatalf("unexpected ok for %q: got=%v want=%v", tt.q, ok, tt.ok)
		}
		if ok && (got < tt.want-0.1 || got > tt.want+0.1) {
			t.Fatalf("unexpected value for %q: got=%v want=%v", tt.q, got, tt.want)
		}
	}
}

func TestEstimateProbability(t *testing.T) {
	highValues := []float64{46, 47, 48}
	pHigh := estimateProbability(highValues, 45, "temperature_high", 0.5)
	if pHigh <= 0.5 {
		t.Fatalf("expected high probability > 0.5, got=%v", pHigh)
	}

	lowValues := []float64{28, 30, 31}
	pLow := estimateProbability(lowValues, 32, "temperature_low", 0.5)
	if pLow <= 0.5 {
		t.Fatalf("expected low probability > 0.5, got=%v", pLow)
	}
}

func TestMeanStdEdgeCases(t *testing.T) {
	mean, sigma := meanStd(nil)
	if mean != 0 || sigma != 0 {
		t.Fatalf("empty slice should return 0,0 got=%v,%v", mean, sigma)
	}

	mean, sigma = meanStd([]float64{7.5})
	if mean != 7.5 || sigma != 0 {
		t.Fatalf("single value should return mean=7.5 sigma=0, got=%v,%v", mean, sigma)
	}

	mean, sigma = meanStd([]float64{-1, 0, 1})
	if math.Abs(mean-0) > 1e-9 {
		t.Fatalf("unexpected mean: %v", mean)
	}
	if math.Abs(sigma-1) > 1e-9 {
		t.Fatalf("unexpected sigma: %v", sigma)
	}
}

func TestNormalCDFProperties(t *testing.T) {
	if math.Abs(normalCDF(0)-0.5) > 1e-9 {
		t.Fatalf("normalCDF(0) should be 0.5, got=%v", normalCDF(0))
	}
	sum := normalCDF(1.25) + normalCDF(-1.25)
	if math.Abs(sum-1.0) > 1e-6 {
		t.Fatalf("normal CDF symmetry broken, got=%v", sum)
	}
}

func TestMarketForecastDate(t *testing.T) {
	end := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	target, err := marketForecastDate(end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target.Format("2006-01-02") != "2026-03-01" {
		t.Fatalf("unexpected target date: %s", target.Format("2006-01-02"))
	}
}

func TestGenerateSignalsDedupByDay(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	marketID := uuid.NewString()
	targetDate := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	endDate := targetDate.AddDate(0, 0, 1)

	for _, ddl := range []string{
		`CREATE TABLE markets (
			id TEXT PRIMARY KEY,
			polymarket_id TEXT,
			question TEXT,
			city TEXT,
			market_type TEXT,
			threshold_f DECIMAL(6,2),
			comparator TEXT,
			weather_target_date DATE,
			spec_status TEXT,
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
		`CREATE TABLE signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			signal_date DATE,
			signal_type TEXT,
			direction TEXT,
			edge_pct DECIMAL(8,4),
			edge_exec_pct DECIMAL(8,4),
			confidence DECIMAL(5,2),
			market_price DECIMAL(10,4),
			market_price_executable DECIMAL(10,4),
			our_estimate DECIMAL(10,4),
			friction_pct DECIMAL(8,4),
			reasoning TEXT,
			ai_model TEXT,
			acted_on NUMERIC,
			created_at DATETIME
		)`,
		`CREATE TABLE weather_stations (
			id TEXT PRIMARY KEY,
			city TEXT
		)`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, city, market_type, end_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)",
		marketID,
		"poly-1",
		"Will New York high exceed 45F?",
		"new york",
		"temperature_high",
		endDate,
		true,
	).Error; err != nil {
		t.Fatalf("create market: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		marketID,
		decimal.NewFromFloat(0.40).String(),
		decimal.NewFromFloat(0.60).String(),
		time.Now().UTC(),
	).Error; err != nil {
		t.Fatalf("create market price: %v", err)
	}

	for _, item := range []struct {
		source string
		temp   float64
	}{
		{source: "openmeteo", temp: 48},
		{source: "nws", temp: 47},
		{source: "visualcrossing", temp: 49},
	} {
		if err := db.Exec(
			"INSERT INTO forecasts(location, city, forecast_date, source, temp_high_f, temp_low_f, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			"New York, United States",
			"new york",
			targetDate,
			item.source,
			item.temp,
			38,
			time.Now().UTC(),
		).Error; err != nil {
			t.Fatalf("create forecast: %v", err)
		}
	}
	if err := db.Exec("INSERT INTO weather_stations(id, city) VALUES (?, ?)", "KNYC", "new york").Error; err != nil {
		t.Fatalf("create weather station: %v", err)
	}

	svc := NewService(config.SignalConfig{
		MinEdgePct:          1,
		MaxMarkets:          10,
		DefaultLimit:        10,
		Concurrency:         2,
		ForecastMaxAgeHours: 24,
		MinSigma:            0.5,
	}, db, zap.NewNop())

	res1, err := svc.GenerateSignals(context.Background(), GenerateOptions{Limit: 10})
	if err != nil {
		t.Fatalf("first generate: %v", err)
	}
	if res1.Generated != 1 {
		t.Fatalf("expected 1 generated on first run, got=%d skipped=%d reasons=%v errors=%v", res1.Generated, res1.Skipped, res1.SkipReasons, res1.Errors)
	}

	res2, err := svc.GenerateSignals(context.Background(), GenerateOptions{Limit: 10})
	if err != nil {
		t.Fatalf("second generate: %v", err)
	}
	if res2.Generated != 1 {
		t.Fatalf("expected 1 generated on second run, got=%d", res2.Generated)
	}

	var count int64
	if err := db.Model(&model.Signal{}).Where("market_id = ?", marketID).Count(&count).Error; err != nil {
		t.Fatalf("count signals: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one deduplicated signal row, got=%d", count)
	}

	var signalRow model.Signal
	if err := db.Where("market_id = ?", marketID).First(&signalRow).Error; err != nil {
		t.Fatalf("load signal: %v", err)
	}
	if signalRow.SignalDate.Format("2006-01-02") != targetDate.Format("2006-01-02") {
		t.Fatalf("unexpected signal_date: %s", signalRow.SignalDate.Format("2006-01-02"))
	}
}
