package sim

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/pkg/config"
)

func setupSimTestDB(t *testing.T) *gorm.DB {
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
			question TEXT DEFAULT '',
			market_type TEXT DEFAULT '',
			city TEXT DEFAULT '',
			is_active BOOLEAN DEFAULT TRUE,
			is_resolved BOOLEAN DEFAULT FALSE,
			resolution TEXT DEFAULT '',
			liquidity DECIMAL(18,2),
			token_id_yes TEXT,
			token_id_no TEXT,
			end_date DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			signal_date DATE,
			signal_type TEXT DEFAULT '',
			direction TEXT DEFAULT '',
			edge_pct DECIMAL(8,4),
			confidence DECIMAL(5,2) DEFAULT 0,
			market_price DECIMAL(10,4) DEFAULT 0,
			our_estimate DECIMAL(10,4) DEFAULT 0,
			reasoning TEXT DEFAULT '',
			ai_model TEXT DEFAULT '',
			acted_on BOOLEAN DEFAULT FALSE,
			created_at DATETIME
		)`,
		`CREATE TABLE trades (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			signal_id INTEGER,
			order_id TEXT,
			side TEXT,
			outcome TEXT,
			price DECIMAL(10,4),
			size DECIMAL(18,6),
			cost_usdc DECIMAL(18,6),
			fee_usdc DECIMAL(18,6),
			status TEXT,
			fill_price DECIMAL(10,4),
			fill_size DECIMAL(18,6),
			pnl_usdc DECIMAL(18,6),
			tx_hash TEXT,
			is_paper BOOLEAN DEFAULT FALSE,
			executed_at DATETIME,
			created_at DATETIME
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
			market_id TEXT,
			station_id TEXT DEFAULT '',
			location TEXT DEFAULT '',
			city TEXT DEFAULT '',
			forecast_date DATE,
			source TEXT DEFAULT '',
			temp_high_f DECIMAL(5,1) DEFAULT 0,
			temp_low_f DECIMAL(5,1) DEFAULT 0,
			precip_in DECIMAL(6,2) DEFAULT 0,
			snow_in DECIMAL(6,2) DEFAULT 0,
			wind_speed_mph DECIMAL(5,1) DEFAULT 0,
			wind_gust_mph DECIMAL(5,1) DEFAULT 0,
			cloud_cover_pct INT DEFAULT 0,
			humidity_pct INT DEFAULT 0,
			raw_data TEXT,
			lead_days INT DEFAULT 0,
			fetched_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

func seedSimFixtures(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC()
	// Markets
	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, market_type, city, is_active, liquidity, token_id_yes, token_id_no, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "poly-1", "Will it be hot?", "temperature_high", "New York", true, "2500.00", "1000001", "1000002", now.Add(48*time.Hour),
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, market_type, city, is_active, liquidity, token_id_yes, token_id_no, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-2", "poly-2", "Will it be cold?", "temperature_low", "Chicago", true, "3000.00", "2000001", "2000002", now.Add(48*time.Hour),
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}

	// Signals with high edge
	if err := db.Exec(
		"INSERT INTO signals(market_id, signal_type, direction, edge_pct, acted_on, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		"market-1", "temperature_high", "YES", 12.5, false, now,
	).Error; err != nil {
		t.Fatalf("insert signal: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO signals(market_id, signal_type, direction, edge_pct, acted_on, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		"market-2", "temperature_low", "NO", -8.0, false, now,
	).Error; err != nil {
		t.Fatalf("insert signal: %v", err)
	}

	// Market prices
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		"market-1", "0.65", "0.35", now,
	).Error; err != nil {
		t.Fatalf("insert price: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		"market-2", "0.40", "0.60", now,
	).Error; err != nil {
		t.Fatalf("insert price: %v", err)
	}
}

func newSimServiceForTest(db *gorm.DB) *Service {
	signalSvc := signal.NewService(config.SignalConfig{
		MinEdgePct:          1,
		MaxMarkets:          10,
		DefaultLimit:        10,
		Concurrency:         2,
		ForecastMaxAgeHours: 24,
		MinSigma:            0.5,
	}, db, zap.NewNop())

	tradeSvc := trade.NewService(config.TradeConfig{
		ChainID:              137,
		MaxPositionSize:      100,
		MaxDailyLoss:         50,
		MinEdgePct:           5,
		MaxOpenPositions:     20,
		MinLiquidity:         1000,
		ConfirmationRequired: false,
		PaperMode:            true,
	}, config.MarketConfig{
		CLOBBaseURL:    "http://127.0.0.1:1",
		RequestTimeout: 5 * time.Second,
	}, db, zap.NewNop(), signalSvc)

	return NewService(config.SimConfig{
		Interval:     30 * time.Second,
		OrderSize:    1.0,
		MinEdgePct:   5.0,
		MaxPositions: 20,
		AutoSettle:   true,
	}, db, zap.NewNop(), nil, nil, nil, tradeSvc)
}

func TestRunCyclePlacesTrades(t *testing.T) {
	db := setupSimTestDB(t)
	seedSimFixtures(t, db)
	svc := newSimServiceForTest(db)

	placed, errs := svc.placeTradesForSignals(context.Background())
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if placed != 2 {
		t.Fatalf("expected 2 trades placed, got %d", placed)
	}

	// Verify trades exist in DB
	var count int64
	db.Table("trades").Where("is_paper = ?", true).Count(&count)
	if count != 2 {
		t.Fatalf("expected 2 paper trades in db, got %d", count)
	}

	// Verify signals marked as acted_on
	var actedCount int64
	db.Table("signals").Where("acted_on = ?", true).Count(&actedCount)
	if actedCount != 2 {
		t.Fatalf("expected 2 signals acted_on, got %d", actedCount)
	}
}

func TestSettlePnLCalculation(t *testing.T) {
	tests := []struct {
		name       string
		outcome    string
		resolution string
		price      float64
		size       float64
		wantPnL    float64
	}{
		{name: "buy_yes_resolve_yes", outcome: "YES", resolution: "YES", price: 0.60, size: 10, wantPnL: 4.0},
		{name: "buy_yes_resolve_no", outcome: "YES", resolution: "NO", price: 0.60, size: 10, wantPnL: -6.0},
		{name: "buy_no_resolve_no", outcome: "NO", resolution: "NO", price: 0.40, size: 10, wantPnL: 6.0},
		{name: "buy_no_resolve_yes", outcome: "NO", resolution: "YES", price: 0.40, size: 10, wantPnL: -4.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := model.Trade{
				Price:     decimal.NewFromFloat(tt.price),
				Size:      decimal.NewFromFloat(tt.size),
				FillPrice: decimal.NullDecimal{Decimal: decimal.NewFromFloat(tt.price), Valid: true},
				FillSize:  decimal.NullDecimal{Decimal: decimal.NewFromFloat(tt.size), Valid: true},
				Outcome:   tt.outcome,
			}
			pnl := calculatePnL(tr, tt.resolution)
			got := pnl.InexactFloat64()
			if got < tt.wantPnL-0.01 || got > tt.wantPnL+0.01 {
				t.Fatalf("expected pnl=%.2f, got=%.4f", tt.wantPnL, got)
			}
		})
	}
}

func TestRunCycleSkipsDuplicate(t *testing.T) {
	db := setupSimTestDB(t)
	seedSimFixtures(t, db)
	svc := newSimServiceForTest(db)

	// First run
	placed1, _ := svc.placeTradesForSignals(context.Background())
	if placed1 != 2 {
		t.Fatalf("first run: expected 2, got %d", placed1)
	}

	// Reset signals to not acted_on to simulate re-run
	db.Exec("UPDATE signals SET acted_on = false")

	// Second run should skip because positions already exist
	placed2, _ := svc.placeTradesForSignals(context.Background())
	if placed2 != 0 {
		t.Fatalf("second run: expected 0 (duplicate skip), got %d", placed2)
	}
}

func TestSettleResolvedMarkets(t *testing.T) {
	db := setupSimTestDB(t)
	now := time.Now().UTC()

	// Insert a resolved market
	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, market_type, is_active, is_resolved, resolution, liquidity, token_id_yes, token_id_no, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-r1", "poly-r1", "Resolved?", "temperature_high", false, true, "YES", "2500.00", "r001", "r002", now.Add(-24*time.Hour),
	).Error; err != nil {
		t.Fatalf("insert resolved market: %v", err)
	}

	// Insert an open paper trade on that market
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, fill_price, fill_size, is_paper, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-r1", "paper-settle-1", "BUY", "YES", "0.60", "10.00", "6.00", "filled", "0.60", "10.00", true, now.Add(-2*time.Hour),
	).Error; err != nil {
		t.Fatalf("insert paper trade: %v", err)
	}

	svc := newSimServiceForTest(db)
	settled, errs := svc.settleResolvedMarkets(context.Background())
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if settled != 1 {
		t.Fatalf("expected 1 settled, got %d", settled)
	}

	// Verify PnL
	var tr struct {
		Status  string
		PnlUsdc *float64 `gorm:"column:pnl_usdc"`
	}
	db.Table("trades").Where("order_id = ?", "paper-settle-1").Scan(&tr)
	if tr.Status != "closed" {
		t.Fatalf("expected closed, got: %s", tr.Status)
	}
	if tr.PnlUsdc == nil {
		t.Fatalf("expected pnl_usdc to be set")
	}
	// buy YES at 0.60, resolved YES -> pnl = (1.0 - 0.60) * 10 = 4.0
	if *tr.PnlUsdc < 3.99 || *tr.PnlUsdc > 4.01 {
		t.Fatalf("expected pnl=4.0, got=%.4f", *tr.PnlUsdc)
	}
}

func TestGetReport(t *testing.T) {
	db := setupSimTestDB(t)
	now := time.Now().UTC()

	// Insert market
	db.Exec(
		"INSERT INTO markets(id, polymarket_id, question, market_type, is_active, liquidity, token_id_yes, token_id_no, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "poly-1", "Test?", "temperature_high", true, "2500.00", "1000001", "1000002", now.Add(48*time.Hour),
	)

	// Insert paper trades with PnL
	db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, pnl_usdc, is_paper, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "paper-r1", "BUY", "YES", "0.60", "10.00", "6.00", "closed", "4.00", true, now,
	)
	db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, pnl_usdc, is_paper, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "paper-r2", "BUY", "YES", "0.50", "8.00", "4.00", "closed", "-4.00", true, now.Add(1*time.Hour),
	)
	// Live trade should not appear in report
	db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, pnl_usdc, is_paper, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-live-1", "BUY", "YES", "0.55", "5.00", "2.75", "closed", "2.25", false, now.Add(2*time.Hour),
	)

	svc := newSimServiceForTest(db)
	report, err := svc.GetReport(context.Background(), ReportOptions{
		From: now.Add(-24 * time.Hour),
		To:   now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("get report: %v", err)
	}

	if report.TotalTrades != 2 {
		t.Fatalf("expected 2 total trades, got %d", report.TotalTrades)
	}
	if report.WinTrades != 1 || report.LossTrades != 1 {
		t.Fatalf("expected win=1 loss=1, got win=%d loss=%d", report.WinTrades, report.LossTrades)
	}
	if report.WinRate < 49.9 || report.WinRate > 50.1 {
		t.Fatalf("expected win rate 50%%, got %.1f%%", report.WinRate)
	}
	if report.RealizedPnLUSDC < -0.01 || report.RealizedPnLUSDC > 0.01 {
		t.Fatalf("expected realized pnl ~0, got %.4f", report.RealizedPnLUSDC)
	}
}
