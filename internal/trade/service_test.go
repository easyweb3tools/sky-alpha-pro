package trade

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/config"
)

func TestSubmitOrderAndCancel(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	var cancelCalled bool
	clob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/order":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"orderID":"oid-1"}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/order/oid-1":
			cancelCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer clob.Close()

	svc := newTradeServiceForTest(db, clob.URL)

	submit, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		MarketRef: "market-1",
		Side:      "BUY",
		Outcome:   "YES",
		Price:     0.62,
		Size:      10,
		Confirm:   true,
	})
	if err != nil {
		t.Fatalf("submit order: %v", err)
	}
	if submit.OrderID == "" || submit.TradeID == 0 {
		t.Fatalf("unexpected submit response: %+v", submit)
	}

	cancel, err := svc.CancelOrder(context.Background(), submit.TradeID)
	if err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	if !cancelCalled || !cancel.Cancelled {
		t.Fatalf("expected cancel to be called")
	}
}

func TestSubmitOrderRiskReject(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	clob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"orderID":"oid-2"}`))
	}))
	defer clob.Close()

	svc := newTradeServiceForTest(db, clob.URL)
	_, err := svc.SubmitOrder(context.Background(), SubmitOrderRequest{
		MarketRef: "market-1",
		Side:      "BUY",
		Outcome:   "YES",
		Price:     0.62,
		Size:      1000,
		Confirm:   true,
	})
	if err == nil {
		t.Fatalf("expected risk reject")
	}
}

func setupTradeTestDB(t *testing.T) *gorm.DB {
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
			is_active NUMERIC,
			liquidity DECIMAL(18,2)
		)`,
		`CREATE TABLE signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			edge_pct DECIMAL(8,4),
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
			executed_at DATETIME,
			created_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

func seedTradeFixtures(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC()
	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, is_active, liquidity) VALUES (?, ?, ?, ?)",
		"market-1", "poly-1", true, "2500.00",
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO signals(market_id, edge_pct, created_at) VALUES (?, ?, ?)",
		"market-1", "8.50", now,
	).Error; err != nil {
		t.Fatalf("insert signal: %v", err)
	}
}

func newTradeServiceForTest(db *gorm.DB, clobURL string) *Service {
	signalSvc := signal.NewService(config.SignalConfig{
		MinEdgePct:          1,
		MaxMarkets:          10,
		DefaultLimit:        10,
		Concurrency:         2,
		ForecastMaxAgeHours: 24,
		MinSigma:            0.5,
	}, db, zap.NewNop())

	return NewService(config.TradeConfig{
		PrivateKeyHex:        "4c0883a69102937d6231471b5dbb6204fe512961708279f7b00f6d0f7a4f2f5f",
		ChainID:              137,
		MaxPositionSize:      100,
		MaxDailyLoss:         50,
		MinEdgePct:           5,
		MaxOpenPositions:     10,
		MinLiquidity:         1000,
		ConfirmationRequired: true,
	}, config.MarketConfig{
		CLOBBaseURL:    clobURL,
		RequestTimeout: 5 * time.Second,
	}, db, zap.NewNop(), signalSvc)
}
