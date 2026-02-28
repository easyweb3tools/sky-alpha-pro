package trade

import (
	"context"
	"encoding/json"
	"errors"
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
	var sawOrderSchema bool
	clob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/order":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			order, ok := payload["order"].(map[string]any)
			if !ok {
				t.Fatalf("order payload missing")
			}
			if order["tokenId"] == nil || order["makerAmount"] == nil || order["takerAmount"] == nil {
				t.Fatalf("unexpected order schema: %#v", order)
			}
			sawOrderSchema = true
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
	if !sawOrderSchema || !cancelCalled || !cancel.Cancelled {
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
	if !errors.Is(err, ErrTradeRiskRejected) {
		t.Fatalf("expected ErrTradeRiskRejected, got: %v", err)
	}
}

func TestCancelOrderRejectsNonCancellableStatus(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	now := time.Now().UTC()
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-filled", "BUY", "YES", "0.50", "1.00", "0.50", "filled", now,
	).Error; err != nil {
		t.Fatalf("insert trade: %v", err)
	}

	clob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected clob call for non-cancellable status")
	}))
	defer clob.Close()

	svc := newTradeServiceForTest(db, clob.URL)
	_, err := svc.CancelOrder(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected cancel rejection")
	}
	if !errors.Is(err, ErrTradeNotCancellable) {
		t.Fatalf("expected ErrTradeNotCancellable, got: %v", err)
	}
}

func TestListTradesWithFilters(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	now := time.Now().UTC()
	inserts := []struct {
		MarketID string
		OrderID  string
		Status   string
	}{
		{MarketID: "market-1", OrderID: "oid-1", Status: "placed"},
		{MarketID: "market-2", OrderID: "oid-2", Status: "filled"},
		{MarketID: "market-1", OrderID: "oid-3", Status: "filled"},
	}
	for _, it := range inserts {
		if err := db.Exec(
			"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			it.MarketID, it.OrderID, "BUY", "YES", "0.50", "1.00", "0.50", it.Status, now,
		).Error; err != nil {
			t.Fatalf("insert trade: %v", err)
		}
	}

	svc := newTradeServiceForTest(db, "http://127.0.0.1:1")
	filtered, err := svc.ListTrades(context.Background(), ListTradesOptions{
		Limit:    10,
		Status:   "filled",
		MarketID: "market-1",
	})
	if err != nil {
		t.Fatalf("list trades: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(filtered))
	}
	if filtered[0].OrderID != "oid-3" {
		t.Fatalf("unexpected trade: %+v", filtered[0])
	}

	_, err = svc.ListTrades(context.Background(), ListTradesOptions{Limit: 10, Status: "bad_status"})
	if err == nil {
		t.Fatalf("expected invalid status error")
	}
	if !errors.Is(err, ErrTradeInvalidRequest) {
		t.Fatalf("expected ErrTradeInvalidRequest, got: %v", err)
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
			liquidity DECIMAL(18,2),
			token_id_yes TEXT,
			token_id_no TEXT
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
		"INSERT INTO markets(id, polymarket_id, is_active, liquidity, token_id_yes, token_id_no) VALUES (?, ?, ?, ?, ?, ?)",
		"market-1", "poly-1", true, "2500.00", "1000001", "1000002",
	).Error; err != nil {
		t.Fatalf("insert market: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO markets(id, polymarket_id, is_active, liquidity, token_id_yes, token_id_no) VALUES (?, ?, ?, ?, ?, ?)",
		"market-2", "poly-2", true, "2500.00", "2000001", "2000002",
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
		MaxOrderSize:         0,
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
