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

func TestListPositions(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	now := time.Now().UTC()
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-b1", "BUY", "YES", "0.60", "10.00", "6.00", "filled", now,
	).Error; err != nil {
		t.Fatalf("insert trade buy: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-s1", "SELL", "YES", "0.70", "4.00", "2.80", "filled", now.Add(1*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert trade sell: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-2", "oid-b2", "BUY", "NO", "0.40", "5.00", "2.00", "filled", now.Add(2*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert trade market2: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		"market-1", "0.65", "0.35", now.Add(3*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert market price 1: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		"market-2", "0.55", "0.45", now.Add(3*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert market price 2: %v", err)
	}

	svc := newTradeServiceForTest(db, "http://127.0.0.1:1")
	items, err := svc.ListPositions(context.Background(), ListPositionsOptions{MarketID: "market-1"})
	if err != nil {
		t.Fatalf("list positions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 position, got %d", len(items))
	}
	if items[0].MarketID != "market-1" || items[0].Outcome != "YES" {
		t.Fatalf("unexpected position: %+v", items[0])
	}
	if items[0].NetSize != 6 {
		t.Fatalf("expected net size 6, got %.4f", items[0].NetSize)
	}
	if items[0].AvgEntryPrice < 0.599 || items[0].AvgEntryPrice > 0.601 {
		t.Fatalf("expected avg entry around 0.60, got %.4f", items[0].AvgEntryPrice)
	}
	if items[0].MarkPrice == nil || *items[0].MarkPrice <= 0 {
		t.Fatalf("expected mark price")
	}
	if items[0].LatestTradeAt.IsZero() {
		t.Fatalf("expected latest trade at")
	}
}

func TestGetPnLReport(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)

	base := time.Date(2026, 2, 20, 12, 0, 0, 0, time.UTC)
	inserts := []struct {
		OrderID   string
		Status    string
		CostUSDC  string
		PnLUSDC   *string
		CreatedAt time.Time
	}{
		{OrderID: "oid-1", Status: "filled", CostUSDC: "10.00", PnLUSDC: ptrString("1.20"), CreatedAt: base},
		{OrderID: "oid-2", Status: "closed", CostUSDC: "8.00", PnLUSDC: ptrString("-0.80"), CreatedAt: base.Add(2 * time.Hour)},
		{OrderID: "oid-3", Status: "closed", CostUSDC: "2.00", PnLUSDC: ptrString("0.00"), CreatedAt: base.Add(3 * time.Hour)},
		{OrderID: "oid-4", Status: "placed", CostUSDC: "2.00", PnLUSDC: nil, CreatedAt: base.Add(4 * time.Hour)},
	}
	for _, it := range inserts {
		if it.PnLUSDC == nil {
			if err := db.Exec(
				"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
				"market-1", it.OrderID, "BUY", "YES", "0.50", "1.00", it.CostUSDC, it.Status, it.CreatedAt,
			).Error; err != nil {
				t.Fatalf("insert trade: %v", err)
			}
			continue
		}
		if err := db.Exec(
			"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, pnl_usdc, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"market-1", it.OrderID, "BUY", "YES", "0.50", "1.00", it.CostUSDC, it.Status, *it.PnLUSDC, it.CreatedAt,
		).Error; err != nil {
			t.Fatalf("insert trade with pnl: %v", err)
		}
	}

	svc := newTradeServiceForTest(db, "http://127.0.0.1:1")
	report, err := svc.GetPnLReport(context.Background(), PnLReportOptions{
		From: base.Add(-24 * time.Hour),
		To:   base.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("pnl report: %v", err)
	}
	if report.TotalTrades != 4 {
		t.Fatalf("expected total trades=4, got %d", report.TotalTrades)
	}
	if report.FilledTrades != 3 {
		t.Fatalf("expected filled trades=3, got %d", report.FilledTrades)
	}
	if report.WinTrades != 1 || report.LossTrades != 1 {
		t.Fatalf("unexpected win/loss: %+v", report)
	}
	if report.BreakEvenTrades != 1 {
		t.Fatalf("expected break-even trades=1, got %d", report.BreakEvenTrades)
	}
	if report.RealizedPnLUSDC < 0.39 || report.RealizedPnLUSDC > 0.41 {
		t.Fatalf("expected realized pnl around 0.40, got %.4f", report.RealizedPnLUSDC)
	}
	if report.OpenExposureUSDC < 1.99 || report.OpenExposureUSDC > 2.01 {
		t.Fatalf("expected open exposure around 2.00, got %.4f", report.OpenExposureUSDC)
	}
	if len(report.Daily) == 0 {
		t.Fatalf("expected daily pnl rows")
	}
	if report.WinRate < 33.3 || report.WinRate > 33.4 {
		t.Fatalf("expected win rate around 33.33%%, got %.4f", report.WinRate)
	}
}

func TestListPositionsShortFlatAndNoPrice(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)
	now := time.Now().UTC()

	// market-1 YES -> net short position (SELL > BUY)
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-s-short", "SELL", "YES", "0.70", "10.00", "7.00", "filled", now,
	).Error; err != nil {
		t.Fatalf("insert short trade: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-1", "oid-b-cover", "BUY", "YES", "0.60", "4.00", "2.40", "filled", now.Add(1*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert cover trade: %v", err)
	}
	// market-2 NO -> flat position should be excluded
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-2", "oid-b-flat", "BUY", "NO", "0.30", "5.00", "1.50", "filled", now,
	).Error; err != nil {
		t.Fatalf("insert flat buy: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-2", "oid-s-flat", "SELL", "NO", "0.35", "5.00", "1.75", "filled", now.Add(1*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert flat sell: %v", err)
	}
	// only market-1 has latest mark price
	if err := db.Exec(
		"INSERT INTO market_prices(market_id, price_yes, price_no, captured_at) VALUES (?, ?, ?, ?)",
		"market-1", "0.80", "0.20", now.Add(2*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert market price: %v", err)
	}

	svc := newTradeServiceForTest(db, "http://127.0.0.1:1")
	items, err := svc.ListPositions(context.Background(), ListPositionsOptions{})
	if err != nil {
		t.Fatalf("list positions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 non-flat position, got %d", len(items))
	}
	pos := items[0]
	if pos.NetSize >= 0 {
		t.Fatalf("expected net short position, got %.4f", pos.NetSize)
	}
	if pos.UnrealizedPnL == nil {
		t.Fatalf("expected unrealized pnl for short")
	}
	if *pos.UnrealizedPnL >= 0 {
		t.Fatalf("expected negative unrealized pnl for short mark-up, got %.4f", *pos.UnrealizedPnL)
	}

	// market without price -> mark fields should be nil
	if err := db.Exec(
		"INSERT INTO trades(market_id, order_id, side, outcome, price, size, cost_usdc, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"market-3", "oid-no-price", "BUY", "YES", "0.40", "2.00", "0.80", "filled", now.Add(3*time.Minute),
	).Error; err != nil {
		t.Fatalf("insert no-price trade: %v", err)
	}
	items, err = svc.ListPositions(context.Background(), ListPositionsOptions{MarketID: "market-3"})
	if err != nil {
		t.Fatalf("list positions market-3: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 position for market-3, got %d", len(items))
	}
	if items[0].MarkPrice != nil || items[0].UnrealizedPnL != nil {
		t.Fatalf("expected nil mark/unrealized for no-price market: %+v", items[0])
	}
}

func TestGetPnLReportInvalidRange(t *testing.T) {
	db := setupTradeTestDB(t)
	seedTradeFixtures(t, db)
	svc := newTradeServiceForTest(db, "http://127.0.0.1:1")

	_, err := svc.GetPnLReport(context.Background(), PnLReportOptions{
		From: time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatalf("expected invalid range error")
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
		`CREATE TABLE market_prices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			market_id TEXT,
			price_yes DECIMAL(10,4),
			price_no DECIMAL(10,4),
			captured_at DATETIME
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

func ptrString(v string) *string {
	return &v
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
