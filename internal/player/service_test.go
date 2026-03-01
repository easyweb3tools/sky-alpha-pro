package player

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"sky-alpha-pro/internal/model"
)

func TestRefreshFromCompetitorsAndList(t *testing.T) {
	db := setupPlayerTestDB(t)
	now := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)

	if err := db.Exec(`
		INSERT INTO competitors (
			id, address, is_bot, bot_confidence, total_trades, total_volume,
			avg_trade_interval_sec, created_at, updated_at
		) VALUES
			(1, '0xaaa0000000000000000000000000000000000001', 0, 0, 0, 0, 0, ?, ?),
			(2, '0xbbb0000000000000000000000000000000000002', 0, 0, 0, 0, 0, ?, ?)
	`, now, now, now, now).Error; err != nil {
		t.Fatalf("seed competitors: %v", err)
	}

	trades := []model.CompetitorTrade{
		{CompetitorID: 1, MarketID: stringPtr("m1"), Outcome: "YES", AmountUSDC: decimal.NewNullDecimal(decimal.NewFromFloat(12.3)), Timestamp: now.Add(-2 * time.Hour), TxHash: "0x01", BlockNumber: 1, CreatedAt: now},
		{CompetitorID: 1, MarketID: stringPtr("m2"), Outcome: "NO", AmountUSDC: decimal.NewNullDecimal(decimal.NewFromFloat(8.1)), Timestamp: now.Add(-1 * time.Hour), TxHash: "0x02", BlockNumber: 2, CreatedAt: now},
		{CompetitorID: 2, MarketID: stringPtr("m1"), Outcome: "NO", AmountUSDC: decimal.NewNullDecimal(decimal.NewFromFloat(3.2)), Timestamp: now.Add(-30 * time.Minute), TxHash: "0x03", BlockNumber: 3, CreatedAt: now},
	}
	if err := db.Create(&trades).Error; err != nil {
		t.Fatalf("seed competitor trades: %v", err)
	}

	if err := db.Exec(`
		INSERT INTO markets (id, market_type) VALUES
			('m1', 'temperature_high'),
			('m2', 'temperature_low')
	`).Error; err != nil {
		t.Fatalf("seed markets: %v", err)
	}

	svc := NewService(db, zap.NewNop())
	res, err := svc.RefreshFromCompetitors(context.Background(), RefreshOptions{Limit: 10})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if res.PlayersUpserted != 2 {
		t.Fatalf("expected 2 players upserted, got %d", res.PlayersUpserted)
	}
	if res.PositionsUpserted == 0 {
		t.Fatalf("expected positions upserted")
	}

	players, err := svc.ListPlayers(context.Background(), ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("list players: %v", err)
	}
	if len(players) != 2 {
		t.Fatalf("expected 2 players, got %d", len(players))
	}
	if players[0].Address != "0xaaa0000000000000000000000000000000000001" {
		t.Fatalf("unexpected top player: %s", players[0].Address)
	}

	show, err := svc.GetPlayer(context.Background(), "0xAAA0000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("get player: %v", err)
	}
	if show.TotalMarkets != 2 {
		t.Fatalf("expected total_markets 2, got %d", show.TotalMarkets)
	}
	if show.TotalPnL != nil {
		t.Fatalf("expected total_pnl unavailable for now")
	}
	if show.TotalVolume == nil || *show.TotalVolume <= 0 {
		t.Fatalf("expected total_volume available")
	}

	positions, err := svc.ListPlayerPositions(context.Background(), show.Address, PositionOptions{Limit: 10})
	if err != nil {
		t.Fatalf("list positions: %v", err)
	}
	if len(positions) == 0 {
		t.Fatalf("expected non-empty positions")
	}

	leaderboard, err := svc.GetLeaderboard(context.Background(), LeaderboardOptions{Limit: 10, Type: "weather"})
	if err != nil {
		t.Fatalf("leaderboard: %v", err)
	}
	if len(leaderboard) != 2 {
		t.Fatalf("expected leaderboard 2, got %d", len(leaderboard))
	}
}

func TestCompareWithMyStrategy(t *testing.T) {
	db := setupPlayerTestDB(t)
	now := time.Now().UTC()

	p := model.Player{Address: "0x1111111111111111111111111111111111111111", WinRate: 70, TotalMarkets: 5, CreatedAt: now, UpdatedAt: now}
	if err := db.Create(&p).Error; err != nil {
		t.Fatalf("seed player: %v", err)
	}
	trades := []model.Trade{
		{OrderID: "ord-1", MarketID: "m1", Side: "BUY", Outcome: "YES", Price: decimal.NewFromFloat(0.4), Size: decimal.NewFromFloat(10), CostUSDC: decimal.NewFromFloat(4), Status: "filled", PnLUSDC: decimal.NewNullDecimal(decimal.NewFromFloat(2.0)), CreatedAt: now},
		{OrderID: "ord-2", MarketID: "m2", Side: "BUY", Outcome: "NO", Price: decimal.NewFromFloat(0.6), Size: decimal.NewFromFloat(10), CostUSDC: decimal.NewFromFloat(6), Status: "closed", PnLUSDC: decimal.NewNullDecimal(decimal.NewFromFloat(-1.0)), CreatedAt: now},
	}
	if err := db.Create(&trades).Error; err != nil {
		t.Fatalf("seed trades: %v", err)
	}

	svc := NewService(db, zap.NewNop())
	cmp, err := svc.CompareWithMyStrategy(context.Background(), p.Address)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if cmp.MyFilledTrades != 2 {
		t.Fatalf("expected filled trades 2, got %d", cmp.MyFilledTrades)
	}
	if cmp.MyWinRate <= 0 {
		t.Fatalf("expected positive my win rate")
	}
	if cmp.PlayerTotalPnL != nil {
		t.Fatalf("expected player pnl unavailable")
	}
	if cmp.RealizedPnLDiff != nil {
		t.Fatalf("expected realized pnl diff unavailable")
	}
}

func TestRefreshFromCompetitorsEmptySource(t *testing.T) {
	db := setupPlayerTestDB(t)
	svc := NewService(db, zap.NewNop())

	res, err := svc.RefreshFromCompetitors(context.Background(), RefreshOptions{Limit: 10})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if res.PlayersUpserted != 0 || res.PositionsUpserted != 0 {
		t.Fatalf("expected zero upserts, got players=%d positions=%d", res.PlayersUpserted, res.PositionsUpserted)
	}
}

func TestGetLeaderboardInvalidType(t *testing.T) {
	db := setupPlayerTestDB(t)
	svc := NewService(db, zap.NewNop())

	_, err := svc.GetLeaderboard(context.Background(), LeaderboardOptions{Limit: 10, Type: "invalid"})
	if err == nil {
		t.Fatalf("expected invalid type error")
	}
	if !errors.Is(err, ErrPlayerBadRequest) {
		t.Fatalf("expected ErrPlayerBadRequest, got %v", err)
	}
}

func TestGetPlayerNotFound(t *testing.T) {
	db := setupPlayerTestDB(t)
	svc := NewService(db, zap.NewNop())

	_, err := svc.GetPlayer(context.Background(), "0x9999999999999999999999999999999999999999")
	if err == nil {
		t.Fatalf("expected not found")
	}
	if !errors.Is(err, ErrPlayerNotFound) {
		t.Fatalf("expected ErrPlayerNotFound, got %v", err)
	}
}

func TestRefreshIdempotent(t *testing.T) {
	db := setupPlayerTestDB(t)
	now := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)
	if err := db.Exec(`
		INSERT INTO competitors (
			id, address, is_bot, bot_confidence, total_trades, total_volume,
			avg_trade_interval_sec, created_at, updated_at
		) VALUES
			(1, '0xaaa0000000000000000000000000000000000001', 0, 0, 0, 0, 0, ?, ?)
	`, now, now).Error; err != nil {
		t.Fatalf("seed competitors: %v", err)
	}
	if err := db.Exec(`
		INSERT INTO competitor_trades (
			competitor_id, market_id, outcome, side, amount_usdc, timestamp, tx_hash, block_number, created_at
		) VALUES
			(1, 'm1', 'YES', 'BUY', 10, ?, '0xabc', 1, ?)
	`, now, now).Error; err != nil {
		t.Fatalf("seed competitor trades: %v", err)
	}
	if err := db.Exec(`INSERT INTO markets (id, market_type) VALUES ('m1', 'temperature_high')`).Error; err != nil {
		t.Fatalf("seed markets: %v", err)
	}

	svc := NewService(db, zap.NewNop())
	if _, err := svc.RefreshFromCompetitors(context.Background(), RefreshOptions{Limit: 10}); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if _, err := svc.RefreshFromCompetitors(context.Background(), RefreshOptions{Limit: 10}); err != nil {
		t.Fatalf("second refresh: %v", err)
	}

	var playerCount int64
	if err := db.Model(&model.Player{}).Count(&playerCount).Error; err != nil {
		t.Fatalf("count players: %v", err)
	}
	if playerCount != 1 {
		t.Fatalf("expected 1 player row after repeated refresh, got %d", playerCount)
	}

	var positionCount int64
	if err := db.Model(&model.PlayerPosition{}).Count(&positionCount).Error; err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if positionCount != 1 {
		t.Fatalf("expected 1 player position row after repeated refresh, got %d", positionCount)
	}
}

func setupPlayerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.Competitor{}, &model.CompetitorTrade{}, &model.Player{}, &model.PlayerPosition{}, &model.Trade{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Exec(`CREATE TABLE markets (id TEXT PRIMARY KEY, market_type TEXT)`).Error; err != nil {
		t.Fatalf("create markets table: %v", err)
	}
	return db
}

func stringPtr(v string) *string {
	return &v
}
