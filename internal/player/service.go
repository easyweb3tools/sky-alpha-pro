package player

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
)

const (
	defaultLimit = 20
	maxLimit     = 500
)

var (
	ErrPlayerBadRequest = errors.New("player bad request")
	ErrPlayerNotFound   = errors.New("player not found")
)

type Service struct {
	db  *gorm.DB
	log *zap.Logger
}

type competitorSummary struct {
	Address        string
	TotalTrades    int
	TotalMarkets   int
	WeatherMarkets int
	TotalVolume    decimal.Decimal
	LastSeenAt     *time.Time
}

type positionSummary struct {
	Address    string
	MarketID   string
	Outcome    string
	Size       decimal.Decimal
	FirstEntry *time.Time
	LastUpdate *time.Time
}

func NewService(db *gorm.DB, log *zap.Logger) *Service {
	return &Service{db: db, log: log}
}

func (s *Service) ListPlayers(ctx context.Context, opts ListOptions) ([]PlayerView, error) {
	if err := s.seedPlayersIfEmpty(ctx, opts.Limit); err != nil {
		return nil, err
	}

	limit := normalizeLimit(opts.Limit)
	q := s.db.WithContext(ctx).Model(&model.Player{})
	if opts.MinWeatherMarket > 0 {
		q = q.Where("weather_markets >= ?", opts.MinWeatherMarket)
	}

	var rows []model.Player
	if err := q.Order("rank_weather ASC").Order("rank_overall ASC").Order("last_active_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]PlayerView, 0, len(rows))
	for _, r := range rows {
		items = append(items, mapPlayerView(r))
	}
	return items, nil
}

func (s *Service) GetPlayer(ctx context.Context, address string) (*PlayerView, error) {
	if err := s.seedPlayersIfEmpty(ctx, defaultLimit); err != nil {
		return nil, err
	}
	addr := normalizeAddress(address)
	if addr == "" {
		return nil, fmt.Errorf("%w: address is required", ErrPlayerBadRequest)
	}
	var row model.Player
	if err := s.db.WithContext(ctx).Where("address = ?", addr).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: player not found", ErrPlayerNotFound)
		}
		return nil, err
	}
	view := mapPlayerView(row)
	return &view, nil
}

func (s *Service) ListPlayerPositions(ctx context.Context, address string, opts PositionOptions) ([]PlayerPositionView, error) {
	if err := s.seedPlayersIfEmpty(ctx, defaultLimit); err != nil {
		return nil, err
	}
	addr := normalizeAddress(address)
	if addr == "" {
		return nil, fmt.Errorf("%w: address is required", ErrPlayerBadRequest)
	}

	var playerRow model.Player
	if err := s.db.WithContext(ctx).Where("address = ?", addr).First(&playerRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: player not found", ErrPlayerNotFound)
		}
		return nil, err
	}

	var rows []model.PlayerPosition
	if err := s.db.WithContext(ctx).
		Where("player_id = ?", playerRow.ID).
		Order("last_update_at DESC").
		Limit(normalizeLimit(opts.Limit)).
		Find(&rows).Error; err != nil {
		return nil, err
	}

	items := make([]PlayerPositionView, 0, len(rows))
	for _, r := range rows {
		v := PlayerPositionView{
			MarketID:     r.MarketID,
			Outcome:      r.Outcome,
			Size:         r.Size.InexactFloat64(),
			FirstEntryAt: r.FirstEntryAt,
			LastUpdateAt: r.LastUpdateAt,
		}
		if r.AvgPrice.Valid {
			x := r.AvgPrice.Decimal.InexactFloat64()
			v.AvgPrice = &x
		}
		if r.CurrentValue.Valid {
			x := r.CurrentValue.Decimal.InexactFloat64()
			v.CurrentValue = &x
		}
		if r.PnL.Valid {
			x := r.PnL.Decimal.InexactFloat64()
			v.PnL = &x
		}
		items = append(items, v)
	}
	return items, nil
}

func (s *Service) GetLeaderboard(ctx context.Context, opts LeaderboardOptions) ([]PlayerView, error) {
	if err := s.seedPlayersIfEmpty(ctx, opts.Limit); err != nil {
		return nil, err
	}
	limit := normalizeLimit(opts.Limit)
	leaderType := strings.TrimSpace(strings.ToLower(opts.Type))

	q := s.db.WithContext(ctx).Model(&model.Player{})
	switch leaderType {
	case "", "weather":
		q = q.Order("rank_weather ASC").Order("weather_markets DESC")
	case "overall":
		q = q.Order("rank_overall ASC").Order("total_markets DESC")
	default:
		return nil, fmt.Errorf("%w: unsupported leaderboard type", ErrPlayerBadRequest)
	}

	var rows []model.Player
	if err := q.Order("last_active_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]PlayerView, 0, len(rows))
	for _, r := range rows {
		items = append(items, mapPlayerView(r))
	}
	return items, nil
}

func (s *Service) CompareWithMyStrategy(ctx context.Context, address string) (*CompareView, error) {
	playerView, err := s.GetPlayer(ctx, address)
	if err != nil {
		return nil, err
	}

	type statRow struct {
		FilledTrades int64               `gorm:"column:filled_trades"`
		WinTrades    int64               `gorm:"column:win_trades"`
		LossTrades   int64               `gorm:"column:loss_trades"`
		RealizedPnL  decimal.NullDecimal `gorm:"column:realized_pnl"`
	}
	var st statRow
	if err := s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Select(
			"COUNT(*) AS filled_trades",
			"SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS win_trades",
			"SUM(CASE WHEN pnl_usdc < 0 THEN 1 ELSE 0 END) AS loss_trades",
			"COALESCE(SUM(pnl_usdc), 0) AS realized_pnl",
		).
		Where("status IN ?", []string{"filled", "partially_filled", "closed"}).
		Where("pnl_usdc IS NOT NULL").
		Scan(&st).Error; err != nil {
		return nil, err
	}

	myWinRate := 0.0
	decision := st.WinTrades + st.LossTrades
	if decision > 0 {
		myWinRate = float64(st.WinTrades) * 100 / float64(decision)
	}
	myPnL := 0.0
	if st.RealizedPnL.Valid {
		myPnL = st.RealizedPnL.Decimal.InexactFloat64()
	}
	playerPnL := 0.0
	if playerView.TotalPnL != nil {
		playerPnL = *playerView.TotalPnL
	}

	return &CompareView{
		PlayerAddress:   playerView.Address,
		PlayerWinRate:   playerView.WinRate,
		PlayerTotalPnL:  playerPnL,
		PlayerMarkets:   playerView.TotalMarkets,
		MyWinRate:       myWinRate,
		MyRealizedPnL:   myPnL,
		MyFilledTrades:  int(st.FilledTrades),
		WinRateDiff:     playerView.WinRate - myWinRate,
		RealizedPnLDiff: playerPnL - myPnL,
	}, nil
}

func (s *Service) RefreshFromCompetitors(ctx context.Context, opts RefreshOptions) (*RefreshResult, error) {
	started := time.Now().UTC()
	result := &RefreshResult{StartedAt: started}
	limit := normalizeLimit(opts.Limit)

	summaries, err := s.loadCompetitorSummaries(ctx, limit)
	if err != nil {
		return nil, err
	}
	if len(summaries) == 0 {
		result.FinishedAt = time.Now().UTC()
		return result, nil
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].WeatherMarkets != summaries[j].WeatherMarkets {
			return summaries[i].WeatherMarkets > summaries[j].WeatherMarkets
		}
		if summaries[i].TotalMarkets != summaries[j].TotalMarkets {
			return summaries[i].TotalMarkets > summaries[j].TotalMarkets
		}
		return summaries[i].TotalTrades > summaries[j].TotalTrades
	})
	weatherRank := make(map[string]int, len(summaries))
	for idx, s := range summaries {
		weatherRank[s.Address] = idx + 1
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].TotalMarkets != summaries[j].TotalMarkets {
			return summaries[i].TotalMarkets > summaries[j].TotalMarkets
		}
		if summaries[i].TotalTrades != summaries[j].TotalTrades {
			return summaries[i].TotalTrades > summaries[j].TotalTrades
		}
		return summaries[i].WeatherMarkets > summaries[j].WeatherMarkets
	})
	overallRank := make(map[string]int, len(summaries))
	for idx, s := range summaries {
		overallRank[s.Address] = idx + 1
	}

	positions, err := s.loadPositionSummaries(ctx, limit)
	if err != nil {
		return nil, err
	}

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		playerIDByAddr := make(map[string]uint64, len(summaries))
		now := time.Now().UTC()

		for _, sum := range summaries {
			row := model.Player{
				Address:        sum.Address,
				Username:       maskAddress(sum.Address),
				WinRate:        0,
				TotalMarkets:   sum.TotalMarkets,
				WeatherMarkets: sum.WeatherMarkets,
				RankOverall:    overallRank[sum.Address],
				RankWeather:    weatherRank[sum.Address],
				LastActiveAt:   sum.LastSeenAt,
				UpdatedAt:      now,
				CreatedAt:      now,
			}
			if sum.TotalVolume.GreaterThan(decimal.Zero) {
				row.TotalPnL = decimal.NewNullDecimal(sum.TotalVolume)
			}

			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "address"}},
				DoUpdates: clause.Assignments(map[string]any{
					"username":        row.Username,
					"total_pnl":       row.TotalPnL,
					"win_rate":        row.WinRate,
					"total_markets":   row.TotalMarkets,
					"weather_markets": row.WeatherMarkets,
					"rank_overall":    row.RankOverall,
					"rank_weather":    row.RankWeather,
					"last_active_at":  row.LastActiveAt,
					"updated_at":      row.UpdatedAt,
				}),
			}).Create(&row).Error; err != nil {
				return err
			}
			result.PlayersUpserted++

			var persisted model.Player
			if err := tx.Where("address = ?", row.Address).First(&persisted).Error; err != nil {
				return err
			}
			playerIDByAddr[row.Address] = persisted.ID
		}

		for _, pos := range positions {
			pid := playerIDByAddr[pos.Address]
			if pid == 0 || pos.MarketID == "" || pos.Outcome == "" {
				continue
			}
			lastUpdate := time.Now().UTC()
			if pos.LastUpdate != nil {
				lastUpdate = pos.LastUpdate.UTC()
			}
			entry := model.PlayerPosition{
				PlayerID:     pid,
				MarketID:     pos.MarketID,
				Outcome:      pos.Outcome,
				Size:         pos.Size,
				CurrentValue: decimal.NewNullDecimal(pos.Size),
				FirstEntryAt: pos.FirstEntry,
				LastUpdateAt: lastUpdate,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "player_id"}, {Name: "market_id"}, {Name: "outcome"}},
				DoUpdates: clause.Assignments(map[string]any{
					"size":           entry.Size,
					"current_value":  entry.CurrentValue,
					"first_entry_at": entry.FirstEntryAt,
					"last_update_at": entry.LastUpdateAt,
				}),
			}).Create(&entry).Error; err != nil {
				return err
			}
			result.PositionsUpserted++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func (s *Service) loadCompetitorSummaries(ctx context.Context, limit int) ([]competitorSummary, error) {
	type row struct {
		Address        string  `gorm:"column:address"`
		TotalTrades    int     `gorm:"column:total_trades"`
		TotalMarkets   int     `gorm:"column:total_markets"`
		WeatherMarkets int     `gorm:"column:weather_markets"`
		TotalVolume    float64 `gorm:"column:total_volume"`
		LastSeenAt     string  `gorm:"column:last_seen_at"`
	}
	rows := make([]row, 0)
	if err := s.db.WithContext(ctx).
		Table("competitors AS c").
		Select(`
			c.address AS address,
			COUNT(ct.id) AS total_trades,
			COUNT(DISTINCT CASE WHEN ct.market_id <> '' THEN ct.market_id END) AS total_markets,
			COUNT(DISTINCT CASE WHEN m.market_type IN ('temperature_high','temperature_low') THEN ct.market_id END) AS weather_markets,
			COALESCE(SUM(ct.amount_usdc), 0) AS total_volume,
			MAX(ct.timestamp) AS last_seen_at
		`).
		Joins("JOIN competitor_trades ct ON ct.competitor_id = c.id").
		Joins("LEFT JOIN markets m ON m.id = ct.market_id").
		Group("c.address").
		Order("total_markets DESC").
		Order("total_trades DESC").
		Limit(limit).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]competitorSummary, 0, len(rows))
	for _, r := range rows {
		t, parseErr := parseDBTime(r.LastSeenAt)
		if parseErr != nil {
			return nil, parseErr
		}
		out = append(out, competitorSummary{
			Address:        normalizeAddress(r.Address),
			TotalTrades:    r.TotalTrades,
			TotalMarkets:   r.TotalMarkets,
			WeatherMarkets: r.WeatherMarkets,
			TotalVolume:    decimal.NewFromFloat(r.TotalVolume),
			LastSeenAt:     t,
		})
	}
	return out, nil
}

func (s *Service) loadPositionSummaries(ctx context.Context, limit int) ([]positionSummary, error) {
	type row struct {
		Address    string  `gorm:"column:address"`
		MarketID   string  `gorm:"column:market_id"`
		Outcome    string  `gorm:"column:outcome"`
		Size       float64 `gorm:"column:size"`
		FirstEntry string  `gorm:"column:first_entry_at"`
		LastUpdate string  `gorm:"column:last_update_at"`
	}
	rows := make([]row, 0)
	if err := s.db.WithContext(ctx).
		Table("competitors AS c").
		Select(`
			c.address AS address,
			ct.market_id AS market_id,
			ct.outcome AS outcome,
			COALESCE(SUM(ct.amount_usdc), 0) AS size,
			MIN(ct.timestamp) AS first_entry_at,
			MAX(ct.timestamp) AS last_update_at
		`).
		Joins("JOIN competitor_trades ct ON ct.competitor_id = c.id").
		Where("ct.market_id <> '' AND ct.outcome <> ''").
		Group("c.address, ct.market_id, ct.outcome").
		Order("last_update_at DESC").
		Limit(limit * 20).
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]positionSummary, 0, len(rows))
	for _, r := range rows {
		first, firstErr := parseDBTime(r.FirstEntry)
		if firstErr != nil {
			return nil, firstErr
		}
		last, lastErr := parseDBTime(r.LastUpdate)
		if lastErr != nil {
			return nil, lastErr
		}
		out = append(out, positionSummary{
			Address:    normalizeAddress(r.Address),
			MarketID:   strings.TrimSpace(r.MarketID),
			Outcome:    strings.ToUpper(strings.TrimSpace(r.Outcome)),
			Size:       decimal.NewFromFloat(r.Size),
			FirstEntry: first,
			LastUpdate: last,
		})
	}
	return out, nil
}

func (s *Service) seedPlayersIfEmpty(ctx context.Context, limit int) error {
	var count int64
	if err := s.db.WithContext(ctx).Model(&model.Player{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	_, err := s.RefreshFromCompetitors(ctx, RefreshOptions{Limit: limit})
	if err == nil || s.log == nil {
		return err
	}
	s.log.Warn("refresh players from competitors failed", zap.Error(err))
	return err
}

func normalizeAddress(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

func mapPlayerView(row model.Player) PlayerView {
	v := PlayerView{
		Address:        row.Address,
		Username:       row.Username,
		WinRate:        row.WinRate,
		TotalMarkets:   row.TotalMarkets,
		WeatherMarkets: row.WeatherMarkets,
		RankOverall:    row.RankOverall,
		RankWeather:    row.RankWeather,
		LastActiveAt:   row.LastActiveAt,
		UpdatedAt:      row.UpdatedAt,
	}
	if row.TotalPnL.Valid {
		x := row.TotalPnL.Decimal.InexactFloat64()
		v.TotalPnL = &x
	}
	return v
}

func maskAddress(address string) string {
	a := normalizeAddress(address)
	if len(a) <= 12 {
		return a
	}
	return a[:8] + "..." + a[len(a)-4:]
}

func parseDBTime(raw string) (*time.Time, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return nil, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, v); err == nil {
			t := ts.UTC()
			return &t, nil
		}
	}
	return nil, fmt.Errorf("%w: unsupported time format: %s", ErrPlayerBadRequest, v)
}
