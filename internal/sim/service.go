package sim

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

type Service struct {
	cfg        config.SimConfig
	db         *gorm.DB
	log        *zap.Logger
	marketSvc  *market.Service
	weatherSvc *weather.Service
	signalSvc  *signal.Service
	tradeSvc   *trade.Service
}

func NewService(
	cfg config.SimConfig,
	db *gorm.DB,
	log *zap.Logger,
	marketSvc *market.Service,
	weatherSvc *weather.Service,
	signalSvc *signal.Service,
	tradeSvc *trade.Service,
) *Service {
	return &Service{
		cfg:        cfg,
		db:         db,
		log:        log,
		marketSvc:  marketSvc,
		weatherSvc: weatherSvc,
		signalSvc:  signalSvc,
		tradeSvc:   tradeSvc,
	}
}

func (s *Service) RunCycle(ctx context.Context, cycleNum int) (*CycleResult, error) {
	result := &CycleResult{
		Cycle:  cycleNum,
		Errors: make([]string, 0),
	}

	// 1. Sync markets and prices
	syncRes, err := s.marketSvc.SyncMarkets(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("market sync: %v", err))
		s.log.Warn("sim cycle market sync failed", zap.Int("cycle", cycleNum), zap.Error(err))
	} else {
		result.MarketsSynced = syncRes.MarketsUpserted
		for _, e := range syncRes.Errors {
			result.Errors = append(result.Errors, "market: "+e)
		}
	}

	// 2. Refresh stale forecasts for active weather markets
	s.refreshStaleForecasts(ctx, result)

	// 3. Generate signals
	sigRes, err := s.signalSvc.GenerateSignals(ctx, signal.GenerateOptions{})
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("signal generate: %v", err))
		s.log.Warn("sim cycle signal generation failed", zap.Int("cycle", cycleNum), zap.Error(err))
	} else {
		result.SignalsGenerated = sigRes.Generated
		for _, e := range sigRes.Errors {
			result.Errors = append(result.Errors, "signal: "+e)
		}
	}

	// 4. Place trades for actionable signals
	placed, placeErrs := s.placeTradesForSignals(ctx)
	result.TradesPlaced = placed
	result.Errors = append(result.Errors, placeErrs...)

	// 5. Settle resolved markets
	if s.cfg.AutoSettle {
		settled, settleErrs := s.settleResolvedMarkets(ctx)
		result.TradesSettled = settled
		result.Errors = append(result.Errors, settleErrs...)
	}

	return result, nil
}

func (s *Service) refreshStaleForecasts(ctx context.Context, result *CycleResult) {
	var markets []model.Market
	if err := s.db.WithContext(ctx).
		Where("is_active = ? AND market_type IN ?", true, []string{"temperature_high", "temperature_low"}).
		Find(&markets).Error; err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("load weather markets: %v", err))
		return
	}

	maxAge := 6 * time.Hour
	now := time.Now().UTC()

	for _, m := range markets {
		if m.City == "" {
			continue
		}
		var latest model.Forecast
		err := s.db.WithContext(ctx).
			Where("city = ?", m.City).
			Order("fetched_at DESC").
			First(&latest).Error
		if err == nil && now.Sub(latest.FetchedAt) < maxAge {
			continue
		}

		_, fErr := s.weatherSvc.GetForecast(ctx, weather.ForecastRequest{
			Location: m.City,
			Days:     7,
		})
		if fErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("forecast refresh %s: %v", m.City, fErr))
		}
	}
}

func (s *Service) placeTradesForSignals(ctx context.Context) (int, []string) {
	var errs []string
	placed := 0

	minEdge := s.cfg.MinEdgePct
	if minEdge <= 0 {
		minEdge = 5.0
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	var signals []model.Signal
	if err := s.db.WithContext(ctx).
		Where("acted_on = ? AND created_at >= ? AND ABS(edge_pct) >= ?", false, cutoff, minEdge).
		Order("ABS(edge_pct) DESC").
		Limit(50).
		Find(&signals).Error; err != nil {
		return 0, []string{fmt.Sprintf("load signals: %v", err)}
	}

	isPaper := true
	for _, sig := range signals {
		// Check max positions
		if s.cfg.MaxPositions > 0 {
			positions, err := s.tradeSvc.ListPositions(ctx, trade.ListPositionsOptions{IsPaper: &isPaper})
			if err != nil {
				errs = append(errs, fmt.Sprintf("list positions: %v", err))
				continue
			}
			if len(positions) >= s.cfg.MaxPositions {
				break
			}
		}

		// Determine side and outcome from signal direction
		side := "BUY"
		outcome := "YES"
		if strings.EqualFold(sig.Direction, "NO") || strings.EqualFold(sig.Direction, "SELL") {
			outcome = "NO"
		}

		// Check for existing paper position on same market+outcome
		existingPositions, err := s.tradeSvc.ListPositions(ctx, trade.ListPositionsOptions{
			MarketID: sig.MarketID,
			IsPaper:  &isPaper,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("check position market=%s: %v", sig.MarketID, err))
			continue
		}
		hasDup := false
		for _, p := range existingPositions {
			if strings.EqualFold(p.Outcome, outcome) && p.NetSize > 0 {
				hasDup = true
				break
			}
		}
		if hasDup {
			continue
		}

		// Get mid-price from latest market price
		var mp model.MarketPrice
		if err := s.db.WithContext(ctx).
			Where("market_id = ?", sig.MarketID).
			Order("captured_at DESC").
			First(&mp).Error; err != nil {
			errs = append(errs, fmt.Sprintf("market price market=%s: %v", sig.MarketID, err))
			continue
		}

		price := mp.PriceYes
		if strings.EqualFold(outcome, "NO") {
			price = mp.PriceNo
		}
		priceF := price.InexactFloat64()
		if priceF <= 0 || priceF >= 1 {
			continue
		}

		orderSize := s.cfg.OrderSize
		if orderSize <= 0 {
			orderSize = 1.0
		}
		size := orderSize / priceF

		sigID := sig.ID
		_, err = s.tradeSvc.SubmitOrder(ctx, trade.SubmitOrderRequest{
			MarketRef: sig.MarketID,
			Side:      side,
			Outcome:   outcome,
			Price:     priceF,
			Size:      size,
			Confirm:   true,
			SignalID:  &sigID,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("place trade market=%s: %v", sig.MarketID, err))
			continue
		}
		placed++

		// Mark signal as acted on
		s.db.WithContext(ctx).
			Model(&model.Signal{}).
			Where("id = ?", sig.ID).
			Update("acted_on", true)
	}

	return placed, errs
}

func (s *Service) settleResolvedMarkets(ctx context.Context) (int, []string) {
	var errs []string
	settled := 0

	// Find open paper trades that haven't been settled
	var trades []model.Trade
	if err := s.db.WithContext(ctx).
		Where("is_paper = ? AND status = ? AND pnl_usdc IS NULL", true, "filled").
		Find(&trades).Error; err != nil {
		return 0, []string{fmt.Sprintf("load open paper trades: %v", err)}
	}

	// Collect unique market IDs
	marketIDs := make(map[string]struct{})
	for _, t := range trades {
		marketIDs[t.MarketID] = struct{}{}
	}

	// Load resolved markets
	resolvedMarkets := make(map[string]*model.Market)
	for mID := range marketIDs {
		var m model.Market
		if err := s.db.WithContext(ctx).Where("id = ? AND is_resolved = ?", mID, true).First(&m).Error; err != nil {
			continue
		}
		resolvedMarkets[m.ID] = &m
	}

	now := time.Now().UTC()
	for _, t := range trades {
		m, ok := resolvedMarkets[t.MarketID]
		if !ok {
			continue
		}

		pnl := calculatePnL(t, m.Resolution)

		if err := s.db.WithContext(ctx).
			Model(&model.Trade{}).
			Where("id = ?", t.ID).
			Updates(map[string]any{
				"status":      "closed",
				"pnl_usdc":    pnl.StringFixed(6),
				"executed_at": now,
			}).Error; err != nil {
			errs = append(errs, fmt.Sprintf("settle trade id=%d: %v", t.ID, err))
			continue
		}
		settled++
	}

	return settled, errs
}

func calculatePnL(t model.Trade, resolution string) decimal.Decimal {
	entryPrice := t.Price
	size := t.Size
	if t.FillPrice.Valid && t.FillPrice.Decimal.GreaterThan(decimal.Zero) {
		entryPrice = t.FillPrice.Decimal
	}
	if t.FillSize.Valid && t.FillSize.Decimal.GreaterThan(decimal.Zero) {
		size = t.FillSize.Decimal
	}

	outcome := strings.ToUpper(t.Outcome)
	res := strings.ToUpper(strings.TrimSpace(resolution))

	won := (outcome == "YES" && res == "YES") || (outcome == "NO" && res == "NO")

	if won {
		// PnL = (1.0 - entry_price) * size
		return decimal.NewFromInt(1).Sub(entryPrice).Mul(size)
	}
	// PnL = -entry_price * size
	return entryPrice.Neg().Mul(size)
}

func (s *Service) GetReport(ctx context.Context, opts ReportOptions) (*SimReport, error) {
	isPaper := true
	pnlReport, err := s.tradeSvc.GetPnLReport(ctx, trade.PnLReportOptions{
		From:    opts.From,
		To:      opts.To,
		IsPaper: &isPaper,
	})
	if err != nil {
		return nil, err
	}

	report := &SimReport{
		From:             pnlReport.From,
		To:               pnlReport.To,
		TotalTrades:      pnlReport.TotalTrades,
		FilledTrades:     pnlReport.FilledTrades,
		WinTrades:        pnlReport.WinTrades,
		LossTrades:       pnlReport.LossTrades,
		WinRate:          pnlReport.WinRate,
		RealizedPnLUSDC:  pnlReport.RealizedPnLUSDC,
		GrossVolumeUSDC:  pnlReport.GrossVolumeUSDC,
		OpenExposureUSDC: pnlReport.OpenExposureUSDC,
	}

	// Convert daily
	for _, d := range pnlReport.Daily {
		report.Daily = append(report.Daily, DailySimPnL{
			Date:         d.Date,
			RealizedPnL:  d.RealizedPnL,
			GrossVolume:  d.GrossVolume,
			FilledTrades: d.FilledTrades,
		})
	}

	// Count pending paper trades
	var pendingCount int64
	s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Where("is_paper = ? AND status IN ?", true, []string{"filled"}).
		Where("pnl_usdc IS NULL").
		Count(&pendingCount)
	report.PendingTrades = pendingCount

	// Calculate unrealized PnL for open paper positions
	positions, err := s.tradeSvc.ListPositions(ctx, trade.ListPositionsOptions{IsPaper: &isPaper})
	if err == nil {
		totalUnrealized := 0.0
		for _, p := range positions {
			if p.UnrealizedPnL != nil {
				totalUnrealized += *p.UnrealizedPnL
			}
		}
		report.UnrealizedPnL = totalUnrealized
	}

	// Calculate avg edge for winning vs losing trades
	type edgeRow struct {
		AvgEdge float64 `gorm:"column:avg_edge"`
	}
	from := pnlReport.From
	to := pnlReport.To

	var winEdge edgeRow
	s.db.WithContext(ctx).
		Table("trades").
		Select("COALESCE(AVG(ABS(s.edge_pct)), 0) AS avg_edge").
		Joins("LEFT JOIN signals s ON trades.signal_id = s.id").
		Where("trades.is_paper = ? AND trades.created_at >= ? AND trades.created_at <= ?", true, from, to).
		Where("trades.pnl_usdc > 0").
		Scan(&winEdge)
	report.AvgEdgeWinning = math.Round(winEdge.AvgEdge*100) / 100

	var loseEdge edgeRow
	s.db.WithContext(ctx).
		Table("trades").
		Select("COALESCE(AVG(ABS(s.edge_pct)), 0) AS avg_edge").
		Joins("LEFT JOIN signals s ON trades.signal_id = s.id").
		Where("trades.is_paper = ? AND trades.created_at >= ? AND trades.created_at <= ?", true, from, to).
		Where("trades.pnl_usdc < 0").
		Scan(&loseEdge)
	report.AvgEdgeLosing = math.Round(loseEdge.AvgEdge*100) / 100

	return report, nil
}
