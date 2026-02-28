package trade

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/config"
)

const (
	orderSideBuy      = "BUY"
	orderSideSell     = "SELL"
	outcomeYes        = "YES"
	outcomeNo         = "NO"
	tradeOrderTypeGTC = "GTC"
	orderSigTypeEOA   = uint8(0)
	defaultExpiry     = 5 * time.Minute
)

var (
	ErrTradeInvalidRequest = errors.New("trade invalid request")
	ErrTradeRiskRejected   = errors.New("trade risk rejected")
	ErrTradeNotFound       = errors.New("trade not found")
	ErrTradeNotCancellable = errors.New("trade status not cancellable")
	ErrTradeCLOB           = errors.New("trade clob request failed")
	ErrTradePersistence    = errors.New("trade persistence failed")
	ErrTradeConfig         = errors.New("trade configuration error")
)

type flexTime struct {
	Time  time.Time
	Valid bool
}

type Service struct {
	cfg        config.TradeConfig
	db         *gorm.DB
	log        *zap.Logger
	signalSvc  *signal.Service
	clobClient *CLOBClient
	privateKey *ecdsa.PrivateKey
}

func NewService(cfg config.TradeConfig, marketCfg config.MarketConfig, db *gorm.DB, log *zap.Logger, signalSvc *signal.Service) *Service {
	httpClient := &http.Client{Timeout: marketCfg.RequestTimeout}
	svc := &Service{
		cfg:        cfg,
		db:         db,
		log:        log,
		signalSvc:  signalSvc,
		clobClient: NewCLOBClient(marketCfg.CLOBBaseURL, httpClient),
	}
	if strings.TrimSpace(cfg.PrivateKeyHex) != "" {
		log.Warn("trade private key is configured in plain text env/config; use secret manager in production")
		key, err := ParsePrivateKey(cfg.PrivateKeyHex)
		if err != nil {
			log.Warn("invalid trade private key", zap.Error(err))
		} else {
			svc.privateKey = key
		}
	}
	return svc
}

func (s *Service) SubmitOrder(ctx context.Context, req SubmitOrderRequest) (*SubmitOrderResult, error) {
	normalized, err := normalizeSubmitRequest(req, s.cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTradeInvalidRequest, err)
	}
	if s.cfg.ConfirmationRequired && !normalized.Confirm {
		return nil, fmt.Errorf("%w: confirmation is required", ErrTradeInvalidRequest)
	}

	market, err := s.signalSvc.LoadMarketByRef(ctx, normalized.MarketRef)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: market not found", ErrTradeInvalidRequest)
		}
		return nil, err
	}
	if !market.IsActive {
		return nil, fmt.Errorf("%w: market is not active", ErrTradeRiskRejected)
	}
	if err := s.validateRisk(ctx, market, normalized); err != nil {
		return nil, err
	}
	if s.cfg.PaperMode {
		return s.submitPaperOrder(ctx, market, normalized)
	}
	if s.privateKey == nil {
		return nil, fmt.Errorf("%w: trade private key is not configured", ErrTradeConfig)
	}

	now := time.Now().UTC()
	price := decimal.NewFromFloat(normalized.Price)
	size := decimal.NewFromFloat(normalized.Size)
	cost := price.Mul(size)

	signedOrder, signature, err := s.buildSignedOrder(now, market, normalized, price, size)
	if err != nil {
		return nil, err
	}

	orderID, err := s.clobClient.PlaceOrder(ctx, placeOrderPayload{
		Order:     *signedOrder,
		OrderType: tradeOrderTypeGTC,
		Owner:     signedOrder.Maker,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTradeCLOB, err)
	}

	row := model.Trade{
		MarketID:  market.ID,
		OrderID:   orderID,
		Side:      normalized.Side,
		Outcome:   normalized.Outcome,
		Price:     price,
		Size:      size,
		CostUSDC:  cost,
		Status:    "placed",
		CreatedAt: now,
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		cancelErr := s.clobClient.CancelOrder(rollbackCtx, orderID)
		if cancelErr != nil {
			s.log.Error("trade persistence failed and rollback cancel failed",
				zap.String("order_id", orderID),
				zap.Error(err),
				zap.Error(cancelErr),
			)
			return nil, fmt.Errorf("%w: save local trade failed after clob order=%s: %v (rollback cancel failed: %v)", ErrTradePersistence, orderID, err, cancelErr)
		}
		s.log.Warn("trade persistence failed; rollback cancel succeeded",
			zap.String("order_id", orderID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("%w: save local trade failed after clob order=%s: %v (rollback cancel succeeded)", ErrTradePersistence, orderID, err)
	}

	result := &SubmitOrderResult{
		TradeID:   row.ID,
		OrderID:   row.OrderID,
		Status:    row.Status,
		MarketID:  row.MarketID,
		Side:      row.Side,
		Outcome:   row.Outcome,
		Price:     row.Price.InexactFloat64(),
		Size:      row.Size.InexactFloat64(),
		CostUSDC:  row.CostUSDC.InexactFloat64(),
		Signature: signature,
		CreatedAt: row.CreatedAt,
	}
	return result, nil
}

func (s *Service) submitPaperOrder(ctx context.Context, market *model.Market, req SubmitOrderRequest) (*SubmitOrderResult, error) {
	now := time.Now().UTC()
	price := decimal.NewFromFloat(req.Price)
	size := decimal.NewFromFloat(req.Size)
	cost := price.Mul(size)
	orderID := "paper-" + uuid.New().String()

	row := model.Trade{
		MarketID:  market.ID,
		SignalID:  req.SignalID,
		OrderID:   orderID,
		Side:      req.Side,
		Outcome:   req.Outcome,
		Price:     price,
		Size:      size,
		CostUSDC:  cost,
		Status:    "filled",
		FillPrice: decimal.NullDecimal{Decimal: price, Valid: true},
		FillSize:  decimal.NullDecimal{Decimal: size, Valid: true},
		IsPaper:   true,
		CreatedAt: now,
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		return nil, fmt.Errorf("%w: save paper trade failed: %v", ErrTradePersistence, err)
	}

	fp := price.InexactFloat64()
	fs := size.InexactFloat64()
	return &SubmitOrderResult{
		TradeID:   row.ID,
		OrderID:   row.OrderID,
		Status:    row.Status,
		MarketID:  row.MarketID,
		Side:      row.Side,
		Outcome:   row.Outcome,
		Price:     fp,
		Size:      fs,
		CostUSDC:  cost.InexactFloat64(),
		CreatedAt: row.CreatedAt,
	}, nil
}

func (s *Service) CancelOrder(ctx context.Context, tradeID uint64) (*CancelOrderResult, error) {
	if tradeID == 0 {
		return nil, fmt.Errorf("%w: trade id is required", ErrTradeInvalidRequest)
	}

	var row model.Trade
	if err := s.db.WithContext(ctx).Where("id = ?", tradeID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: trade id=%d", ErrTradeNotFound, tradeID)
		}
		return nil, err
	}
	if strings.TrimSpace(row.OrderID) == "" {
		return nil, fmt.Errorf("%w: order id is empty", ErrTradeInvalidRequest)
	}
	if !isCancellableTradeStatus(row.Status) {
		return nil, fmt.Errorf("%w: current status=%s", ErrTradeNotCancellable, row.Status)
	}

	if err := s.clobClient.CancelOrder(ctx, row.OrderID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTradeCLOB, err)
	}

	now := time.Now().UTC()
	if err := s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Where("id = ?", row.ID).
		Updates(map[string]any{
			"status":      "cancelled",
			"executed_at": now,
		}).Error; err != nil {
		return nil, fmt.Errorf("%w: cancel order=%s db update failed: %v", ErrTradePersistence, row.OrderID, err)
	}

	return &CancelOrderResult{
		TradeID:   row.ID,
		OrderID:   row.OrderID,
		Status:    "cancelled",
		Cancelled: true,
		UpdatedAt: now,
	}, nil
}

func (s *Service) ListTrades(ctx context.Context, opts ListTradesOptions) ([]TradeView, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}

	query := s.db.WithContext(ctx).Model(&model.Trade{})
	if marketID := strings.TrimSpace(opts.MarketID); marketID != "" {
		query = query.Where("market_id = ?", marketID)
	}
	if opts.IsPaper != nil {
		query = query.Where("is_paper = ?", *opts.IsPaper)
	}
	if status := strings.TrimSpace(opts.Status); status != "" {
		normalizedStatus := normalizeTradeStatus(status)
		if normalizedStatus == "" {
			return nil, fmt.Errorf("%w: invalid status filter: %s", ErrTradeInvalidRequest, status)
		}
		query = query.Where("status = ?", normalizedStatus)
	}

	var rows []model.Trade
	if err := query.Order("created_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]TradeView, 0, len(rows))
	for _, row := range rows {
		out = append(out, mapTradeView(row))
	}
	return out, nil
}

func (s *Service) GetTradeByID(ctx context.Context, tradeID uint64) (*TradeView, error) {
	if tradeID == 0 {
		return nil, fmt.Errorf("%w: trade id is required", ErrTradeInvalidRequest)
	}
	var row model.Trade
	if err := s.db.WithContext(ctx).Where("id = ?", tradeID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: trade id=%d", ErrTradeNotFound, tradeID)
		}
		return nil, err
	}
	item := mapTradeView(row)
	return &item, nil
}

func (s *Service) ListPositions(ctx context.Context, opts ListPositionsOptions) ([]PositionView, error) {
	type positionRow struct {
		MarketID      string          `gorm:"column:market_id"`
		Outcome       string          `gorm:"column:outcome"`
		NetSize       decimal.Decimal `gorm:"column:net_size"`
		NetCost       decimal.Decimal `gorm:"column:net_cost"`
		BuySize       decimal.Decimal `gorm:"column:buy_size"`
		BuyCost       decimal.Decimal `gorm:"column:buy_cost"`
		RealizedPnL   decimal.Decimal `gorm:"column:realized_pnl"`
		LatestTradeAt flexTime        `gorm:"column:latest_trade_at"`
	}
	type priceRow struct {
		MarketID string          `gorm:"column:market_id"`
		PriceYes decimal.Decimal `gorm:"column:price_yes"`
		PriceNo  decimal.Decimal `gorm:"column:price_no"`
	}
	qtyExpr := "CASE WHEN fill_size IS NOT NULL AND fill_size > 0 THEN fill_size ELSE size END"
	priceExpr := "CASE WHEN fill_price IS NOT NULL AND fill_price > 0 THEN fill_price ELSE price END"
	signedQtyExpr := "CASE WHEN side = 'BUY' THEN " + qtyExpr + " WHEN side = 'SELL' THEN -(" + qtyExpr + ") ELSE 0 END"
	signedCostExpr := "CASE WHEN side = 'BUY' THEN (" + qtyExpr + " * " + priceExpr + ") WHEN side = 'SELL' THEN -(" + qtyExpr + " * " + priceExpr + ") ELSE 0 END"
	buyQtyExpr := "CASE WHEN side = 'BUY' THEN " + qtyExpr + " ELSE 0 END"
	buyCostExpr := "CASE WHEN side = 'BUY' THEN (" + qtyExpr + " * " + priceExpr + ") ELSE 0 END"

	query := s.db.WithContext(ctx).
		Table("trades").
		Select(
			"market_id, "+
				"UPPER(outcome) AS outcome, "+
				"COALESCE(SUM("+signedQtyExpr+"), CAST(0 AS NUMERIC)) AS net_size, "+
				"COALESCE(SUM("+signedCostExpr+"), CAST(0 AS NUMERIC)) AS net_cost, "+
				"COALESCE(SUM("+buyQtyExpr+"), CAST(0 AS NUMERIC)) AS buy_size, "+
				"COALESCE(SUM("+buyCostExpr+"), CAST(0 AS NUMERIC)) AS buy_cost, "+
				"COALESCE(SUM(COALESCE(pnl_usdc, 0)), CAST(0 AS NUMERIC)) AS realized_pnl, "+
				"MAX(created_at) AS latest_trade_at",
		).
		Where("status IN ?", []string{"filled", "partially_filled"}).
		Group("market_id, UPPER(outcome)").
		Order("market_id ASC, UPPER(outcome) ASC")
	if marketID := strings.TrimSpace(opts.MarketID); marketID != "" {
		query = query.Where("market_id = ?", marketID)
	}
	if opts.IsPaper != nil {
		query = query.Where("is_paper = ?", *opts.IsPaper)
	}

	var rows []positionRow
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	priceMap := make(map[string]priceRow)
	var latestPrices []priceRow
	priceQuery := s.db.WithContext(ctx).
		Table("(" +
			"SELECT market_id, price_yes, price_no, " +
			"ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY captured_at DESC, id DESC) AS rn " +
			"FROM market_prices" +
			") ranked").
		Select("market_id, price_yes, price_no").
		Where("rn = 1")
	if marketID := strings.TrimSpace(opts.MarketID); marketID != "" {
		priceQuery = priceQuery.Where("market_id = ?", marketID)
	}
	if err := priceQuery.Scan(&latestPrices).Error; err != nil {
		return nil, err
	}
	for _, row := range latestPrices {
		priceMap[row.MarketID] = row
	}

	positions := make([]PositionView, 0, len(rows))
	for _, row := range rows {
		if row.NetSize.IsZero() {
			continue
		}
		avgEntry := decimal.Zero
		if row.BuySize.GreaterThan(decimal.Zero) {
			avgEntry = row.BuyCost.Div(row.BuySize)
		}

		item := PositionView{
			MarketID:      row.MarketID,
			Outcome:       row.Outcome,
			NetSize:       row.NetSize.InexactFloat64(),
			AvgEntryPrice: avgEntry.InexactFloat64(),
			RealizedPnL:   row.RealizedPnL.InexactFloat64(),
		}
		if row.LatestTradeAt.Valid {
			item.LatestTradeAt = row.LatestTradeAt.Time.UTC()
		}

		if mp, ok := priceMap[row.MarketID]; ok {
			mark := mp.PriceNo
			if strings.EqualFold(row.Outcome, outcomeYes) {
				mark = mp.PriceYes
			}
			if mark.GreaterThan(decimal.Zero) {
				v := mark.InexactFloat64()
				item.MarkPrice = &v
				mv := row.NetSize.Mul(mark).InexactFloat64()
				item.MarketValueUSD = &mv
				upnl := row.NetSize.Mul(mark).Sub(row.NetCost).InexactFloat64()
				item.UnrealizedPnL = &upnl
			}
		}
		positions = append(positions, item)
	}
	return positions, nil
}

func (t *flexTime) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		t.Valid = false
		t.Time = time.Time{}
		return nil
	case time.Time:
		t.Valid = true
		t.Time = v
		return nil
	case string:
		parsed, err := parseFlexTimeString(v)
		if err != nil {
			return err
		}
		t.Valid = true
		t.Time = parsed
		return nil
	case []byte:
		parsed, err := parseFlexTimeString(string(v))
		if err != nil {
			return err
		}
		t.Valid = true
		t.Time = parsed
		return nil
	default:
		return fmt.Errorf("unsupported time scan type %T", src)
	}
}

func (t flexTime) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}
	return t.Time, nil
}

func parseFlexTimeString(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		if tm, err := time.Parse(layout, s); err == nil {
			if strings.Contains(layout, "Z07:00") || strings.Contains(layout, "-07:00") {
				return tm, nil
			}
			return time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), time.UTC), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", s)
}

func (s *Service) GetPnLReport(ctx context.Context, opts PnLReportOptions) (*PnLReport, error) {
	from := opts.From.UTC()
	to := opts.To.UTC()
	if from.IsZero() {
		from = time.Now().UTC().AddDate(0, 0, -7)
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}
	from = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	to = time.Date(to.Year(), to.Month(), to.Day(), 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC)
	if to.Before(from) {
		return nil, fmt.Errorf("%w: to must be after from", ErrTradeInvalidRequest)
	}

	type row struct {
		Status   string              `gorm:"column:status"`
		CostUSDC decimal.Decimal     `gorm:"column:cost_usdc"`
		PnLUSDC  decimal.NullDecimal `gorm:"column:pnl_usdc"`
	}
	var rows []row
	pnlQuery := s.db.WithContext(ctx).
		Table("trades").
		Select("status, cost_usdc, pnl_usdc").
		Where("created_at >= ? AND created_at <= ?", from, to)
	if opts.IsPaper != nil {
		pnlQuery = pnlQuery.Where("is_paper = ?", *opts.IsPaper)
	}
	if err := pnlQuery.Scan(&rows).Error; err != nil {
		return nil, err
	}

	report := &PnLReport{
		From:  from,
		To:    to,
		Daily: make([]DailyPnL, 0, 16),
	}
	report.TotalTrades = int64(len(rows))

	grossVolume := decimal.Zero
	realized := decimal.Zero
	openExposure := decimal.Zero

	for _, row := range rows {
		grossVolume = grossVolume.Add(row.CostUSDC.Abs())
		status := normalizeTradeStatus(row.Status)
		if status == "filled" || status == "closed" {
			report.FilledTrades++
		}
		if status == "pending" || status == "placed" || status == "partially_filled" {
			openExposure = openExposure.Add(row.CostUSDC.Abs())
		}
		if row.PnLUSDC.Valid {
			realized = realized.Add(row.PnLUSDC.Decimal)
			switch row.PnLUSDC.Decimal.Cmp(decimal.Zero) {
			case 1:
				report.WinTrades++
			case -1:
				report.LossTrades++
			default:
				report.BreakEvenTrades++
			}
		}
	}
	report.GrossVolumeUSDC = grossVolume.InexactFloat64()
	report.RealizedPnLUSDC = realized.InexactFloat64()
	report.OpenExposureUSDC = openExposure.InexactFloat64()
	decisions := report.WinTrades + report.LossTrades + report.BreakEvenTrades
	if decisions > 0 {
		report.WinRate = float64(report.WinTrades) / float64(decisions) * 100
	}

	type dailyRow struct {
		Date        string              `gorm:"column:date"`
		RealizedPnL decimal.NullDecimal `gorm:"column:realized_pnl"`
		GrossVolume decimal.NullDecimal `gorm:"column:gross_volume"`
		FilledCnt   int64               `gorm:"column:filled_trades"`
	}
	var dailyRows []dailyRow
	dailyQuery := s.db.WithContext(ctx).
		Table("trades").
		Select(
			"DATE(created_at) AS date, "+
				"COALESCE(SUM(pnl_usdc), CAST(0 AS NUMERIC)) AS realized_pnl, "+
				"COALESCE(SUM(cost_usdc), CAST(0 AS NUMERIC)) AS gross_volume, "+
				"SUM(CASE WHEN status IN ('filled','closed') THEN 1 ELSE 0 END) AS filled_trades",
		).
		Where("created_at >= ? AND created_at <= ?", from, to)
	if opts.IsPaper != nil {
		dailyQuery = dailyQuery.Where("is_paper = ?", *opts.IsPaper)
	}
	if err := dailyQuery.
		Group("DATE(created_at)").
		Order("DATE(created_at) ASC").
		Scan(&dailyRows).Error; err != nil {
		return nil, err
	}
	for _, row := range dailyRows {
		item := DailyPnL{
			Date:         row.Date,
			FilledTrades: row.FilledCnt,
		}
		if row.RealizedPnL.Valid {
			item.RealizedPnL = row.RealizedPnL.Decimal.InexactFloat64()
		}
		if row.GrossVolume.Valid {
			item.GrossVolume = row.GrossVolume.Decimal.InexactFloat64()
		}
		report.Daily = append(report.Daily, item)
	}

	return report, nil
}

func (s *Service) validateRisk(ctx context.Context, market *model.Market, req SubmitOrderRequest) error {
	cost := req.Price * req.Size
	if s.cfg.MaxPositionSize > 0 && cost > s.cfg.MaxPositionSize {
		return fmt.Errorf("%w: cost %.4f exceeds max_position_size %.4f", ErrTradeRiskRejected, cost, s.cfg.MaxPositionSize)
	}

	if s.cfg.MinLiquidity > 0 {
		liq := 0.0
		if market.Liquidity.Valid {
			liq = market.Liquidity.Decimal.InexactFloat64()
		}
		if liq < s.cfg.MinLiquidity {
			return fmt.Errorf("%w: liquidity %.2f below min_liquidity %.2f", ErrTradeRiskRejected, liq, s.cfg.MinLiquidity)
		}
	}

	if s.cfg.MinEdgePct > 0 {
		var sig model.Signal
		err := s.db.WithContext(ctx).
			Where("market_id = ?", market.ID).
			Order("created_at DESC").
			First(&sig).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: latest signal missing", ErrTradeRiskRejected)
			}
			return err
		}
		if math.Abs(sig.EdgePct) < s.cfg.MinEdgePct {
			return fmt.Errorf("%w: edge %.2f below min_edge_pct %.2f", ErrTradeRiskRejected, sig.EdgePct, s.cfg.MinEdgePct)
		}
	}

	if s.cfg.MaxOpenPositions > 0 {
		var openCount int64
		openQuery := s.db.WithContext(ctx).
			Model(&model.Trade{}).
			Where("status IN ?", []string{"pending", "placed", "partially_filled"}).
			Where("is_paper = ?", s.cfg.PaperMode)
		if err := openQuery.
			Distinct("market_id").
			Count(&openCount).Error; err != nil {
			return err
		}
		if openCount >= int64(s.cfg.MaxOpenPositions) {
			return fmt.Errorf("%w: max_open_positions reached", ErrTradeRiskRejected)
		}
	}

	if s.cfg.MaxDailyLoss > 0 {
		start := time.Now().UTC().Truncate(24 * time.Hour)
		var rows []model.Trade
		if err := s.db.WithContext(ctx).
			Where("created_at >= ?", start).
			Where("status IN ?", []string{"filled", "closed"}).
			Find(&rows).Error; err != nil {
			return err
		}
		loss := 0.0
		for _, row := range rows {
			if row.PnLUSDC.Valid {
				pnl := row.PnLUSDC.Decimal.InexactFloat64()
				if pnl < 0 {
					loss += math.Abs(pnl)
				}
			}
		}
		if loss >= s.cfg.MaxDailyLoss {
			return fmt.Errorf("%w: max_daily_loss reached", ErrTradeRiskRejected)
		}
	}

	var dupCount int64
	if err := s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Where("market_id = ?", market.ID).
		Where("side = ?", req.Side).
		Where("outcome = ?", req.Outcome).
		Where("status IN ?", []string{"pending", "placed", "partially_filled"}).
		Where("is_paper = ?", s.cfg.PaperMode).
		Where("created_at >= ?", time.Now().UTC().Add(-5*time.Minute)).
		Count(&dupCount).Error; err != nil {
		return err
	}
	if dupCount > 0 {
		return fmt.Errorf("%w: duplicate order in cooldown window", ErrTradeRiskRejected)
	}
	return nil
}

func (s *Service) buildSignedOrder(now time.Time, market *model.Market, req SubmitOrderRequest, price, size decimal.Decimal) (*SignedClobOrder, string, error) {
	tokenID := strings.TrimSpace(selectOrderTokenID(market, req.Outcome))
	if tokenID == "" {
		return nil, "", fmt.Errorf("%w: missing token id for outcome=%s", ErrTradeInvalidRequest, req.Outcome)
	}

	makerAddr := crypto.PubkeyToAddress(s.privateKey.PublicKey).Hex()
	nonce, err := randomUint64String()
	if err != nil {
		return nil, "", fmt.Errorf("%w: generate nonce failed: %v", ErrTradeConfig, err)
	}
	salt, err := randomUint128String()
	if err != nil {
		return nil, "", fmt.Errorf("%w: generate salt failed: %v", ErrTradeConfig, err)
	}
	expiration := strconv.FormatInt(now.Add(defaultExpiry).Unix(), 10)

	makerAmount, takerAmount, sideCode, sideText, err := buildOrderAmounts(req.Side, price, size)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrTradeInvalidRequest, err)
	}

	signable := SignableOrder{
		Salt:          salt,
		Maker:         makerAddr,
		Signer:        makerAddr,
		Taker:         "0x0000000000000000000000000000000000000000",
		TokenID:       tokenID,
		MakerAmount:   makerAmount,
		TakerAmount:   takerAmount,
		Expiration:    expiration,
		Nonce:         nonce,
		FeeRateBps:    "0",
		Side:          sideCode,
		SignatureType: orderSigTypeEOA,
	}

	signature, err := SignOrderEIP712(s.privateKey, s.cfg.ChainID, signable)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrTradeConfig, err)
	}

	return &SignedClobOrder{
		Salt:          signable.Salt,
		Maker:         signable.Maker,
		Signer:        signable.Signer,
		Taker:         signable.Taker,
		TokenID:       signable.TokenID,
		MakerAmount:   signable.MakerAmount,
		TakerAmount:   signable.TakerAmount,
		Expiration:    signable.Expiration,
		Nonce:         signable.Nonce,
		FeeRateBps:    signable.FeeRateBps,
		Side:          sideText,
		SignatureType: signable.SignatureType,
		Signature:     signature,
	}, signature, nil
}

func normalizeSubmitRequest(req SubmitOrderRequest, cfg config.TradeConfig) (SubmitOrderRequest, error) {
	out := req
	out.MarketRef = strings.TrimSpace(out.MarketRef)
	out.Side = strings.ToUpper(strings.TrimSpace(out.Side))
	out.Outcome = strings.ToUpper(strings.TrimSpace(out.Outcome))
	if out.MarketRef == "" {
		return out, fmt.Errorf("market_id is required")
	}
	if out.Side != orderSideBuy && out.Side != orderSideSell {
		return out, fmt.Errorf("side must be BUY or SELL")
	}
	if out.Outcome != outcomeYes && out.Outcome != outcomeNo {
		return out, fmt.Errorf("outcome must be YES or NO")
	}
	if out.Price <= 0 || out.Price >= 1 {
		return out, fmt.Errorf("price must be between 0 and 1")
	}
	if out.Size <= 0 {
		return out, fmt.Errorf("size must be positive")
	}
	if cfg.MaxOrderSize > 0 && out.Size > cfg.MaxOrderSize {
		return out, fmt.Errorf("size %.4f exceeds max_order_size %.4f", out.Size, cfg.MaxOrderSize)
	}
	return out, nil
}

func mapTradeView(row model.Trade) TradeView {
	item := TradeView{
		ID:         row.ID,
		MarketID:   row.MarketID,
		SignalID:   row.SignalID,
		OrderID:    row.OrderID,
		Side:       row.Side,
		Outcome:    row.Outcome,
		Price:      row.Price.InexactFloat64(),
		Size:       row.Size.InexactFloat64(),
		CostUSDC:   row.CostUSDC.InexactFloat64(),
		Status:     row.Status,
		TxHash:     row.TxHash,
		IsPaper:    row.IsPaper,
		ExecutedAt: row.ExecutedAt,
		CreatedAt:  row.CreatedAt,
	}
	if row.FeeUSDC.Valid {
		v := row.FeeUSDC.Decimal.InexactFloat64()
		item.FeeUSDC = &v
	}
	if row.FillPrice.Valid {
		v := row.FillPrice.Decimal.InexactFloat64()
		item.FillPrice = &v
	}
	if row.FillSize.Valid {
		v := row.FillSize.Decimal.InexactFloat64()
		item.FillSize = &v
	}
	if row.PnLUSDC.Valid {
		v := row.PnLUSDC.Decimal.InexactFloat64()
		item.PnLUSDC = &v
	}
	return item
}

func selectOrderTokenID(market *model.Market, outcome string) string {
	if strings.EqualFold(outcome, outcomeYes) {
		return market.TokenIDYes
	}
	return market.TokenIDNo
}

func isCancellableTradeStatus(status string) bool {
	s := normalizeTradeStatus(status)
	switch s {
	case "pending", "placed", "partially_filled":
		return true
	default:
		return false
	}
}

func normalizeTradeStatus(status string) string {
	s := strings.TrimSpace(strings.ToLower(status))
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, " ", "_")
	switch s {
	case "pending", "placed", "partially_filled", "filled", "cancelled", "closed", "failed":
		return s
	default:
		return ""
	}
}

func buildOrderAmounts(side string, price, size decimal.Decimal) (makerAmount string, takerAmount string, sideCode uint8, sideText string, err error) {
	var makerRaw decimal.Decimal
	var takerRaw decimal.Decimal

	switch strings.ToUpper(strings.TrimSpace(side)) {
	case orderSideBuy:
		sideCode = 0
		sideText = orderSideBuy
		makerRaw = size.Mul(price)
		takerRaw = size
	case orderSideSell:
		sideCode = 1
		sideText = orderSideSell
		makerRaw = size
		takerRaw = size.Mul(price)
	default:
		return "", "", 0, "", fmt.Errorf("invalid side: %s", side)
	}

	makerAmount, err = toTokenDecimals(makerRaw)
	if err != nil {
		return "", "", 0, "", fmt.Errorf("maker amount: %w", err)
	}
	takerAmount, err = toTokenDecimals(takerRaw)
	if err != nil {
		return "", "", 0, "", fmt.Errorf("taker amount: %w", err)
	}
	return makerAmount, takerAmount, sideCode, sideText, nil
}

func toTokenDecimals(v decimal.Decimal) (string, error) {
	if v.IsNegative() {
		return "", fmt.Errorf("value must be non-negative")
	}
	scaled := v.Mul(decimal.NewFromInt(1_000_000)).Round(0)
	if scaled.IsNegative() {
		return "", fmt.Errorf("scaled value must be non-negative")
	}
	return scaled.StringFixed(0), nil
}

func randomUint64String() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	v := binary.BigEndian.Uint64(buf)
	return strconv.FormatUint(v, 10), nil
}

func randomUint128String() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	x := new(big.Int).SetBytes(buf)
	if x.Sign() == 0 {
		x.SetInt64(1)
	}
	return x.String(), nil
}

func parseTradeID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid trade id")
	}
	return id, nil
}
