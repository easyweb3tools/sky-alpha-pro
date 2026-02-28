package trade

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/config"
)

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
	normalized, err := normalizeSubmitRequest(req)
	if err != nil {
		return nil, err
	}
	if s.cfg.ConfirmationRequired && !normalized.Confirm {
		return nil, fmt.Errorf("confirmation is required")
	}

	market, err := s.signalSvc.LoadMarketByRef(ctx, normalized.MarketRef)
	if err != nil {
		return nil, err
	}
	if !market.IsActive {
		return nil, fmt.Errorf("market is not active")
	}

	if err := s.validateRisk(ctx, market.ID, normalized); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	nonce := uint64(now.UnixNano())
	expiration := now.Add(5 * time.Minute).Unix()
	price := decimal.NewFromFloat(normalized.Price)
	size := decimal.NewFromFloat(normalized.Size)
	cost := price.Mul(size)

	signature := ""
	if s.privateKey == nil {
		return nil, fmt.Errorf("trade private key is not configured")
	}
	signature, err = SignOrderEIP712(s.privateKey, s.cfg.ChainID, SignableOrder{
		MarketID:   market.ID,
		Side:       normalized.Side,
		Outcome:    normalized.Outcome,
		Price:      price.StringFixed(4),
		Size:       size.StringFixed(6),
		Nonce:      nonce,
		Expiration: expiration,
	})
	if err != nil {
		return nil, err
	}

	orderID, err := s.clobClient.PlaceOrder(ctx, placeOrderPayload{
		MarketID:   market.ID,
		Side:       normalized.Side,
		Outcome:    normalized.Outcome,
		Price:      price.StringFixed(4),
		Size:       size.StringFixed(6),
		Signature:  signature,
		Expiration: expiration,
		Nonce:      nonce,
	})
	if err != nil {
		return nil, err
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
		return nil, err
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

func (s *Service) CancelOrder(ctx context.Context, tradeID uint64) (*CancelOrderResult, error) {
	if tradeID == 0 {
		return nil, fmt.Errorf("trade id is required")
	}

	var row model.Trade
	if err := s.db.WithContext(ctx).Where("id = ?", tradeID).First(&row).Error; err != nil {
		return nil, err
	}
	if strings.TrimSpace(row.OrderID) == "" {
		return nil, fmt.Errorf("order id is empty")
	}

	if err := s.clobClient.CancelOrder(ctx, row.OrderID); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if err := s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Where("id = ?", row.ID).
		Updates(map[string]any{
			"status":      "cancelled",
			"executed_at": now,
		}).Error; err != nil {
		return nil, err
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

	var rows []model.Trade
	if err := s.db.WithContext(ctx).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]TradeView, 0, len(rows))
	for _, row := range rows {
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
		out = append(out, item)
	}
	return out, nil
}

func (s *Service) GetTradeByID(ctx context.Context, tradeID uint64) (*TradeView, error) {
	if tradeID == 0 {
		return nil, fmt.Errorf("trade id is required")
	}
	var row model.Trade
	if err := s.db.WithContext(ctx).Where("id = ?", tradeID).First(&row).Error; err != nil {
		return nil, err
	}
	items, err := s.ListTrades(ctx, ListTradesOptions{Limit: 1})
	if err == nil {
		for _, item := range items {
			if item.ID == row.ID {
				return &item, nil
			}
		}
	}

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
		ExecutedAt: row.ExecutedAt,
		CreatedAt:  row.CreatedAt,
	}
	return &item, nil
}

func (s *Service) validateRisk(ctx context.Context, marketID string, req SubmitOrderRequest) error {
	cost := req.Price * req.Size
	if s.cfg.MaxPositionSize > 0 && cost > s.cfg.MaxPositionSize {
		return fmt.Errorf("risk rejected: cost %.4f exceeds max_position_size %.4f", cost, s.cfg.MaxPositionSize)
	}

	var market model.Market
	if err := s.db.WithContext(ctx).Where("id = ?", marketID).First(&market).Error; err != nil {
		return err
	}
	if s.cfg.MinLiquidity > 0 {
		liq := 0.0
		if market.Liquidity.Valid {
			liq = market.Liquidity.Decimal.InexactFloat64()
		}
		if liq < s.cfg.MinLiquidity {
			return fmt.Errorf("risk rejected: liquidity %.2f below min_liquidity %.2f", liq, s.cfg.MinLiquidity)
		}
	}

	if s.cfg.MinEdgePct > 0 {
		var sig model.Signal
		err := s.db.WithContext(ctx).
			Where("market_id = ?", marketID).
			Order("created_at DESC").
			First(&sig).Error
		if err != nil {
			return fmt.Errorf("risk rejected: latest signal missing")
		}
		if math.Abs(sig.EdgePct) < s.cfg.MinEdgePct {
			return fmt.Errorf("risk rejected: edge %.2f below min_edge_pct %.2f", sig.EdgePct, s.cfg.MinEdgePct)
		}
	}

	if s.cfg.MaxOpenPositions > 0 {
		var openCount int64
		if err := s.db.WithContext(ctx).
			Model(&model.Trade{}).
			Where("status IN ?", []string{"pending", "placed", "partially_filled"}).
			Distinct("market_id").
			Count(&openCount).Error; err != nil {
			return err
		}
		if openCount >= int64(s.cfg.MaxOpenPositions) {
			return fmt.Errorf("risk rejected: max_open_positions reached")
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
			return fmt.Errorf("risk rejected: max_daily_loss reached")
		}
	}

	var dupCount int64
	if err := s.db.WithContext(ctx).
		Model(&model.Trade{}).
		Where("market_id = ?", marketID).
		Where("side = ?", req.Side).
		Where("outcome = ?", req.Outcome).
		Where("status IN ?", []string{"pending", "placed", "partially_filled"}).
		Where("created_at >= ?", time.Now().UTC().Add(-5*time.Minute)).
		Count(&dupCount).Error; err != nil {
		return err
	}
	if dupCount > 0 {
		return fmt.Errorf("risk rejected: duplicate order in cooldown window")
	}
	return nil
}

func normalizeSubmitRequest(req SubmitOrderRequest) (SubmitOrderRequest, error) {
	out := req
	out.MarketRef = strings.TrimSpace(out.MarketRef)
	out.Side = strings.ToUpper(strings.TrimSpace(out.Side))
	out.Outcome = strings.ToUpper(strings.TrimSpace(out.Outcome))
	if out.MarketRef == "" {
		return out, fmt.Errorf("market_id is required")
	}
	if out.Side != "BUY" && out.Side != "SELL" {
		return out, fmt.Errorf("side must be BUY or SELL")
	}
	if out.Outcome != "YES" && out.Outcome != "NO" {
		return out, fmt.Errorf("outcome must be YES or NO")
	}
	if out.Price <= 0 || out.Price >= 1 {
		return out, fmt.Errorf("price must be between 0 and 1")
	}
	if out.Size <= 0 {
		return out, fmt.Errorf("size must be positive")
	}
	return out, nil
}

func parseTradeID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid trade id")
	}
	return id, nil
}
