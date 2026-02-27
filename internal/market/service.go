package market

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

type Service struct {
	cfg   config.MarketConfig
	db    *gorm.DB
	log   *zap.Logger
	gamma *GammaClient
	clob  *CLOBClient
}

func NewService(cfg config.MarketConfig, db *gorm.DB, log *zap.Logger) *Service {
	httpClient := &http.Client{Timeout: cfg.RequestTimeout}
	return &Service{
		cfg:   cfg,
		db:    db,
		log:   log,
		gamma: NewGammaClient(cfg.GammaBaseURL, httpClient),
		clob:  NewCLOBClient(cfg.CLOBBaseURL, httpClient),
	}
}

func (s *Service) SyncMarkets(ctx context.Context) (*SyncResult, error) {
	markets, err := s.gamma.ListMarkets(ctx, s.cfg.WeatherTag, true, s.cfg.SyncLimit)
	if err != nil {
		return nil, err
	}

	result := &SyncResult{
		MarketsFetched: len(markets),
		Errors:         make([]string, 0),
	}

	for _, gm := range markets {
		stored, err := s.upsertMarket(ctx, gm)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("upsert market %s: %v", gm.PolymarketID, err))
			continue
		}
		result.MarketsUpserted++

		priceYes := gm.PriceYes
		priceNo := gm.PriceNo
		spread := 0.0

		if gm.TokenIDYes != "" {
			if p, err := s.clob.GetMidpoint(ctx, gm.TokenIDYes); err == nil && p > 0 {
				priceYes = p
			} else if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("midpoint yes %s: %v", gm.TokenIDYes, err))
			}
			if sp, err := s.clob.GetSpread(ctx, gm.TokenIDYes); err == nil && sp > 0 {
				spread = sp
			}
		}

		if gm.TokenIDNo != "" {
			if p, err := s.clob.GetMidpoint(ctx, gm.TokenIDNo); err == nil && p > 0 {
				priceNo = p
			} else if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("midpoint no %s: %v", gm.TokenIDNo, err))
			}
		}

		if priceYes > 0 && priceNo <= 0 {
			priceNo = 1 - priceYes
		}
		if priceNo > 0 && priceYes <= 0 {
			priceYes = 1 - priceNo
		}
		if spread <= 0 && priceYes > 0 && priceNo > 0 {
			spread = math.Abs(1 - (priceYes + priceNo))
		}

		if priceYes <= 0 || priceNo <= 0 {
			continue
		}

		price := model.MarketPrice{
			MarketID:   stored.ID,
			PriceYes:   clamp01(priceYes),
			PriceNo:    clamp01(priceNo),
			Spread:     spread,
			Volume24h:  stored.VolumeTotal,
			Source:     "clob",
			CapturedAt: time.Now().UTC(),
		}
		if err := s.db.WithContext(ctx).Create(&price).Error; err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("insert price %s: %v", stored.PolymarketID, err))
			continue
		}
		result.PriceSnapshots++
	}

	s.log.Info("market sync finished",
		zap.Int("fetched", result.MarketsFetched),
		zap.Int("upserted", result.MarketsUpserted),
		zap.Int("prices", result.PriceSnapshots),
		zap.Int("errors", len(result.Errors)),
	)

	return result, nil
}

func (s *Service) ListMarketSnapshots(ctx context.Context, opts ListOptions) ([]MarketSnapshot, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}

	query := s.db.WithContext(ctx).Model(&model.Market{})
	if opts.ActiveOnly {
		query = query.Where("is_active = ?", true)
	}
	if strings.TrimSpace(opts.City) != "" {
		query = query.Where("city = ?", strings.TrimSpace(opts.City))
	}

	var rows []model.Market
	if err := query.Order("end_date ASC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}

	items := make([]MarketSnapshot, 0, len(rows))
	for _, m := range rows {
		item := MarketSnapshot{
			ID:           m.ID,
			PolymarketID: m.PolymarketID,
			Slug:         m.Slug,
			Question:     m.Question,
			City:         m.City,
			MarketType:   m.MarketType,
			IsActive:     m.IsActive,
			EndDate:      m.EndDate,
		}

		var latest model.MarketPrice
		err := s.db.WithContext(ctx).
			Where("market_id = ?", m.ID).
			Order("captured_at DESC").
			First(&latest).Error
		if err == nil {
			item.PriceYes = latest.PriceYes
			item.PriceNo = latest.PriceNo
			item.Spread = latest.Spread
			item.Volume24h = latest.Volume24h
			captured := latest.CapturedAt
			item.CapturedAt = &captured
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

func (s *Service) upsertMarket(ctx context.Context, gm GammaMarket) (*model.Market, error) {
	var row model.Market
	tx := s.db.WithContext(ctx).Where("polymarket_id = ?", gm.PolymarketID).Limit(1).Find(&row)
	if tx.Error != nil {
		return nil, tx.Error
	}

	if tx.RowsAffected == 0 {
		row = model.Market{
			ID:               uuid.NewString(),
			PolymarketID:     gm.PolymarketID,
			ConditionID:      gm.ConditionID,
			Slug:             gm.Slug,
			Question:         gm.Question,
			Description:      gm.Description,
			City:             gm.City,
			MarketType:       normalizeMarketType(gm.MarketType, gm.Question),
			ResolutionSource: "",
			TokenIDYes:       gm.TokenIDYes,
			TokenIDNo:        gm.TokenIDNo,
			OutcomeYes:       fallbackString(gm.OutcomeYes, "YES"),
			OutcomeNo:        fallbackString(gm.OutcomeNo, "NO"),
			EndDate:          fallbackEndDate(gm.EndDate),
			IsActive:         gm.IsActive,
			IsResolved:       gm.IsResolved,
			Resolution:       gm.Resolution,
			VolumeTotal:      gm.VolumeTotal,
			Liquidity:        gm.Liquidity,
		}
		if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
			return nil, err
		}
		return &row, nil
	}

	row.ConditionID = gm.ConditionID
	row.Slug = gm.Slug
	row.Question = gm.Question
	row.Description = gm.Description
	row.City = gm.City
	row.MarketType = normalizeMarketType(gm.MarketType, gm.Question)
	row.TokenIDYes = gm.TokenIDYes
	row.TokenIDNo = gm.TokenIDNo
	row.OutcomeYes = fallbackString(gm.OutcomeYes, "YES")
	row.OutcomeNo = fallbackString(gm.OutcomeNo, "NO")
	row.EndDate = fallbackEndDate(gm.EndDate)
	row.IsActive = gm.IsActive
	row.IsResolved = gm.IsResolved
	row.Resolution = gm.Resolution
	row.VolumeTotal = gm.VolumeTotal
	row.Liquidity = gm.Liquidity
	if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
		return nil, err
	}
	return &row, nil
}

func normalizeMarketType(marketType string, question string) string {
	mt := strings.TrimSpace(strings.ToLower(marketType))
	if mt != "" {
		return mt
	}
	q := strings.ToLower(question)
	switch {
	case strings.Contains(q, "high temperature"), strings.Contains(q, "high temp"), strings.Contains(q, "exceed"):
		return "temperature_high"
	case strings.Contains(q, "low temperature"), strings.Contains(q, "low temp"), strings.Contains(q, "below"):
		return "temperature_low"
	case strings.Contains(q, "rain"), strings.Contains(q, "precip"):
		return "precipitation"
	case strings.Contains(q, "snow"):
		return "snow"
	default:
		return "temperature_high"
	}
}

func fallbackEndDate(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC().Add(24 * time.Hour)
	}
	return t.UTC()
}

func fallbackString(v string, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}
