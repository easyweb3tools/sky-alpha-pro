package market

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/config"
)

const defaultSyncConcurrency = 10

var marketThresholdPattern = regexp.MustCompile(`(?i)(?:above|below|over|under|exceed|at least|at most|no more than|no less than)[^0-9-]{0,24}(-?\d+(?:\.\d+)?)\s*°?\s*([FC])?`)
var marketLooseThresholdPattern = regexp.MustCompile(`(?i)(-?\d+(?:\.\d+)?)\s*°?\s*([FC])`)

type Service struct {
	cfg    config.MarketConfig
	db     *gorm.DB
	log    *zap.Logger
	gamma  *GammaClient
	clob   *CLOBClient
	events EventSink
}

type EventSink interface {
	EmitMarketEvent(ctx context.Context, eventType, marketID, severity string, payload map[string]any) error
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

func (s *Service) SetEventSink(sink EventSink) {
	s.events = sink
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
	if len(markets) == 0 {
		return result, nil
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultSyncConcurrency)

	for _, gm := range markets {
		gm := gm
		g.Go(func() error {
			stored, upsertErr := s.upsertMarket(gctx, gm)
			if upsertErr != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("upsert market %s: %v", gm.PolymarketID, upsertErr))
				mu.Unlock()
				return nil
			}
			if s.events != nil && stored != nil && stored.IsActive {
				_ = s.events.EmitMarketEvent(gctx, "market_sync_seen", stored.ID, "info", map[string]any{
					"market_type":         stored.MarketType,
					"is_signal_supported": stored.IsSignalSupported,
					"spec_status":         stored.SpecStatus,
				})
			}

			priceRow, priceErr, warnings := s.buildPriceSnapshot(gctx, gm, stored.ID)
			mu.Lock()
			result.MarketsUpserted++
			result.Errors = append(result.Errors, warnings...)
			mu.Unlock()

			if priceErr != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("price snapshot %s: %v", gm.PolymarketID, priceErr))
				mu.Unlock()
				return nil
			}
			if priceRow == nil {
				return nil
			}

			if err := s.db.WithContext(gctx).Create(priceRow).Error; err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("insert price %s: %v", gm.PolymarketID, err))
				mu.Unlock()
				return nil
			}
			if s.events != nil && stored != nil {
				_ = s.events.EmitMarketEvent(gctx, "price_snapshot_updated", stored.ID, "info", map[string]any{
					"source": priceRow.Source,
				})
			}

			mu.Lock()
			result.PriceSnapshots++
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	s.log.Info("market sync finished",
		zap.Int("fetched", result.MarketsFetched),
		zap.Int("upserted", result.MarketsUpserted),
		zap.Int("prices", result.PriceSnapshots),
		zap.Int("errors", len(result.Errors)),
	)

	return result, nil
}

func (s *Service) buildPriceSnapshot(ctx context.Context, gm GammaMarket, marketID string) (*model.MarketPrice, error, []string) {
	priceYes := gm.PriceYes
	priceNo := gm.PriceNo
	spread := 0.0
	source := "gamma_fallback"
	warnings := make([]string, 0)
	clobMidpointOK := false

	type result struct {
		value float64
		err   error
	}
	var yesMid, noMid, yesSpread result

	subGroup, subCtx := errgroup.WithContext(ctx)
	if gm.TokenIDYes != "" {
		token := gm.TokenIDYes
		subGroup.Go(func() error {
			v, err := s.clob.GetMidpoint(subCtx, token)
			yesMid = result{value: v, err: err}
			return nil
		})
		subGroup.Go(func() error {
			v, err := s.clob.GetSpread(subCtx, token)
			yesSpread = result{value: v, err: err}
			return nil
		})
	}
	if gm.TokenIDNo != "" {
		token := gm.TokenIDNo
		subGroup.Go(func() error {
			v, err := s.clob.GetMidpoint(subCtx, token)
			noMid = result{value: v, err: err}
			return nil
		})
	}
	_ = subGroup.Wait()

	if yesMid.err != nil {
		warnings = append(warnings, fmt.Sprintf("midpoint yes %s: %v", gm.TokenIDYes, yesMid.err))
	}
	if noMid.err != nil {
		warnings = append(warnings, fmt.Sprintf("midpoint no %s: %v", gm.TokenIDNo, noMid.err))
	}
	if yesSpread.err != nil {
		warnings = append(warnings, fmt.Sprintf("spread yes %s: %v", gm.TokenIDYes, yesSpread.err))
	}

	if yesMid.value > 0 {
		priceYes = yesMid.value
		clobMidpointOK = true
	}
	if noMid.value > 0 {
		priceNo = noMid.value
		clobMidpointOK = true
	}
	if clobMidpointOK {
		source = "clob"
	}
	if yesSpread.value > 0 {
		spread = yesSpread.value
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
		return nil, fmt.Errorf("invalid prices yes=%.4f no=%.4f", priceYes, priceNo), warnings
	}

	row := &model.MarketPrice{
		MarketID:   marketID,
		PriceYes:   decimalFromFloat(clamp01(priceYes)),
		PriceNo:    decimalFromFloat(clamp01(priceNo)),
		Spread:     nullDecimalFromFloat(spread),
		Volume24h:  nullDecimalFromFloat(gm.Volume24h),
		Source:     source,
		CapturedAt: time.Now().UTC(),
	}
	return row, nil, warnings
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
	if strings.TrimSpace(opts.MarketType) != "" {
		query = query.Where("market_type = ?", strings.TrimSpace(opts.MarketType))
	}

	var markets []model.Market
	if err := query.Order("end_date ASC").Limit(limit).Find(&markets).Error; err != nil {
		return nil, err
	}
	if len(markets) == 0 {
		return []MarketSnapshot{}, nil
	}

	marketIDs := make([]string, 0, len(markets))
	for _, m := range markets {
		marketIDs = append(marketIDs, m.ID)
	}

	type latestPrice struct {
		MarketID   string              `gorm:"column:market_id"`
		PriceYes   decimal.Decimal     `gorm:"column:price_yes"`
		PriceNo    decimal.Decimal     `gorm:"column:price_no"`
		Spread     decimal.NullDecimal `gorm:"column:spread"`
		Volume24h  decimal.NullDecimal `gorm:"column:volume_24h"`
		CapturedAt time.Time           `gorm:"column:captured_at"`
	}

	var latestRows []latestPrice
	if err := s.db.WithContext(ctx).
		Raw(`
			SELECT DISTINCT ON (market_id)
				market_id,
				price_yes,
				price_no,
				spread,
				volume_24h,
				captured_at
			FROM market_prices
			WHERE market_id IN ?
			ORDER BY market_id, captured_at DESC
		`, marketIDs).
		Scan(&latestRows).Error; err != nil {
		return nil, err
	}

	latestByMarket := make(map[string]latestPrice, len(latestRows))
	for _, row := range latestRows {
		latestByMarket[row.MarketID] = row
	}

	items := make([]MarketSnapshot, 0, len(markets))
	for _, m := range markets {
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

		if latest, ok := latestByMarket[m.ID]; ok {
			item.PriceYes = latest.PriceYes.InexactFloat64()
			item.PriceNo = latest.PriceNo.InexactFloat64()
			if latest.Spread.Valid {
				item.Spread = latest.Spread.Decimal.InexactFloat64()
			}
			if latest.Volume24h.Valid {
				item.Volume24h = latest.Volume24h.Decimal.InexactFloat64()
			}
			captured := latest.CapturedAt
			item.CapturedAt = &captured
		}
		items = append(items, item)
	}

	return items, nil
}

func (s *Service) upsertMarket(ctx context.Context, gm GammaMarket) (*model.Market, error) {
	endDate, isFallbackEndDate := fallbackEndDate(gm.EndDate)
	active := gm.IsActive
	if isFallbackEndDate {
		active = false
	}

	targetDate := endDate.UTC().AddDate(0, 0, -1)
	targetDate = time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 0, 0, 0, 0, time.UTC)
	threshold, thresholdOK := parseQuestionThresholdF(gm.Question)
	comparator := inferComparator(gm.Question, normalizeMarketType(gm.MarketType, gm.Question))
	specStatus := "incomplete"
	thresholdValue := decimal.NullDecimal{}
	if thresholdOK {
		thresholdValue = decimal.NullDecimal{
			Decimal: decimal.NewFromFloat(threshold),
			Valid:   true,
		}
	}
	if thresholdOK && comparator != "" {
		specStatus = "ready"
	}

	upsertRow := model.Market{
		ID:                uuid.NewString(),
		PolymarketID:      gm.PolymarketID,
		ConditionID:       gm.ConditionID,
		Slug:              gm.Slug,
		Question:          gm.Question,
		Description:       gm.Description,
		City:              gm.City,
		MarketType:        normalizeMarketType(gm.MarketType, gm.Question),
		ResolutionSource:  "",
		TokenIDYes:        gm.TokenIDYes,
		TokenIDNo:         gm.TokenIDNo,
		OutcomeYes:        fallbackString(gm.OutcomeYes, "YES"),
		OutcomeNo:         fallbackString(gm.OutcomeNo, "NO"),
		EndDate:           endDate,
		IsActive:          active,
		IsResolved:        gm.IsResolved,
		Resolution:        gm.Resolution,
		VolumeTotal:       nullDecimalFromFloat(gm.VolumeTotal),
		Liquidity:         nullDecimalFromFloat(gm.Liquidity),
		ThresholdF:        thresholdValue,
		Comparator:        comparator,
		WeatherTargetDate: &targetDate,
		SpecStatus:        specStatus,
		IsSignalSupported: signal.IsSupportedMarketForSignal(model.Market{
			Question:   gm.Question,
			Slug:       gm.Slug,
			MarketType: normalizeMarketType(gm.MarketType, gm.Question),
			Comparator: comparator,
			ThresholdF: thresholdValue,
		}),
	}

	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "polymarket_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"condition_id":        upsertRow.ConditionID,
				"slug":                upsertRow.Slug,
				"question":            upsertRow.Question,
				"description":         upsertRow.Description,
				"city":                gorm.Expr("CASE WHEN EXCLUDED.city IS NOT NULL AND EXCLUDED.city <> '' THEN EXCLUDED.city ELSE markets.city END"),
				"market_type":         upsertRow.MarketType,
				"resolution_source":   upsertRow.ResolutionSource,
				"token_id_yes":        upsertRow.TokenIDYes,
				"token_id_no":         upsertRow.TokenIDNo,
				"outcome_yes":         upsertRow.OutcomeYes,
				"outcome_no":          upsertRow.OutcomeNo,
				"end_date":            upsertRow.EndDate,
				"is_active":           upsertRow.IsActive,
				"is_resolved":         upsertRow.IsResolved,
				"resolution":          upsertRow.Resolution,
				"volume_total":        upsertRow.VolumeTotal,
				"liquidity":           upsertRow.Liquidity,
				"threshold_f":         gorm.Expr("CASE WHEN EXCLUDED.threshold_f IS NOT NULL THEN EXCLUDED.threshold_f ELSE markets.threshold_f END"),
				"comparator":          gorm.Expr("CASE WHEN EXCLUDED.comparator IS NOT NULL AND EXCLUDED.comparator <> '' THEN EXCLUDED.comparator ELSE markets.comparator END"),
				"weather_target_date": gorm.Expr("CASE WHEN EXCLUDED.weather_target_date IS NOT NULL THEN EXCLUDED.weather_target_date ELSE markets.weather_target_date END"),
				"spec_status": gorm.Expr(`CASE
					WHEN COALESCE(EXCLUDED.spec_status,'') = 'ready' THEN 'ready'
					WHEN COALESCE(markets.spec_status,'') = 'ready' THEN 'ready'
					WHEN COALESCE(EXCLUDED.spec_status,'') IN ('','incomplete') AND COALESCE(markets.spec_status,'') <> '' THEN markets.spec_status
					ELSE EXCLUDED.spec_status
				END`),
				"is_signal_supported": upsertRow.IsSignalSupported,
				"updated_at":          time.Now().UTC(),
			}),
		}).
		Create(&upsertRow).Error
	if err != nil {
		return nil, err
	}

	var stored model.Market
	if err := s.db.WithContext(ctx).Where("polymarket_id = ?", gm.PolymarketID).Take(&stored).Error; err != nil {
		return nil, err
	}
	return &stored, nil
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
		return "unknown"
	}
}

func fallbackEndDate(t time.Time) (time.Time, bool) {
	if t.IsZero() {
		return time.Now().UTC().Add(30 * 24 * time.Hour), true
	}
	return t.UTC(), false
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

func decimalFromFloat(v float64) decimal.Decimal {
	return decimal.NewFromFloat(v)
}

func nullDecimalFromFloat(v float64) decimal.NullDecimal {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return decimal.NullDecimal{}
	}
	return decimal.NullDecimal{
		Decimal: decimal.NewFromFloat(v),
		Valid:   true,
	}
}

func parseQuestionThresholdF(question string) (float64, bool) {
	matches := marketThresholdPattern.FindStringSubmatch(question)
	if len(matches) >= 2 {
		value, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err == nil {
			unit := "F"
			if len(matches) >= 3 && strings.TrimSpace(matches[2]) != "" {
				unit = strings.ToUpper(strings.TrimSpace(matches[2]))
			}
			if unit == "C" {
				value = (value * 9.0 / 5.0) + 32.0
			}
			return value, true
		}
	}

	loose := marketLooseThresholdPattern.FindStringSubmatch(question)
	if len(loose) >= 3 {
		value, err := strconv.ParseFloat(strings.TrimSpace(loose[1]), 64)
		if err != nil {
			return 0, false
		}
		unit := strings.ToUpper(strings.TrimSpace(loose[2]))
		if unit == "C" {
			value = (value * 9.0 / 5.0) + 32.0
		}
		return value, true
	}
	return 0, false
}

func inferComparator(question string, marketType string) string {
	q := strings.ToLower(question)
	switch {
	case strings.Contains(q, "below"), strings.Contains(q, "under"), strings.Contains(q, "at most"), strings.Contains(q, "no more than"), strings.Contains(q, "lower"), strings.Contains(q, "at or below"):
		return "le"
	case strings.Contains(q, "above"), strings.Contains(q, "exceed"), strings.Contains(q, "at least"), strings.Contains(q, "no less than"), strings.Contains(q, "higher"), strings.Contains(q, "at or above"):
		return "ge"
	}
	switch marketType {
	case "temperature_low":
		return "le"
	case "temperature_high":
		return "ge"
	default:
		return ""
	}
}
