package signal

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/pkg/config"
)

var thresholdPattern = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s*°?\s*([FC])`)

type Service struct {
	cfg config.SignalConfig
	db  *gorm.DB
	log *zap.Logger
}

func NewService(cfg config.SignalConfig, db *gorm.DB, log *zap.Logger) *Service {
	return &Service{cfg: cfg, db: db, log: log}
}

func (s *Service) GenerateSignals(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit > s.cfg.MaxMarkets {
		limit = s.cfg.MaxMarkets
	}

	var markets []model.Market
	if err := s.db.WithContext(ctx).
		Where("is_active = ?", true).
		Where("market_type IN ?", []string{"temperature_high", "temperature_low"}).
		Order("end_date ASC").
		Limit(limit).
		Find(&markets).Error; err != nil {
		return nil, err
	}

	result := &GenerateResult{Errors: make([]string, 0)}
	for _, m := range markets {
		result.Processed++
		if err := s.generateSignalForMarket(ctx, m); err != nil {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", m.PolymarketID, err))
			continue
		}
		result.Generated++
	}
	return result, nil
}

func (s *Service) ListSignals(ctx context.Context, opts ListOptions) ([]SignalView, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	query := s.db.WithContext(ctx).Model(&model.Signal{})
	if opts.MinEdge > 0 {
		query = query.Where("ABS(edge_pct) >= ?", opts.MinEdge)
	}

	var rows []model.Signal
	if err := query.Order("created_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}

	out := make([]SignalView, 0, len(rows))
	for _, row := range rows {
		out = append(out, SignalView{
			ID:          row.ID,
			MarketID:    row.MarketID,
			Direction:   row.Direction,
			EdgePct:     row.EdgePct,
			Confidence:  row.Confidence,
			MarketPrice: row.MarketPrice,
			OurEstimate: row.OurEstimate,
			Reasoning:   row.Reasoning,
			CreatedAt:   row.CreatedAt,
		})
	}
	return out, nil
}

func (s *Service) generateSignalForMarket(ctx context.Context, m model.Market) error {
	city := inferCity(m)
	if city == "" {
		return fmt.Errorf("city missing")
	}

	thresholdF, ok := parseThresholdFahrenheit(m.Question)
	if !ok {
		return fmt.Errorf("threshold not found in question")
	}

	var latestPrice model.MarketPrice
	if err := s.db.WithContext(ctx).
		Where("market_id = ?", m.ID).
		Order("captured_at DESC").
		First(&latestPrice).Error; err != nil {
		return fmt.Errorf("latest price not found: %w", err)
	}
	marketPrice := latestPrice.PriceYes.InexactFloat64()
	if marketPrice <= 0 || marketPrice >= 1 {
		return fmt.Errorf("invalid market price: %.4f", marketPrice)
	}

	values, err := s.loadForecastValues(ctx, city, m.EndDate, m.MarketType)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("no matched forecast values")
	}

	ourEstimate := estimateProbability(values, thresholdF, m.MarketType)
	edgePct := (ourEstimate - marketPrice) * 100.0
	if math.Abs(edgePct) < s.cfg.MinEdgePct {
		return fmt.Errorf("edge %.2f below threshold %.2f", edgePct, s.cfg.MinEdgePct)
	}

	direction := "YES"
	if edgePct < 0 {
		direction = "NO"
	}
	confidence := estimateConfidence(values, edgePct)
	reasoning := fmt.Sprintf("city=%s threshold=%.1fF source_count=%d estimate=%.4f market=%.4f edge=%.2f%%", city, thresholdF, len(values), ourEstimate, marketPrice, edgePct)

	row := model.Signal{
		MarketID:    m.ID,
		SignalType:  "forecast_edge",
		Direction:   direction,
		EdgePct:     edgePct,
		Confidence:  confidence,
		MarketPrice: marketPrice,
		OurEstimate: ourEstimate,
		Reasoning:   reasoning,
		AIModel:     "probability-model-v1",
		ActedOn:     false,
		CreatedAt:   time.Now().UTC(),
	}
	return s.db.WithContext(ctx).Create(&row).Error
}

func (s *Service) loadForecastValues(ctx context.Context, city string, endDate time.Time, marketType string) ([]float64, error) {
	targetDate := endDate.UTC().Format("2006-01-02")

	type row struct {
		Source    string  `gorm:"column:source"`
		TempHighF float64 `gorm:"column:temp_high_f"`
		TempLowF  float64 `gorm:"column:temp_low_f"`
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Raw(`
			SELECT DISTINCT ON (source) source, temp_high_f, temp_low_f
			FROM forecasts
			WHERE forecast_date = ?
			  AND location ILIKE ?
			ORDER BY source, fetched_at DESC
		`, targetDate, "%"+city+"%").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	values := make([]float64, 0, len(rows))
	for _, r := range rows {
		switch marketType {
		case "temperature_low":
			if r.TempLowF != 0 {
				values = append(values, r.TempLowF)
			}
		default:
			if r.TempHighF != 0 {
				values = append(values, r.TempHighF)
			}
		}
	}
	return values, nil
}

func inferCity(m model.Market) string {
	if strings.TrimSpace(m.City) != "" {
		return strings.TrimSpace(m.City)
	}
	q := strings.ToLower(m.Question)
	known := []string{
		"new york",
		"chicago",
		"miami",
		"los angeles",
		"austin",
		"dallas",
	}
	for _, c := range known {
		if strings.Contains(q, c) {
			return c
		}
	}
	return ""
}

func parseThresholdFahrenheit(question string) (float64, bool) {
	matches := thresholdPattern.FindStringSubmatch(question)
	if len(matches) < 3 {
		return 0, false
	}
	value, err := strconvParseFloat(matches[1])
	if err != nil {
		return 0, false
	}
	unit := strings.ToUpper(matches[2])
	if unit == "C" {
		value = (value * 9.0 / 5.0) + 32.0
	}
	return value, true
}

func estimateProbability(values []float64, threshold float64, marketType string) float64 {
	mu, sigma := meanStd(values)
	if sigma < 1.5 {
		sigma = 1.5
	}

	var p float64
	switch marketType {
	case "temperature_low":
		// P(low <= threshold)
		z := (threshold - mu) / sigma
		p = normalCDF(z)
	default:
		// P(high >= threshold)
		z := (threshold - mu) / sigma
		p = 1.0 - normalCDF(z)
	}
	if p < 0 {
		return 0
	}
	if p > 1 {
		return 1
	}
	return p
}

func estimateConfidence(values []float64, edgePct float64) float64 {
	_, sigma := meanStd(values)
	conf := 50.0 + math.Min(25.0, math.Abs(edgePct)*2.0) + math.Min(20.0, float64(len(values))*5.0) - math.Min(20.0, sigma*2.0)
	if conf < 1 {
		return 1
	}
	if conf > 99 {
		return 99
	}
	return conf
}

func meanStd(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	if len(values) == 1 {
		return mean, 0
	}
	varSq := 0.0
	for _, v := range values {
		d := v - mean
		varSq += d * d
	}
	return mean, math.Sqrt(varSq / float64(len(values)-1))
}

func normalCDF(z float64) float64 {
	return 0.5 * (1.0 + math.Erf(z/math.Sqrt2))
}

func strconvParseFloat(v string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(v), 64)
}
