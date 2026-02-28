package signal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

type forecastSourceValue = ForecastSnapshot

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

	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		s.log.Warn("load known cities failed", zap.Error(err))
	}

	result := &GenerateResult{
		Processed: len(markets),
		Errors:    make([]string, 0),
	}
	if len(markets) == 0 {
		return result, nil
	}

	workers := s.cfg.Concurrency
	if workers <= 0 {
		workers = 1
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, market := range markets {
		m := market
		g.Go(func() error {
			genErr := s.generateSignalForMarket(gctx, m, knownCities)
			mu.Lock()
			defer mu.Unlock()
			if genErr != nil {
				result.Skipped++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", m.PolymarketID, genErr))
				if errors.Is(genErr, context.Canceled) || errors.Is(genErr, context.DeadlineExceeded) {
					return genErr
				}
				return nil
			}
			result.Generated++
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return result, err
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
			SignalDate:  row.SignalDate,
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

func (s *Service) GenerateSignalForMarketID(ctx context.Context, marketRef string) (*SignalView, error) {
	market, err := s.LoadMarketByRef(ctx, marketRef)
	if err != nil {
		return nil, err
	}
	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		s.log.Warn("load known cities failed", zap.Error(err))
	}
	if err := s.generateSignalForMarket(ctx, *market, knownCities); err != nil {
		return nil, err
	}
	targetDate, err := marketForecastDate(market.EndDate)
	if err != nil {
		return nil, err
	}

	var row model.Signal
	if err := s.db.WithContext(ctx).
		Where("market_id = ?", market.ID).
		Where("signal_type = ?", "forecast_edge").
		Where("DATE(signal_date) = ?", targetDate.Format("2006-01-02")).
		Order("created_at DESC").
		First(&row).Error; err != nil {
		return nil, err
	}
	view := mapSignalView(row)
	return &view, nil
}

func (s *Service) LoadMarketByRef(ctx context.Context, marketRef string) (*model.Market, error) {
	return s.loadMarket(ctx, marketRef)
}

func (s *Service) ResolveMarketCity(ctx context.Context, market model.Market) (string, error) {
	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		return "", err
	}
	return inferCity(market, knownCities), nil
}

func (s *Service) LoadForecastSnapshots(ctx context.Context, city string, targetDate time.Time, marketType string) ([]ForecastSnapshot, error) {
	return s.loadForecastValues(ctx, city, targetDate, marketType)
}

func (s *Service) GetSignalByID(ctx context.Context, id uint64) (*SignalView, error) {
	var row model.Signal
	if err := s.db.WithContext(ctx).Where("id = ?", id).First(&row).Error; err != nil {
		return nil, err
	}
	view := mapSignalView(row)
	return &view, nil
}

func (s *Service) generateSignalForMarket(ctx context.Context, m model.Market, knownCities []string) error {
	city := inferCity(m, knownCities)
	if city == "" {
		return fmt.Errorf("city missing")
	}

	thresholdF, ok := parseThresholdFahrenheit(m.Question)
	if !ok {
		return fmt.Errorf("threshold not found in question")
	}

	targetDate, err := marketForecastDate(m.EndDate)
	if err != nil {
		return err
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

	sourceValues, err := s.loadForecastValues(ctx, city, targetDate, m.MarketType)
	if err != nil {
		return err
	}
	if len(sourceValues) == 0 {
		return fmt.Errorf("no matched forecast values")
	}

	values := make([]float64, 0, len(sourceValues))
	for _, sv := range sourceValues {
		values = append(values, sv.Value)
	}

	ourEstimate := estimateProbability(values, thresholdF, m.MarketType, s.cfg.MinSigma)
	edgePct := (ourEstimate - marketPrice) * 100.0
	if math.Abs(edgePct) < s.cfg.MinEdgePct {
		return fmt.Errorf("edge %.2f below threshold %.2f", edgePct, s.cfg.MinEdgePct)
	}

	direction := "YES"
	if edgePct < 0 {
		direction = "NO"
	}

	_, sigma := meanStd(values)
	if sigma < s.cfg.MinSigma {
		sigma = s.cfg.MinSigma
	}
	confidence := estimateConfidence(len(sourceValues), edgePct, sigma)
	reasoning := buildReasoning(city, thresholdF, targetDate, sourceValues, ourEstimate, marketPrice, edgePct, sigma)

	row := model.Signal{
		MarketID:    m.ID,
		SignalDate:  targetDate,
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
	return s.upsertSignalByDay(ctx, row)
}

func (s *Service) loadMarket(ctx context.Context, marketRef string) (*model.Market, error) {
	ref := strings.TrimSpace(marketRef)
	if ref == "" {
		return nil, fmt.Errorf("market id is required")
	}
	var row model.Market
	if err := s.db.WithContext(ctx).
		Where("id = ? OR polymarket_id = ?", ref, ref).
		Take(&row).Error; err != nil {
		return nil, err
	}
	return &row, nil
}

func (s *Service) upsertSignalByDay(ctx context.Context, row model.Signal) error {
	var existing model.Signal
	err := s.db.WithContext(ctx).
		Where("market_id = ?", row.MarketID).
		Where("signal_type = ?", row.SignalType).
		Where("DATE(signal_date) = ?", row.SignalDate.Format("2006-01-02")).
		Order("created_at DESC").
		First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return s.db.WithContext(ctx).Create(&row).Error
	}
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Model(&model.Signal{}).Where("id = ?", existing.ID).Updates(map[string]any{
		"signal_date":  row.SignalDate,
		"direction":    row.Direction,
		"edge_pct":     row.EdgePct,
		"confidence":   row.Confidence,
		"market_price": row.MarketPrice,
		"our_estimate": row.OurEstimate,
		"reasoning":    row.Reasoning,
		"ai_model":     row.AIModel,
		"created_at":   row.CreatedAt,
	}).Error
}

func (s *Service) loadForecastValues(ctx context.Context, city string, targetDate time.Time, marketType string) ([]forecastSourceValue, error) {
	type row struct {
		Source    string          `gorm:"column:source"`
		City      string          `gorm:"column:city"`
		Location  string          `gorm:"column:location"`
		TempHighF sql.NullFloat64 `gorm:"column:temp_high_f"`
		TempLowF  sql.NullFloat64 `gorm:"column:temp_low_f"`
		FetchedAt time.Time       `gorm:"column:fetched_at"`
	}

	maxAge := s.cfg.ForecastMaxAgeHours
	if maxAge <= 0 {
		maxAge = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(maxAge) * time.Hour)

	var rows []row
	if err := s.db.WithContext(ctx).
		Table("forecasts").
		Select("source, city, location, temp_high_f, temp_low_f, fetched_at").
		Where("DATE(forecast_date) = ?", targetDate.Format("2006-01-02")).
		Where("fetched_at >= ?", cutoff).
		Order("source ASC").
		Order("fetched_at DESC").
		Scan(&rows).Error; err != nil {
		return nil, err
	}

	values := make([]forecastSourceValue, 0, 3)
	seenSource := make(map[string]struct{}, 3)
	for _, r := range rows {
		if _, ok := seenSource[r.Source]; ok {
			continue
		}
		rowCity := normalizeForecastCity(r.City, r.Location)
		if rowCity != city {
			continue
		}

		var picked sql.NullFloat64
		switch marketType {
		case "temperature_low":
			picked = r.TempLowF
		default:
			picked = r.TempHighF
		}
		if !picked.Valid {
			continue
		}

		values = append(values, forecastSourceValue{
			Source:    r.Source,
			Value:     picked.Float64,
			FetchedAt: r.FetchedAt,
		})
		seenSource[r.Source] = struct{}{}
	}
	return values, nil
}

func (s *Service) loadKnownCities(ctx context.Context) ([]string, error) {
	citySet := make(map[string]struct{})
	appendCities := func(values []string) {
		for _, item := range values {
			v := normalizeCityToken(item)
			if v == "" {
				continue
			}
			citySet[v] = struct{}{}
		}
	}

	var stationCities []string
	if err := s.db.WithContext(ctx).
		Model(&model.WeatherStation{}).
		Distinct("city").
		Where("city <> ''").
		Pluck("city", &stationCities).Error; err != nil {
		return nil, err
	}
	appendCities(stationCities)

	var forecastCities []string
	if err := s.db.WithContext(ctx).
		Model(&model.Forecast{}).
		Distinct("city").
		Where("city <> ''").
		Pluck("city", &forecastCities).Error; err != nil {
		return nil, err
	}
	appendCities(forecastCities)

	if len(citySet) == 0 {
		appendCities([]string{
			"new york",
			"chicago",
			"miami",
			"los angeles",
			"austin",
			"dallas",
		})
	}

	cities := make([]string, 0, len(citySet))
	for city := range citySet {
		cities = append(cities, city)
	}
	sort.Slice(cities, func(i, j int) bool {
		return len(cities[i]) > len(cities[j])
	})
	return cities, nil
}

func inferCity(m model.Market, knownCities []string) string {
	if city := normalizeCityToken(m.City); city != "" {
		return city
	}
	q := strings.ToLower(m.Question)
	for _, city := range knownCities {
		if strings.Contains(q, city) {
			return city
		}
	}
	return ""
}

func normalizeForecastCity(city string, location string) string {
	if v := normalizeCityToken(city); v != "" {
		return v
	}
	loc := strings.TrimSpace(location)
	if loc == "" {
		return ""
	}
	parts := strings.Split(loc, ",")
	if len(parts) >= 2 {
		if _, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64); err == nil {
			if _, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err2 == nil {
				return ""
			}
		}
	}
	if idx := strings.Index(loc, ","); idx > 0 {
		loc = loc[:idx]
	}
	return normalizeCityToken(loc)
}

func normalizeCityToken(city string) string {
	v := strings.ToLower(strings.TrimSpace(city))
	if v == "" {
		return ""
	}
	return strings.Join(strings.Fields(v), " ")
}

func marketForecastDate(endDate time.Time) (time.Time, error) {
	if endDate.IsZero() {
		return time.Time{}, fmt.Errorf("market end_date missing")
	}
	target := endDate.UTC().AddDate(0, 0, -1)
	return time.Date(target.Year(), target.Month(), target.Day(), 0, 0, 0, 0, time.UTC), nil
}

func buildReasoning(city string, thresholdF float64, targetDate time.Time, values []forecastSourceValue, ourEstimate float64, marketPrice float64, edgePct float64, sigma float64) string {
	sort.Slice(values, func(i, j int) bool {
		return values[i].Source < values[j].Source
	})
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%s=%.1fF", v.Source, v.Value))
	}
	return fmt.Sprintf(
		"city=%s date=%s threshold=%.1fF forecasts=[%s] sigma=%.2f estimate=%.4f market=%.4f edge=%.2f%%",
		city,
		targetDate.Format("2006-01-02"),
		thresholdF,
		strings.Join(parts, ", "),
		sigma,
		ourEstimate,
		marketPrice,
		edgePct,
	)
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

func estimateProbability(values []float64, threshold float64, marketType string, minSigma float64) float64 {
	if minSigma <= 0 {
		minSigma = 0.5
	}

	mu, sigma := meanStd(values)
	if sigma < minSigma {
		sigma = minSigma
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

func estimateConfidence(sourceCount int, edgePct float64, sigma float64) float64 {
	// Confidence combines edge strength, source coverage, and model dispersion.
	edgeComponent := math.Min(40.0, math.Abs(edgePct)*3.0)
	sourceComponent := math.Min(20.0, float64(sourceCount)*5.0)
	dispersionPenalty := math.Min(20.0, sigma*6.0)
	conf := 45.0 + edgeComponent + sourceComponent - dispersionPenalty
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

func mapSignalView(row model.Signal) SignalView {
	return SignalView{
		ID:          row.ID,
		MarketID:    row.MarketID,
		SignalDate:  row.SignalDate,
		Direction:   row.Direction,
		EdgePct:     row.EdgePct,
		Confidence:  row.Confidence,
		MarketPrice: row.MarketPrice,
		OurEstimate: row.OurEstimate,
		Reasoning:   row.Reasoning,
		CreatedAt:   row.CreatedAt,
	}
}
