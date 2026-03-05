package signal

import (
	"context"
	"database/sql"
	"encoding/json"
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

var thresholdPattern = regexp.MustCompile(`(?i)(?:above|below|over|under|exceed|at least|at most|no more than|no less than)[^0-9-]{0,24}(-?\d+(?:\.\d+)?)\s*°?\s*([FC])?`)
var thresholdLoosePattern = regexp.MustCompile(`(?i)(-?\d+(?:\.\d+)?)\s*°?\s*([FC])`)

type Service struct {
	cfg          config.SignalConfig
	db           *gorm.DB
	log          *zap.Logger
	cityResolver *cityResolver
}

type forecastSourceValue = ForecastSnapshot

const (
	skipReasonSpecIncomplete     = "spec_incomplete"
	skipReasonCityMissing        = "city_missing"
	skipReasonThresholdMissing   = "threshold_missing"
	skipReasonLatestPriceMissing = "latest_price_missing"
	skipReasonInvalidMarketPrice = "invalid_market_price"
	skipReasonNoForecast         = "no_forecast"
	skipReasonRawEdgeBelow       = "edge_below_raw_threshold"
	skipReasonExecEdgeBelow      = "edge_below_exec_threshold"
	skipReasonPersistFailed      = "persist_failed"
	skipReasonUnsupportedMarket  = "unsupported_market_type"
)

type marketSpec struct {
	City       string
	ThresholdF float64
	Comparator string
	TargetDate time.Time
	Status     string
}

type cityAlias struct {
	Alias string
	City  string
}

type signalEval struct {
	SpecReady     bool
	ForecastReady bool
	RawEdgePass   bool
	ExecEdgePass  bool
	Generated     bool
	SkipReason    string
}

var defaultCityAliases = []cityAlias{
	{Alias: "new york city", City: "new york"},
	{Alias: "new york", City: "new york"},
	{Alias: "los angeles", City: "los angeles"},
	{Alias: "san francisco", City: "san francisco"},
	{Alias: "san diego", City: "san diego"},
	{Alias: "las vegas", City: "las vegas"},
	{Alias: "salt lake city", City: "salt lake city"},
	{Alias: "washington dc", City: "washington dc"},
	{Alias: "washington, dc", City: "washington dc"},
	{Alias: "philadelphia", City: "philadelphia"},
	{Alias: "new orleans", City: "new orleans"},
	{Alias: "st louis", City: "st louis"},
	{Alias: "kansas city", City: "kansas city"},
	{Alias: "minneapolis", City: "minneapolis"},
	{Alias: "chicago", City: "chicago"},
	{Alias: "miami", City: "miami"},
	{Alias: "boston", City: "boston"},
	{Alias: "seattle", City: "seattle"},
	{Alias: "atlanta", City: "atlanta"},
	{Alias: "denver", City: "denver"},
	{Alias: "houston", City: "houston"},
	{Alias: "phoenix", City: "phoenix"},
	{Alias: "dallas", City: "dallas"},
	{Alias: "austin", City: "austin"},
	{Alias: "orlando", City: "orlando"},
	{Alias: "tampa", City: "tampa"},
	{Alias: "portland", City: "portland"},
	{Alias: "detroit", City: "detroit"},
	{Alias: "pittsburgh", City: "pittsburgh"},
	{Alias: "cleveland", City: "cleveland"},
	// Common airport/station aliases used by weather markets.
	{Alias: "jfk", City: "new york"},
	{Alias: "lga", City: "new york"},
	{Alias: "ewr", City: "new york"},
	{Alias: "central park", City: "new york"},
	{Alias: "ord", City: "chicago"},
	{Alias: "ohare", City: "chicago"},
	{Alias: "o'hare", City: "chicago"},
	{Alias: "lax", City: "los angeles"},
	{Alias: "burbank", City: "los angeles"},
	{Alias: "sfo", City: "san francisco"},
	{Alias: "oak", City: "san francisco"},
	{Alias: "sea", City: "seattle"},
	{Alias: "bos", City: "boston"},
	{Alias: "mia", City: "miami"},
	{Alias: "atl", City: "atlanta"},
	{Alias: "den", City: "denver"},
	{Alias: "iah", City: "houston"},
	{Alias: "hou", City: "houston"},
	{Alias: "phx", City: "phoenix"},
	{Alias: "dfw", City: "dallas"},
	{Alias: "dal", City: "dallas"},
	{Alias: "aus", City: "austin"},
	{Alias: "mco", City: "orlando"},
	{Alias: "tpa", City: "tampa"},
	{Alias: "pdx", City: "portland"},
	{Alias: "dtw", City: "detroit"},
	{Alias: "pit", City: "pittsburgh"},
	{Alias: "cle", City: "cleveland"},
	{Alias: "las", City: "las vegas"},
}

func NewService(cfg config.SignalConfig, db *gorm.DB, log *zap.Logger) *Service {
	return &Service{
		cfg:          cfg,
		db:           db,
		log:          log,
		cityResolver: newCityResolver(db, log),
	}
}

func (s *Service) GenerateSignals(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	// Keep signal generation deterministic and low-cost; reserve Vertex city resolution
	// for dedicated spec-fill workflows.
	ctx = WithCityResolverVertexDisabled(ctx)
	startedAt := time.Now().UTC()
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
		Processed:    len(markets),
		MarketsTotal: len(markets),
		SkipReasons:  make(map[string]int),
		Errors:       make([]string, 0),
	}
	if len(markets) == 0 {
		s.persistSignalRun(ctx, result, startedAt, time.Now().UTC())
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
			eval, genErr := s.generateSignalForMarket(gctx, m, knownCities)
			mu.Lock()
			defer mu.Unlock()
			if eval.SpecReady {
				result.SpecReady++
			}
			if eval.ForecastReady {
				result.ForecastReady++
			}
			if eval.RawEdgePass {
				result.RawEdgePass++
			}
			if eval.ExecEdgePass {
				result.ExecEdgePass++
			}
			if eval.SkipReason != "" {
				result.SkipReasons[eval.SkipReason]++
			}
			if genErr != nil {
				result.Skipped++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", m.PolymarketID, genErr))
				if eval.SkipReason == "" {
					result.SkipReasons[skipReasonPersistFailed]++
				}
				if errors.Is(genErr, context.Canceled) || errors.Is(genErr, context.DeadlineExceeded) {
					return genErr
				}
				return nil
			}
			if eval.Generated {
				result.Generated++
			} else {
				result.Skipped++
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		s.persistSignalRun(ctx, result, startedAt, time.Now().UTC())
		return result, err
	}
	s.persistSignalRun(ctx, result, startedAt, time.Now().UTC())
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
	// Keep single-market generation deterministic and low-cost.
	ctx = WithCityResolverVertexDisabled(ctx)
	market, err := s.LoadMarketByRef(ctx, marketRef)
	if err != nil {
		return nil, err
	}
	knownCities, err := s.loadKnownCities(ctx)
	if err != nil {
		s.log.Warn("load known cities failed", zap.Error(err))
	}
	eval, err := s.generateSignalForMarket(ctx, *market, knownCities)
	if err != nil {
		return nil, err
	}
	if !eval.Generated {
		if eval.SkipReason == "" {
			return nil, fmt.Errorf("signal skipped")
		}
		return nil, fmt.Errorf("signal skipped: %s", eval.SkipReason)
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
	return s.resolveCity(ctx, market, knownCities), nil
}

func (s *Service) resolveCity(ctx context.Context, market model.Market, knownCities []string) string {
	if s.cityResolver == nil {
		return ""
	}
	return s.cityResolver.Resolve(ctx, market, knownCities)
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

func (s *Service) generateSignalForMarket(ctx context.Context, m model.Market, knownCities []string) (signalEval, error) {
	eval := signalEval{}
	spec, skipReason, err := s.ensureMarketSpec(ctx, m, knownCities)
	if err != nil {
		return eval, err
	}
	if skipReason != "" {
		eval.SkipReason = skipReason
		return eval, nil
	}
	eval.SpecReady = true
	marketType := resolveMarketType(m.MarketType, spec.Comparator, m.Question)
	if marketType == "" {
		eval.SkipReason = skipReasonUnsupportedMarket
		return eval, nil
	}

	var latestPrice model.MarketPrice
	if err := s.db.WithContext(ctx).
		Where("market_id = ?", m.ID).
		Order("captured_at DESC").
		First(&latestPrice).Error; err != nil {
		eval.SkipReason = skipReasonLatestPriceMissing
		return eval, fmt.Errorf("latest price not found: %w", err)
	}
	marketPrice := latestPrice.PriceYes.InexactFloat64()
	if marketPrice <= 0 || marketPrice >= 1 {
		eval.SkipReason = skipReasonInvalidMarketPrice
		return eval, fmt.Errorf("invalid market price: %.4f", marketPrice)
	}

	sourceValues, err := s.loadForecastValues(ctx, spec.City, spec.TargetDate, marketType)
	if err != nil {
		return eval, err
	}
	if len(sourceValues) == 0 {
		eval.SkipReason = skipReasonNoForecast
		return eval, nil
	}
	eval.ForecastReady = true

	values := make([]float64, 0, len(sourceValues))
	for _, sv := range sourceValues {
		values = append(values, sv.Value)
	}

	ourEstimate := estimateProbability(values, spec.ThresholdF, marketType, s.cfg.MinSigma)
	rawEdgePct := (ourEstimate - marketPrice) * 100.0
	if math.Abs(rawEdgePct) < s.cfg.MinEdgePct {
		eval.SkipReason = skipReasonRawEdgeBelow
		return eval, nil
	}
	eval.RawEdgePass = true

	marketPriceExecutable := selectExecutableMarketPrice(latestPrice, rawEdgePct)
	execEdgePct := (ourEstimate - marketPriceExecutable) * 100.0
	frictionPct := s.computeFrictionPct(latestPrice)
	execEdgePct = applyFriction(execEdgePct, frictionPct)
	minExecEdge := s.cfg.MinEdgeExecPct
	if minExecEdge <= 0 {
		minExecEdge = s.cfg.MinEdgePct
	}
	if math.Abs(execEdgePct) < minExecEdge {
		eval.SkipReason = skipReasonExecEdgeBelow
		return eval, nil
	}
	eval.ExecEdgePass = true

	direction := "YES"
	if execEdgePct < 0 {
		direction = "NO"
	}

	_, sigma := meanStd(values)
	if sigma < s.cfg.MinSigma {
		sigma = s.cfg.MinSigma
	}
	confidence := estimateConfidence(len(sourceValues), execEdgePct, sigma)
	reasoning := buildReasoning(spec.City, spec.ThresholdF, spec.TargetDate, sourceValues, ourEstimate, marketPrice, rawEdgePct, execEdgePct, frictionPct, sigma)

	row := model.Signal{
		MarketID:              m.ID,
		SignalDate:            spec.TargetDate,
		SignalType:            "forecast_edge",
		Direction:             direction,
		EdgePct:               rawEdgePct,
		EdgeExecPct:           execEdgePct,
		Confidence:            confidence,
		MarketPrice:           marketPrice,
		MarketPriceExecutable: marketPriceExecutable,
		OurEstimate:           ourEstimate,
		FrictionPct:           frictionPct,
		Reasoning:             reasoning,
		AIModel:               "probability-model-v1",
		ActedOn:               false,
		CreatedAt:             time.Now().UTC(),
	}
	if err := s.upsertSignalByDay(ctx, row); err != nil {
		eval.SkipReason = skipReasonPersistFailed
		return eval, err
	}
	eval.Generated = true
	return eval, nil
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

func (s *Service) ensureMarketSpec(ctx context.Context, m model.Market, knownCities []string) (marketSpec, string, error) {
	marketType := strings.ToLower(strings.TrimSpace(m.MarketType))
	comparator := normalizeComparator(m.Comparator, marketType)
	if comparator == "" {
		comparator = inferComparatorFromQuestion(m.Question)
	}

	spec := marketSpec{
		City:       s.resolveCity(ctx, m, knownCities),
		Comparator: comparator,
		Status:     strings.ToLower(strings.TrimSpace(m.SpecStatus)),
	}
	if spec.Status == "" {
		spec.Status = "incomplete"
	}

	if m.WeatherTargetDate != nil && !m.WeatherTargetDate.IsZero() {
		spec.TargetDate = time.Date(m.WeatherTargetDate.Year(), m.WeatherTargetDate.Month(), m.WeatherTargetDate.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		targetDate, err := marketForecastDate(m.EndDate)
		if err != nil {
			return spec, skipReasonSpecIncomplete, err
		}
		spec.TargetDate = targetDate
	}

	if m.ThresholdF.Valid {
		spec.ThresholdF = m.ThresholdF.Decimal.InexactFloat64()
	} else {
		parsed, ok := parseThresholdFahrenheit(m.Question)
		if !ok {
			spec.Status = "missing_threshold"
			_ = s.persistMarketSpec(ctx, m.ID, spec)
			return spec, skipReasonThresholdMissing, nil
		}
		spec.ThresholdF = parsed
	}
	if spec.City == "" {
		spec.Status = "missing_city"
		_ = s.persistMarketSpec(ctx, m.ID, spec)
		return spec, skipReasonCityMissing, nil
	}
	if spec.Comparator == "" || spec.TargetDate.IsZero() {
		spec.Status = "incomplete"
		_ = s.persistMarketSpec(ctx, m.ID, spec)
		return spec, skipReasonSpecIncomplete, nil
	}
	spec.Status = "ready"

	if err := s.persistMarketSpec(ctx, m.ID, spec); err != nil {
		return spec, "", err
	}
	return spec, "", nil
}

func (s *Service) persistMarketSpec(ctx context.Context, marketID string, spec marketSpec) error {
	update := map[string]any{
		"city":                spec.City,
		"threshold_f":         spec.ThresholdF,
		"comparator":          spec.Comparator,
		"weather_target_date": spec.TargetDate,
		"spec_status":         spec.Status,
	}
	return s.db.WithContext(ctx).
		Table("markets").
		Where("id = ?", marketID).
		Updates(update).Error
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
		"signal_date":             row.SignalDate,
		"direction":               row.Direction,
		"edge_pct":                row.EdgePct,
		"edge_exec_pct":           row.EdgeExecPct,
		"confidence":              row.Confidence,
		"market_price":            row.MarketPrice,
		"market_price_executable": row.MarketPriceExecutable,
		"our_estimate":            row.OurEstimate,
		"friction_pct":            row.FrictionPct,
		"reasoning":               row.Reasoning,
		"ai_model":                row.AIModel,
		"created_at":              row.CreatedAt,
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
	v = strings.Join(strings.Fields(v), " ")
	return canonicalCityAlias(v)
}

func canonicalCityAlias(city string) string {
	token := strings.TrimSpace(strings.ToLower(city))
	if token == "" {
		return ""
	}
	for _, alias := range defaultCityAliases {
		if token == alias.Alias {
			return alias.City
		}
	}
	return token
}

func marketForecastDate(endDate time.Time) (time.Time, error) {
	if endDate.IsZero() {
		return time.Time{}, fmt.Errorf("market end_date missing")
	}
	target := endDate.UTC().AddDate(0, 0, -1)
	return time.Date(target.Year(), target.Month(), target.Day(), 0, 0, 0, 0, time.UTC), nil
}

func buildReasoning(city string, thresholdF float64, targetDate time.Time, values []forecastSourceValue, ourEstimate float64, marketPrice float64, rawEdgePct float64, execEdgePct float64, frictionPct float64, sigma float64) string {
	sort.Slice(values, func(i, j int) bool {
		return values[i].Source < values[j].Source
	})
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%s=%.1fF", v.Source, v.Value))
	}
	return fmt.Sprintf(
		"city=%s date=%s threshold=%.1fF forecasts=[%s] sigma=%.2f estimate=%.4f market=%.4f edge_raw=%.2f%% edge_exec=%.2f%% friction=%.2f%%",
		city,
		targetDate.Format("2006-01-02"),
		thresholdF,
		strings.Join(parts, ", "),
		sigma,
		ourEstimate,
		marketPrice,
		rawEdgePct,
		execEdgePct,
		frictionPct,
	)
}

func parseThresholdFahrenheit(question string) (float64, bool) {
	matches := thresholdPattern.FindStringSubmatch(question)
	if len(matches) >= 2 {
		value, err := strconvParseFloat(matches[1])
		if err == nil {
			unit := "F"
			if len(matches) >= 3 && strings.TrimSpace(matches[2]) != "" {
				unit = strings.ToUpper(matches[2])
			}
			if unit == "C" {
				value = (value * 9.0 / 5.0) + 32.0
			}
			return value, true
		}
	}

	loose := thresholdLoosePattern.FindStringSubmatch(question)
	if len(loose) >= 3 {
		value, err := strconvParseFloat(loose[1])
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

func resolveMarketType(raw string, comparator string, question string) string {
	mt := strings.TrimSpace(strings.ToLower(raw))
	switch mt {
	case "temperature_high", "temperature_low":
		return mt
	}
	switch comparator {
	case "ge":
		return "temperature_high"
	case "le":
		return "temperature_low"
	}
	q := strings.ToLower(question)
	switch {
	case strings.Contains(q, "high temperature"), strings.Contains(q, "high temp"), strings.Contains(q, "above"), strings.Contains(q, "exceed"), strings.Contains(q, "higher"), strings.Contains(q, "at or above"):
		return "temperature_high"
	case strings.Contains(q, "low temperature"), strings.Contains(q, "low temp"), strings.Contains(q, "below"), strings.Contains(q, "under"), strings.Contains(q, "lower"), strings.Contains(q, "at or below"):
		return "temperature_low"
	default:
		return ""
	}
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

func normalizeComparator(value string, marketType string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "ge", ">=", ">":
		return "ge"
	case "le", "<=", "<":
		return "le"
	}
	switch strings.ToLower(strings.TrimSpace(marketType)) {
	case "temperature_low":
		return "le"
	case "temperature_high":
		return "ge"
	default:
		return ""
	}
}

func inferComparatorFromQuestion(question string) string {
	q := strings.ToLower(strings.TrimSpace(question))
	switch {
	case strings.Contains(q, "below"),
		strings.Contains(q, "under"),
		strings.Contains(q, "at most"),
		strings.Contains(q, "no more than"),
		strings.Contains(q, "lower"),
		strings.Contains(q, "at or below"):
		return "le"
	case strings.Contains(q, "above"),
		strings.Contains(q, "over"),
		strings.Contains(q, "exceed"),
		strings.Contains(q, "at least"),
		strings.Contains(q, "no less than"),
		strings.Contains(q, "higher"),
		strings.Contains(q, "at or above"):
		return "ge"
	default:
		return ""
	}
}

func selectExecutableMarketPrice(price model.MarketPrice, rawEdgePct float64) float64 {
	market := price.PriceYes.InexactFloat64()
	if rawEdgePct >= 0 {
		if price.AskYes.Valid {
			v := price.AskYes.Decimal.InexactFloat64()
			if v > 0 && v < 1 {
				return v
			}
		}
		return market
	}
	if price.BidYes.Valid {
		v := price.BidYes.Decimal.InexactFloat64()
		if v > 0 && v < 1 {
			return v
		}
	}
	return market
}

func applyFriction(edgePct float64, frictionPct float64) float64 {
	if frictionPct <= 0 {
		return edgePct
	}
	if edgePct > 0 {
		return edgePct - frictionPct
	}
	return edgePct + frictionPct
}

func (s *Service) computeFrictionPct(latestPrice model.MarketPrice) float64 {
	friction := s.cfg.ExecFeePct + s.cfg.ExecSlippagePct
	if latestPrice.Spread.Valid {
		friction += latestPrice.Spread.Decimal.InexactFloat64() * 100.0
	}
	if friction < 0 {
		return 0
	}
	return friction
}

func (s *Service) persistSignalRun(ctx context.Context, result *GenerateResult, startedAt time.Time, finishedAt time.Time) {
	if result == nil {
		return
	}
	payload, err := json.Marshal(result.SkipReasons)
	if err != nil {
		payload = []byte("{}")
	}
	run := model.SignalRun{
		StartedAt:        startedAt,
		FinishedAt:       finishedAt,
		DurationMS:       int(finishedAt.Sub(startedAt).Milliseconds()),
		MarketsTotal:     result.MarketsTotal,
		SpecReady:        result.SpecReady,
		ForecastReady:    result.ForecastReady,
		RawEdgePass:      result.RawEdgePass,
		ExecEdgePass:     result.ExecEdgePass,
		SignalsGenerated: result.Generated,
		Skipped:          result.Skipped,
		SkipReasonsJSON:  payload,
		CreatedAt:        finishedAt,
	}
	if err := s.db.WithContext(ctx).Create(&run).Error; err != nil {
		if s.log != nil {
			s.log.Warn("persist signal run failed", zap.Error(err))
		}
		return
	}
	result.SignalRunID = run.ID
}

func mapSignalView(row model.Signal) SignalView {
	return SignalView{
		ID:                    row.ID,
		MarketID:              row.MarketID,
		SignalDate:            row.SignalDate,
		Direction:             row.Direction,
		EdgePct:               row.EdgePct,
		EdgeExecPct:           row.EdgeExecPct,
		Confidence:            row.Confidence,
		MarketPrice:           row.MarketPrice,
		MarketPriceExecutable: row.MarketPriceExecutable,
		OurEstimate:           row.OurEstimate,
		FrictionPct:           row.FrictionPct,
		Reasoning:             row.Reasoning,
		CreatedAt:             row.CreatedAt,
	}
}
