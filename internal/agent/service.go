package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

type Service struct {
	cfg       config.AgentConfig
	db        *gorm.DB
	log       *zap.Logger
	weather   *weather.Service
	signalSvc *signal.Service
}

type forecastSnapshot struct {
	Source    string
	Value     float64
	FetchedAt time.Time
}

func NewService(cfg config.AgentConfig, db *gorm.DB, log *zap.Logger, weatherSvc *weather.Service, signalSvc *signal.Service) *Service {
	return &Service{
		cfg:       cfg,
		db:        db,
		log:       log,
		weather:   weatherSvc,
		signalSvc: signalSvc,
	}
}

func (s *Service) Analyze(ctx context.Context, req AnalyzeRequest) (*AnalyzeResponse, error) {
	markets, err := s.selectMarkets(ctx, req)
	if err != nil {
		return nil, err
	}

	resp := &AnalyzeResponse{
		SessionID:   uuid.NewString(),
		GeneratedAt: time.Now().UTC(),
		Items:       make([]AnalyzeItem, 0, len(markets)),
		Errors:      make([]string, 0),
		Count:       len(markets),
	}

	for _, market := range markets {
		item, analyzeErr := s.analyzeMarket(ctx, resp.SessionID, market, req.Depth)
		if analyzeErr != nil {
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %v", market.PolymarketID, analyzeErr))
			continue
		}
		resp.Items = append(resp.Items, *item)
	}
	resp.Count = len(resp.Items)
	return resp, nil
}

func (s *Service) analyzeMarket(ctx context.Context, sessionID string, m model.Market, depth string) (*AnalyzeItem, error) {
	start := time.Now()
	toolCalls := make([]ToolCall, 0, 4)
	riskFactors := make([]string, 0)

	targetDate := m.EndDate.UTC().AddDate(0, 0, -1)
	targetDate = time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 0, 0, 0, 0, time.UTC)

	city := normalizeCity(m.City)
	if city == "" {
		city = inferCityFromQuestion(m.Question)
	}
	if city == "" {
		riskFactors = append(riskFactors, "market city is missing")
	}

	toolStart := time.Now()
	priceYes, loadPriceErr := s.loadLatestMarketPrice(ctx, m.ID)
	toolCalls = append(toolCalls, newToolCall("get_market_prices", map[string]any{
		"market_id": m.ID,
	}, toolStart, loadPriceErr))
	if loadPriceErr != nil {
		return nil, loadPriceErr
	}

	toolStart = time.Now()
	forecastValues, forecastErr := s.loadForecastBySource(ctx, city, targetDate, m.MarketType)
	toolCalls = append(toolCalls, newToolCall("get_weather_forecast", map[string]any{
		"location": city,
		"date":     targetDate.Format("2006-01-02"),
		"source":   "db",
	}, toolStart, forecastErr))

	if forecastErr != nil && city != "" && s.weather != nil {
		toolStart = time.Now()
		days := daysUntil(targetDate)
		_, refreshErr := s.weather.GetForecast(ctx, weather.ForecastRequest{
			Location: city,
			Source:   "all",
			Days:     days,
		})
		toolCalls = append(toolCalls, newToolCall("get_weather_forecast", map[string]any{
			"location": city,
			"source":   "all",
			"days":     days,
			"refresh":  true,
		}, toolStart, refreshErr))
		if refreshErr != nil {
			riskFactors = append(riskFactors, "weather refresh failed: "+refreshErr.Error())
		}

		toolStart = time.Now()
		forecastValues, forecastErr = s.loadForecastBySource(ctx, city, targetDate, m.MarketType)
		toolCalls = append(toolCalls, newToolCall("get_weather_forecast", map[string]any{
			"location": city,
			"date":     targetDate.Format("2006-01-02"),
			"source":   "db_after_refresh",
		}, toolStart, forecastErr))
	}

	forecastSummary := "no forecast snapshot available"
	if len(forecastValues) > 0 {
		forecastSummary = summarizeForecasts(forecastValues)
		oldestAge := oldestForecastAgeHours(forecastValues)
		if oldestAge > 24 {
			riskFactors = append(riskFactors, fmt.Sprintf("forecast data stale: oldest %.0fh", oldestAge))
		}
	}
	if forecastErr != nil {
		riskFactors = append(riskFactors, "forecast unavailable")
	}

	toolStart = time.Now()
	generatedSignal, signalErr := s.signalSvc.GenerateSignalForMarketID(ctx, m.ID)
	toolCalls = append(toolCalls, newToolCall("calculate_edge", map[string]any{
		"market_id": m.ID,
		"signal":    "forecast_edge",
	}, toolStart, signalErr))

	var (
		ourProbability = priceYes
		edgePct        = 0.0
		confidence     = 50.0
		reasoning      = "insufficient data to compute edge; default hold"
	)
	if signalErr == nil && generatedSignal != nil {
		ourProbability = generatedSignal.OurEstimate
		edgePct = generatedSignal.EdgePct
		confidence = generatedSignal.Confidence
		reasoning = generatedSignal.Reasoning
	} else {
		latestSignal, latestErr := s.loadLatestSignalForMarket(ctx, m.ID)
		if latestErr == nil && latestSignal != nil {
			ourProbability = latestSignal.OurEstimate
			edgePct = latestSignal.EdgePct
			confidence = latestSignal.Confidence
			reasoning = latestSignal.Reasoning
			riskFactors = append(riskFactors, "using latest historical signal due to generation failure")
		}
		if signalErr != nil {
			riskFactors = append(riskFactors, "signal generation failed: "+signalErr.Error())
		}
	}

	recommendation := buildRecommendation(edgePct, priceYes)
	if depth == "full" && len(forecastValues) > 0 {
		reasoning = reasoning + "; forecasts: " + forecastSummary
	}

	item := &AnalyzeItem{
		MarketID:     m.ID,
		PolymarketID: m.PolymarketID,
		Question:     m.Question,
		ToolCalls:    toolCalls,
		Analysis: AnalysisBlock{
			ForecastSummary: forecastSummary,
			MarketPriceYes:  priceYes,
			OurProbability:  ourProbability,
			EdgePct:         edgePct,
			Confidence:      confidence,
			Recommendation:  recommendation,
			Reasoning:       reasoning,
			RiskFactors:     riskFactors,
		},
	}

	if err := s.persistAgentLog(ctx, sessionID, m.ID, item, time.Since(start)); err != nil {
		s.log.Warn("persist agent log failed", zap.Error(err), zap.String("market_id", m.ID))
	}
	return item, nil
}

func (s *Service) selectMarkets(ctx context.Context, req AnalyzeRequest) ([]model.Market, error) {
	if strings.TrimSpace(req.MarketID) != "" && !req.All {
		m, err := s.loadMarket(ctx, req.MarketID)
		if err != nil {
			return nil, err
		}
		return []model.Market{*m}, nil
	}

	limit := req.Limit
	if limit <= 0 {
		limit = s.cfg.AnalyzeLimit
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}

	var markets []model.Market
	err := s.db.WithContext(ctx).
		Where("is_active = ?", true).
		Where("market_type IN ?", []string{"temperature_high", "temperature_low"}).
		Order("end_date ASC").
		Limit(limit).
		Find(&markets).Error
	if err != nil {
		return nil, err
	}
	return markets, nil
}

func (s *Service) loadMarket(ctx context.Context, marketRef string) (*model.Market, error) {
	ref := strings.TrimSpace(marketRef)
	if ref == "" {
		return nil, fmt.Errorf("market id is required")
	}
	var m model.Market
	err := s.db.WithContext(ctx).
		Where("id = ? OR polymarket_id = ?", ref, ref).
		Take(&m).Error
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Service) loadLatestMarketPrice(ctx context.Context, marketID string) (float64, error) {
	var row model.MarketPrice
	if err := s.db.WithContext(ctx).
		Where("market_id = ?", marketID).
		Order("captured_at DESC").
		First(&row).Error; err != nil {
		return 0, err
	}
	return row.PriceYes.InexactFloat64(), nil
}

func (s *Service) loadForecastBySource(ctx context.Context, city string, targetDate time.Time, marketType string) ([]forecastSnapshot, error) {
	if city == "" {
		return nil, fmt.Errorf("city missing")
	}

	type row struct {
		Source    string          `gorm:"column:source"`
		TempHighF sql.NullFloat64 `gorm:"column:temp_high_f"`
		TempLowF  sql.NullFloat64 `gorm:"column:temp_low_f"`
		FetchedAt time.Time       `gorm:"column:fetched_at"`
	}
	var rows []row
	if err := s.db.WithContext(ctx).
		Table("forecasts").
		Select("source, temp_high_f, temp_low_f, fetched_at").
		Where("city = ?", city).
		Where("DATE(forecast_date) = ?", targetDate.Format("2006-01-02")).
		Order("source ASC").
		Order("fetched_at DESC").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no forecast found")
	}

	out := make([]forecastSnapshot, 0, 3)
	seen := make(map[string]struct{}, 3)
	for _, r := range rows {
		if _, ok := seen[r.Source]; ok {
			continue
		}
		val := sql.NullFloat64{}
		if marketType == "temperature_low" {
			val = r.TempLowF
		} else {
			val = r.TempHighF
		}
		if !val.Valid {
			continue
		}
		out = append(out, forecastSnapshot{
			Source:    r.Source,
			Value:     val.Float64,
			FetchedAt: r.FetchedAt,
		})
		seen[r.Source] = struct{}{}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no usable forecast values")
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Source < out[j].Source
	})
	return out, nil
}

func (s *Service) loadLatestSignalForMarket(ctx context.Context, marketID string) (*signal.SignalView, error) {
	var row model.Signal
	err := s.db.WithContext(ctx).
		Where("market_id = ?", marketID).
		Order("created_at DESC").
		First(&row).Error
	if err != nil {
		return nil, err
	}
	return &signal.SignalView{
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
	}, nil
}

func (s *Service) persistAgentLog(ctx context.Context, sessionID string, marketID string, item *AnalyzeItem, elapsed time.Duration) error {
	if item == nil {
		return nil
	}
	toolCallsJSON, err := json.Marshal(item.ToolCalls)
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(item.Analysis)
	if err != nil {
		return err
	}

	row := model.AgentLog{
		SessionID:  sessionID,
		MarketID:   marketID,
		Action:     "analyze",
		Model:      s.cfg.Model,
		ToolCalls:  datatypes.JSON(toolCallsJSON),
		Reasoning:  item.Analysis.Reasoning,
		Result:     datatypes.JSON(resultJSON),
		DurationMS: int(elapsed.Milliseconds()),
		CreatedAt:  time.Now().UTC(),
	}
	return s.db.WithContext(ctx).Create(&row).Error
}

func newToolCall(name string, args map[string]any, start time.Time, callErr error) ToolCall {
	tc := ToolCall{
		Name:       name,
		Args:       args,
		DurationMS: time.Since(start).Milliseconds(),
		Status:     "ok",
	}
	if callErr != nil {
		tc.Status = "error"
		tc.Error = callErr.Error()
	}
	return tc
}

func summarizeForecasts(items []forecastSnapshot) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s=%.1fF", item.Source, item.Value))
	}
	return strings.Join(parts, ", ")
}

func oldestForecastAgeHours(items []forecastSnapshot) float64 {
	if len(items) == 0 {
		return 0
	}
	oldest := items[0].FetchedAt
	for _, item := range items {
		if item.FetchedAt.Before(oldest) {
			oldest = item.FetchedAt
		}
	}
	return math.Max(0, time.Since(oldest).Hours())
}

func buildRecommendation(edgePct float64, marketPrice float64) string {
	if math.Abs(edgePct) < 1 {
		return "HOLD (no clear edge)"
	}
	if edgePct > 0 {
		return fmt.Sprintf("BUY YES at %.4f or better", marketPrice)
	}
	return fmt.Sprintf("BUY NO at %.4f or better", 1-marketPrice)
}

func daysUntil(targetDate time.Time) int {
	days := int(targetDate.Sub(time.Now().UTC()).Hours()/24) + 1
	if days < 1 {
		return 1
	}
	if days > 10 {
		return 10
	}
	return days
}

func normalizeCity(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ""
	}
	return strings.Join(strings.Fields(v), " ")
}

func inferCityFromQuestion(question string) string {
	q := strings.ToLower(strings.TrimSpace(question))
	if q == "" {
		return ""
	}
	known := []string{
		"new york",
		"chicago",
		"miami",
		"los angeles",
		"austin",
		"dallas",
	}
	for _, city := range known {
		if strings.Contains(q, city) {
			return city
		}
	}
	return ""
}
