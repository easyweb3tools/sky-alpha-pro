package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

const (
	defaultAnalyzeConcurrency = 8
	defaultMarketTimeout      = 20 * time.Second
	defaultForecastDaysCap    = 10
	defaultStaleHoursLimit    = 24.0
	ruleFallbackModelName     = "rule-based-fallback-v1"
)

type Service struct {
	cfg       config.AgentConfig
	db        *gorm.DB
	log       *zap.Logger
	weather   *weather.Service
	signalSvc *signal.Service
	vertexAI  *vertexAIClient
}

func NewService(cfg config.AgentConfig, db *gorm.DB, log *zap.Logger, weatherSvc *weather.Service, signalSvc *signal.Service) *Service {
	vertexAI, err := newVertexAIClient(cfg, log)
	if err != nil && log != nil {
		log.Warn("vertex ai init failed; fallback to rule-based mode", zap.Error(err))
	}

	return &Service{
		cfg:       cfg,
		db:        db,
		log:       log,
		weather:   weatherSvc,
		signalSvc: signalSvc,
		vertexAI:  vertexAI,
	}
}

func (s *Service) Analyze(ctx context.Context, req AnalyzeRequest) (*AnalyzeResponse, error) {
	depth, err := ValidateDepth(req.Depth)
	if err != nil {
		return nil, err
	}

	markets, err := s.selectMarkets(ctx, req)
	if err != nil {
		return nil, err
	}

	resp := &AnalyzeResponse{
		SessionID:   uuid.NewString(),
		GeneratedAt: time.Now().UTC(),
		Items:       make([]AnalyzeItem, 0, len(markets)),
		Errors:      make([]string, 0),
		Count:       0,
	}
	if len(markets) == 0 {
		return resp, nil
	}

	workers := s.cfg.Concurrency
	if workers <= 0 {
		workers = defaultAnalyzeConcurrency
	}
	marketTimeout := s.cfg.MarketTimeout
	if marketTimeout <= 0 {
		marketTimeout = defaultMarketTimeout
	}

	type indexedItem struct {
		Index int
		Item  AnalyzeItem
	}

	var (
		mu        sync.Mutex
		collected []indexedItem
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for idx, market := range markets {
		i := idx
		m := market
		g.Go(func() error {
			marketCtx, cancel := context.WithTimeout(gctx, marketTimeout)
			defer cancel()

			item, analyzeErr := s.analyzeMarket(marketCtx, resp.SessionID, m, depth)
			mu.Lock()
			defer mu.Unlock()
			if analyzeErr != nil {
				resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %v", m.PolymarketID, analyzeErr))
				return nil
			}
			collected = append(collected, indexedItem{Index: i, Item: *item})
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(collected, func(i, j int) bool { return collected[i].Index < collected[j].Index })
	for _, item := range collected {
		resp.Items = append(resp.Items, item.Item)
	}
	resp.Count = len(resp.Items)
	return resp, nil
}

func (s *Service) analyzeMarket(ctx context.Context, sessionID string, m model.Market, depth string) (*AnalyzeItem, error) {
	start := time.Now()
	toolCalls := make([]ToolCall, 0, 4)
	riskFactors := make([]string, 0)
	status := "ok"

	targetDate := m.EndDate.UTC().AddDate(0, 0, -1)
	targetDate = time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 0, 0, 0, 0, time.UTC)

	toolStart := time.Now()
	city, cityErr := s.signalSvc.ResolveMarketCity(ctx, m)
	toolCalls = append(toolCalls, newToolCall("resolve_city", map[string]any{
		"market_id": m.ID,
	}, toolStart, cityErr))
	if cityErr != nil {
		return nil, cityErr
	}
	if city == "" {
		status = "degraded"
		riskFactors = append(riskFactors, "market city unresolved")
	}

	toolStart = time.Now()
	priceYes, loadPriceErr := s.loadLatestMarketPrice(ctx, m.ID)
	toolCalls = append(toolCalls, newToolCall("get_market_prices", map[string]any{
		"market_id": m.ID,
	}, toolStart, loadPriceErr))
	if loadPriceErr != nil {
		return nil, loadPriceErr
	}

	toolStart = time.Now()
	forecastValues, forecastErr := s.signalSvc.LoadForecastSnapshots(ctx, city, targetDate, m.MarketType)
	toolCalls = append(toolCalls, newToolCall("get_weather_forecast", map[string]any{
		"location": city,
		"date":     targetDate.Format("2006-01-02"),
		"source":   "db",
	}, toolStart, forecastErr))

	if forecastErr != nil && city != "" && s.weather != nil {
		toolStart = time.Now()
		days := daysUntil(targetDate, s.cfg.MaxForecastDays)
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
			status = "degraded"
			riskFactors = append(riskFactors, "weather refresh failed: "+refreshErr.Error())
		}

		toolStart = time.Now()
		forecastValues, forecastErr = s.signalSvc.LoadForecastSnapshots(ctx, city, targetDate, m.MarketType)
		toolCalls = append(toolCalls, newToolCall("get_weather_forecast", map[string]any{
			"location": city,
			"date":     targetDate.Format("2006-01-02"),
			"source":   "db_after_refresh",
		}, toolStart, forecastErr))
	}

	forecastSummary := "no forecast snapshot available"
	if len(forecastValues) > 0 {
		forecastSummary = summarizeForecasts(forecastValues)
		oldestAge := oldestForecastAgeHours(time.Now().UTC(), forecastValues)
		if oldestAge > defaultStaleHoursLimit {
			status = "degraded"
			riskFactors = append(riskFactors, fmt.Sprintf("forecast data stale: oldest %.0fh", oldestAge))
		}
	}
	if forecastErr != nil {
		status = "degraded"
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
		usedModel      = ruleFallbackModelName
		promptTokens   = 0
		outputTokens   = 0
	)
	if signalErr == nil && generatedSignal != nil {
		ourProbability = generatedSignal.OurEstimate
		edgePct = generatedSignal.EdgePct
		confidence = generatedSignal.Confidence
		reasoning = generatedSignal.Reasoning
	} else {
		status = "degraded"
		latestSignal, latestErr := s.loadLatestSignalForMarket(ctx, m.ID)
		if latestErr == nil && latestSignal != nil {
			ourProbability = latestSignal.OurEstimate
			edgePct = latestSignal.EdgePct
			confidence = latestSignal.Confidence
			reasoning = latestSignal.Reasoning
			riskFactors = append(riskFactors, "using latest historical signal due to generation failure")
		} else {
			confidence = 5
			reasoning = "degraded analysis: signal generation failed and no prior signal available"
		}
		if signalErr != nil {
			riskFactors = append(riskFactors, "signal generation failed: "+signalErr.Error())
		}
	}

	if status == "degraded" && confidence > 25 {
		confidence = 25
	}

	recommendation := buildRecommendation(edgePct, priceYes)
	if s.vertexAI != nil {
		toolStart = time.Now()
		vertexResult, usage, vertexErr := s.vertexAI.Analyze(ctx, vertexPromptInput{
			Question:         m.Question,
			City:             city,
			ForecastSummary:  forecastSummary,
			MarketPriceYes:   priceYes,
			OurProbability:   ourProbability,
			EdgePct:          edgePct,
			Confidence:       confidence,
			Recommendation:   recommendation,
			RiskFactors:      riskFactors,
			DeterministicRsn: reasoning,
		})
		toolCalls = append(toolCalls, newToolCall("vertex_ai_generate", map[string]any{
			"project": s.cfg.VertexProject,
			"model":   s.vertexAI.ModelName(),
		}, toolStart, vertexErr))
		if vertexErr != nil {
			riskFactors = append(riskFactors, "vertex ai unavailable: "+vertexErr.Error())
		} else {
			usedModel = s.vertexAI.ModelName()
			promptTokens = usage.PromptTokens
			outputTokens = usage.CompletionTokens
			recommendation = vertexResult.Recommendation
			reasoning = vertexResult.Reasoning
			riskFactors = mergeUniqueStrings(riskFactors, vertexResult.RiskFactors)
		}
	}

	if depth == DepthFull && len(forecastValues) > 0 {
		reasoning = reasoning + "; forecasts: " + forecastSummary
	}

	item := &AnalyzeItem{
		MarketID:     m.ID,
		PolymarketID: m.PolymarketID,
		Question:     m.Question,
		ToolCalls:    toolCalls,
		Analysis: AnalysisBlock{
			Status:          status,
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

	if err := s.persistAgentLog(ctx, sessionID, m.ID, item, time.Since(start), usedModel, promptTokens, outputTokens); err != nil {
		s.log.Warn("persist agent log failed", zap.Error(err), zap.String("market_id", m.ID))
	}
	return item, nil
}

func (s *Service) selectMarkets(ctx context.Context, req AnalyzeRequest) ([]model.Market, error) {
	if strings.TrimSpace(req.MarketID) != "" && !req.All {
		m, err := s.signalSvc.LoadMarketByRef(ctx, req.MarketID)
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

func (s *Service) persistAgentLog(ctx context.Context, sessionID string, marketID string, item *AnalyzeItem, elapsed time.Duration, modelName string, promptTokens int, completionTokens int) error {
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
		SessionID:        sessionID,
		MarketID:         marketID,
		Action:           "analyze",
		Model:            modelName,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		ToolCalls:        datatypes.JSON(toolCallsJSON),
		Reasoning:        item.Analysis.Reasoning,
		Result:           datatypes.JSON(resultJSON),
		DurationMS:       int(elapsed.Milliseconds()),
		CreatedAt:        time.Now().UTC(),
	}
	return s.db.WithContext(ctx).Create(&row).Error
}

func ValidateDepth(raw string) (string, error) {
	depth := strings.ToLower(strings.TrimSpace(raw))
	if depth == "" {
		return DepthSummary, nil
	}
	if depth != DepthSummary && depth != DepthFull {
		return "", fmt.Errorf("invalid depth: %s (allowed: summary|full)", raw)
	}
	return depth, nil
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

func summarizeForecasts(items []signal.ForecastSnapshot) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s=%.1fF", item.Source, item.Value))
	}
	return strings.Join(parts, ", ")
}

func oldestForecastAgeHours(now time.Time, items []signal.ForecastSnapshot) float64 {
	if len(items) == 0 {
		return 0
	}
	oldest := items[0].FetchedAt
	for _, item := range items {
		if item.FetchedAt.Before(oldest) {
			oldest = item.FetchedAt
		}
	}
	return math.Max(0, now.Sub(oldest).Hours())
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

func daysUntil(targetDate time.Time, maxDays int) int {
	if maxDays <= 0 {
		maxDays = defaultForecastDaysCap
	}
	days := int(targetDate.Sub(time.Now().UTC()).Hours()/24) + 1
	if days < 1 {
		return 1
	}
	if days > maxDays {
		return maxDays
	}
	return days
}

func mergeUniqueStrings(base []string, values []string) []string {
	if len(values) == 0 {
		return base
	}
	seen := make(map[string]struct{}, len(base)+len(values))
	out := make([]string, 0, len(base)+len(values))
	for _, item := range base {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, item := range values {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func normalizeStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, item := range values {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}
