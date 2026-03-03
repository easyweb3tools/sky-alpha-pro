package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/datatypes"

	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/weather"
)

const (
	defaultCycleRunMode             = "observe"
	defaultCyclePromptVersion       = "v1.0.0"
	defaultCycleToolBudget          = 12
	defaultCycleExternalReqBudget   = 200
	defaultCycleMemoryWindow        = 3
	defaultCycleMarketLimit         = 300
	cycleStatusOK                   = "success"
	cycleStatusDegraded             = "degraded"
	cycleStatusError                = "error"
	cycleStatusSkipped              = "skipped_no_input"
	issueCodeToolUnavailable        = "tool_unavailable"
	issueCodeToolUnknown            = "tool_unknown"
	issueCodeCitiesEmpty            = "empty_city_set"
	issueCodePlanDecodeFailed       = "plan_decode_failed"
	issueCodePlanValidationFailed   = "plan_validation_failed"
	issueCodeAgentCycleNoExecutable = "agent_cycle_no_executable_step"
)

type cyclePromptInput struct {
	PromptVersion string                   `json:"prompt_version"`
	CycleID       string                   `json:"cycle_id"`
	RunMode       string                   `json:"run_mode"`
	TradeEnabled  bool                     `json:"trade_enabled"`
	Budgets       cyclePromptBudget        `json:"budgets"`
	Coverage      cyclePromptCoverage      `json:"coverage"`
	Freshness     cyclePromptFreshness     `json:"freshness"`
	Funnel        cyclePromptFunnel        `json:"funnel_last_cycle"`
	MemorySummary []cyclePromptMemoryBrief `json:"memory_summary"`
}

type cyclePromptBudget struct {
	MaxToolCalls        int `json:"max_tool_calls"`
	MaxExternalRequests int `json:"max_external_requests"`
}

type cyclePromptCoverage struct {
	ActiveMarkets      int `json:"active_markets"`
	MarketsSpecReady   int `json:"markets_spec_ready"`
	MarketsCityMissing int `json:"markets_city_missing"`
	ForecastRows24h    int `json:"forecast_rows_24h"`
}

type cyclePromptFreshness struct {
	MarketsLastSyncAt string `json:"markets_last_sync_at,omitempty"`
	MarketPricesLast  string `json:"market_prices_last_at,omitempty"`
	ForecastsLastAt   string `json:"forecasts_last_at,omitempty"`
	ChainLastAt       string `json:"chain_last_at,omitempty"`
}

type cyclePromptFunnel struct {
	MarketsTotal     int            `json:"markets_total"`
	SpecReady        int            `json:"spec_ready"`
	ForecastReady    int            `json:"forecast_ready"`
	RawEdgePass      int            `json:"raw_edge_pass"`
	ExecEdgePass     int            `json:"exec_edge_pass"`
	SignalsGenerated int            `json:"signals_generated"`
	Skipped          int            `json:"skipped"`
	TopSkipReasons   map[string]int `json:"top_skip_reasons"`
	UpdatedAt        string         `json:"updated_at,omitempty"`
}

type cyclePromptMemoryBrief struct {
	CycleID     string   `json:"cycle_id"`
	Outcome     string   `json:"outcome"`
	KeyFailures []string `json:"key_failures"`
}

type cyclePlanOutput struct {
	CycleGoal       string               `json:"cycle_goal"`
	Decision        string               `json:"decision"`
	Reasoning       string               `json:"reasoning"`
	Plan            []cyclePlanStep      `json:"plan"`
	RiskControls    cycleRiskControls    `json:"risk_controls"`
	ExpectedOutput  []string             `json:"expected_outputs"`
	MemoryWriteback cycleMemoryWriteback `json:"memory_writeback"`
}

type cyclePlanStep struct {
	Step            int            `json:"step"`
	Tool            string         `json:"tool"`
	Why             string         `json:"why"`
	Args            map[string]any `json:"args"`
	SuccessCriteria string         `json:"success_criteria"`
	OnFail          string         `json:"on_fail"`
}

type cycleRiskControls struct {
	MaxToolCalls        int  `json:"max_tool_calls"`
	MaxExternalRequests int  `json:"max_external_requests"`
	TradeEnabled        bool `json:"trade_enabled"`
}

type cycleMemoryWriteback struct {
	Focus      []string `json:"focus"`
	Hypothesis string   `json:"hypothesis"`
}

type cycleStepExecution struct {
	Status           string
	Records          []CycleRecord
	Errors           []CycleIssue
	Warnings         []CycleIssue
	Freshness        map[string]float64
	ExternalRequests int
	Meta             map[string]any
}

func (s *Service) RunCycle(ctx context.Context, opts CycleOptions) (*CycleResult, error) {
	started := time.Now().UTC()
	options := normalizeCycleOptions(opts)
	if options.CycleID == "" {
		options.CycleID = fmt.Sprintf("cycle-%s", started.Format("20060102T150405Z"))
	}

	input, err := s.buildCyclePromptInput(ctx, options)
	if err != nil {
		return nil, err
	}

	result := &CycleResult{
		SessionID: uuid.NewString(),
		CycleID:   options.CycleID,
		RunMode:   options.RunMode,
		Status:    cycleStatusOK,
		Records:   make([]CycleRecord, 0, 16),
		Errors:    make([]CycleIssue, 0, 8),
		Warnings:  make([]CycleIssue, 0, 8),
		Freshness: make(map[string]float64),
		StartedAt: started,
	}

	plan, modelName := s.buildPlan(ctx, input, result)
	if planErr := validatePlan(plan); planErr != nil {
		result.Warnings = append(result.Warnings, CycleIssue{
			Code:    issueCodePlanValidationFailed,
			Message: planErr.Error(),
			Source:  "agent_cycle",
			Count:   1,
		})
		plan = s.buildFallbackPlan(input)
	}
	result.Decision = strings.TrimSpace(plan.Decision)
	if result.Decision == "" {
		result.Decision = "run"
	}
	result.Model = modelName

	if len(plan.Plan) == 0 {
		result.Status = cycleStatusSkipped
		result.SkipReason = "plan has no executable steps"
		result.Errors = append(result.Errors, CycleIssue{
			Code:    issueCodeAgentCycleNoExecutable,
			Message: result.SkipReason,
			Source:  "agent_cycle",
			Count:   1,
		})
	}

	externalUsed := 0
	funnel := map[string]any{}
	for _, step := range sortPlanSteps(plan.Plan) {
		if result.ToolCalls >= options.MaxToolCalls {
			result.Warnings = append(result.Warnings, CycleIssue{
				Code:    "tool_budget_exhausted",
				Message: "max tool calls reached",
				Source:  "agent_cycle",
				Count:   1,
			})
			result.Status = cycleStatusDegraded
			break
		}
		if externalUsed >= options.MaxExternalRequests {
			result.Warnings = append(result.Warnings, CycleIssue{
				Code:    "external_budget_exhausted",
				Message: "max external request budget reached",
				Source:  "agent_cycle",
				Count:   1,
			})
			result.Status = cycleStatusDegraded
			break
		}
		attempts := 1
		if strings.EqualFold(step.OnFail, "retry") {
			attempts = 2
		}

		var exec cycleStepExecution
		var stepErr error
		stepStarted := time.Now().UTC()
		for attempt := 1; attempt <= attempts; attempt++ {
			exec, stepErr = s.executePlanStep(ctx, step, options)
			if stepErr == nil || attempt == attempts {
				break
			}
			result.Warnings = append(result.Warnings, CycleIssue{
				Code:    "step_retry",
				Message: fmt.Sprintf("step %d retry after error: %v", step.Step, stepErr),
				Source:  step.Tool,
				Count:   1,
			})
		}
		result.ToolCalls++
		externalUsed += exec.ExternalRequests
		result.Records = append(result.Records, exec.Records...)
		result.Errors = append(result.Errors, exec.Errors...)
		result.Warnings = append(result.Warnings, exec.Warnings...)
		mergeFreshness(result.Freshness, exec.Freshness)
		if sf, ok := exec.Meta["signal_funnel"]; ok {
			if m, ok := sf.(map[string]any); ok {
				funnel = m
			}
		}

		stepFinished := time.Now().UTC()
		s.persistCycleStep(ctx, result.SessionID, step, exec, stepErr, stepStarted, stepFinished)

		if stepErr != nil {
			if strings.EqualFold(step.OnFail, "abort") {
				result.Status = cycleStatusError
				result.Errors = append(result.Errors, CycleIssue{
					Code:    "step_aborted",
					Message: stepErr.Error(),
					Source:  step.Tool,
					Count:   1,
				})
				break
			}
			if result.Status == cycleStatusOK {
				result.Status = cycleStatusDegraded
			}
		}
	}

	if result.Decision == "skip" && result.Status == cycleStatusOK {
		result.Status = cycleStatusSkipped
	}
	if result.Status == cycleStatusOK && len(result.Errors) > 0 {
		result.Status = cycleStatusDegraded
	}
	if result.Status == cycleStatusSkipped && result.SkipReason == "" {
		result.SkipReason = "decision=skip"
	}

	result.FinishedAt = time.Now().UTC()
	result.DurationMS = result.FinishedAt.Sub(result.StartedAt).Milliseconds()

	s.persistCycleSession(ctx, result, input, plan, funnel)
	s.persistCycleMemory(ctx, result, plan, funnel)
	return result, nil
}

func normalizeCycleOptions(opts CycleOptions) CycleOptions {
	out := opts
	out.RunMode = strings.ToLower(strings.TrimSpace(out.RunMode))
	if out.RunMode == "" {
		out.RunMode = defaultCycleRunMode
	}
	if out.MaxToolCalls <= 0 {
		out.MaxToolCalls = defaultCycleToolBudget
	}
	if out.MaxExternalRequests <= 0 {
		out.MaxExternalRequests = defaultCycleExternalReqBudget
	}
	if out.MemoryWindow <= 0 {
		out.MemoryWindow = defaultCycleMemoryWindow
	}
	if out.MarketLimit <= 0 {
		out.MarketLimit = defaultCycleMarketLimit
	}
	return out
}

func (s *Service) buildCyclePromptInput(ctx context.Context, opts CycleOptions) (cyclePromptInput, error) {
	activeMarkets, err := s.countRows(ctx, "markets", "is_active = true")
	if err != nil {
		return cyclePromptInput{}, err
	}
	specReady, err := s.countRows(ctx, "markets", "is_active = true AND spec_status = 'ready'")
	if err != nil {
		return cyclePromptInput{}, err
	}
	cityMissing, err := s.countRows(ctx, "markets", "is_active = true AND COALESCE(city,'') = ''")
	if err != nil {
		return cyclePromptInput{}, err
	}
	var forecastRowsCount int64
	if err := s.db.WithContext(ctx).
		Table("forecasts").
		Where("fetched_at >= ?", time.Now().UTC().Add(-24*time.Hour)).
		Count(&forecastRowsCount).Error; err != nil {
		return cyclePromptInput{}, err
	}

	funnel := cyclePromptFunnel{TopSkipReasons: map[string]int{}}
	var run model.SignalRun
	if err := s.db.WithContext(ctx).Order("started_at DESC").Limit(1).Take(&run).Error; err == nil {
		funnel.MarketsTotal = run.MarketsTotal
		funnel.SpecReady = run.SpecReady
		funnel.ForecastReady = run.ForecastReady
		funnel.RawEdgePass = run.RawEdgePass
		funnel.ExecEdgePass = run.ExecEdgePass
		funnel.SignalsGenerated = run.SignalsGenerated
		funnel.Skipped = run.Skipped
		funnel.UpdatedAt = run.FinishedAt.UTC().Format(time.RFC3339)
		if len(run.SkipReasonsJSON) > 0 {
			_ = json.Unmarshal(run.SkipReasonsJSON, &funnel.TopSkipReasons)
		}
	}

	memories, err := s.loadRecentMemories(ctx, opts.MemoryWindow)
	if err != nil {
		return cyclePromptInput{}, err
	}

	input := cyclePromptInput{
		PromptVersion: defaultCyclePromptVersion,
		CycleID:       opts.CycleID,
		RunMode:       opts.RunMode,
		TradeEnabled:  opts.TradeEnabled,
		Budgets: cyclePromptBudget{
			MaxToolCalls:        opts.MaxToolCalls,
			MaxExternalRequests: opts.MaxExternalRequests,
		},
		Coverage: cyclePromptCoverage{
			ActiveMarkets:      activeMarkets,
			MarketsSpecReady:   specReady,
			MarketsCityMissing: cityMissing,
			ForecastRows24h:    int(forecastRowsCount),
		},
		Freshness: cyclePromptFreshness{
			MarketsLastSyncAt: s.maxTimeRFC3339(ctx, "markets", "updated_at", ""),
			MarketPricesLast:  s.maxTimeRFC3339(ctx, "market_prices", "captured_at", ""),
			ForecastsLastAt:   s.maxTimeRFC3339(ctx, "forecasts", "fetched_at", ""),
			ChainLastAt:       s.maxTimeRFC3339(ctx, "competitor_trades", "timestamp", ""),
		},
		Funnel:        funnel,
		MemorySummary: memories,
	}
	return input, nil
}

func (s *Service) buildPlan(ctx context.Context, input cyclePromptInput, result *CycleResult) (cyclePlanOutput, string) {
	modelName := ""
	if s.vertexAI == nil {
		return s.buildFallbackPlan(input), modelName
	}
	result.LLMCalls++
	plan, err := s.vertexAI.PlanCycle(ctx, input)
	if err != nil {
		result.Warnings = append(result.Warnings, CycleIssue{
			Code:    issueCodePlanDecodeFailed,
			Message: err.Error(),
			Source:  "vertex_ai",
			Count:   1,
		})
		return s.buildFallbackPlan(input), modelName
	}
	modelName = s.vertexAI.ModelName()
	return *plan, modelName
}

func (s *Service) buildFallbackPlan(input cyclePromptInput) cyclePlanOutput {
	steps := make([]cyclePlanStep, 0, 6)
	stepNo := 1

	if s.market != nil && input.Coverage.ActiveMarkets == 0 {
		steps = append(steps, cyclePlanStep{
			Step:            stepNo,
			Tool:            "market.sync.batch",
			Why:             "refresh active markets before downstream tasks",
			Args:            map[string]any{"limit": defaultCycleMarketLimit},
			SuccessCriteria: "markets_total > 0",
			OnFail:          "abort",
		})
		stepNo++
	}
	if s.signalSvc != nil && input.Coverage.MarketsCityMissing > 0 {
		steps = append(steps, cyclePlanStep{
			Step:            stepNo,
			Tool:            "market.city.resolve.batch",
			Why:             "resolve missing city/spec metadata for active markets",
			Args:            map[string]any{"only_missing": true, "limit": defaultCycleMarketLimit},
			SuccessCriteria: "spec_ready increases",
			OnFail:          "continue",
		})
		stepNo++
	}
	if s.signalSvc != nil {
		steps = append(steps, cyclePlanStep{
			Step:            stepNo,
			Tool:            "market.cities.active",
			Why:             "load active cities for weather sync",
			Args:            map[string]any{},
			SuccessCriteria: "cities_count > 0",
			OnFail:          "abort",
		})
		stepNo++
	}
	if s.weather != nil {
		steps = append(steps, cyclePlanStep{
			Step:            stepNo,
			Tool:            "weather.forecast.batch",
			Why:             "sync latest forecasts for active cities",
			Args:            map[string]any{"days": 7, "source": "all"},
			SuccessCriteria: "forecast_rows_24h increases",
			OnFail:          "continue",
		})
		stepNo++
	}
	if s.signalSvc != nil {
		steps = append(steps, cyclePlanStep{
			Step:            stepNo,
			Tool:            "signal.generate.batch",
			Why:             "generate executable signals after coverage updates",
			Args:            map[string]any{"limit": defaultCycleMarketLimit},
			SuccessCriteria: "signals_generated >= 1 or explicit skip reasons",
			OnFail:          "continue",
		})
		stepNo++
	}
	steps = append(steps, cyclePlanStep{
		Step:            stepNo,
		Tool:            "report.generate",
		Why:             "persist cycle summary for ops and memory",
		Args:            map[string]any{"include_funnel": true},
		SuccessCriteria: "cycle summary persisted",
		OnFail:          "continue",
	})

	decision := "run"
	if len(steps) == 1 && steps[0].Tool == "report.generate" {
		decision = "skip"
	}
	return cyclePlanOutput{
		CycleGoal: "stabilize data coverage and produce trading signals",
		Decision:  decision,
		Reasoning: "fallback deterministic plan used",
		Plan:      steps,
		RiskControls: cycleRiskControls{
			MaxToolCalls:        input.Budgets.MaxToolCalls,
			MaxExternalRequests: input.Budgets.MaxExternalRequests,
			TradeEnabled:        input.TradeEnabled,
		},
		ExpectedOutput: []string{"scheduler_runs", "signal_runs", "signals"},
		MemoryWriteback: cycleMemoryWriteback{
			Focus:      []string{"markets_spec_ready", "forecast_ready", "signals_generated"},
			Hypothesis: "city/spec and forecast coverage increase should improve signal output",
		},
	}
}

func validatePlan(plan cyclePlanOutput) error {
	if len(plan.Plan) == 0 {
		return nil
	}
	for _, step := range plan.Plan {
		if strings.TrimSpace(step.Tool) == "" {
			return fmt.Errorf("plan step missing tool")
		}
		if step.Step <= 0 {
			return fmt.Errorf("plan step must be >= 1")
		}
	}
	return nil
}

func sortPlanSteps(steps []cyclePlanStep) []cyclePlanStep {
	out := append([]cyclePlanStep(nil), steps...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Step == out[j].Step {
			return out[i].Tool < out[j].Tool
		}
		return out[i].Step < out[j].Step
	})
	return out
}

func (s *Service) executePlanStep(ctx context.Context, step cyclePlanStep, opts CycleOptions) (cycleStepExecution, error) {
	tool := strings.TrimSpace(strings.ToLower(step.Tool))
	switch tool {
	case "market.sync.batch":
		return s.runToolMarketSync(ctx), nil
	case "market.city.resolve.batch":
		return s.runToolMarketCityResolve(ctx, step.Args), nil
	case "market.cities.active":
		return s.runToolMarketCitiesActive(ctx), nil
	case "weather.forecast.batch":
		return s.runToolWeatherForecast(ctx, step.Args), nil
	case "signal.generate.batch":
		return s.runToolSignalGenerate(ctx, step.Args, opts), nil
	case "chain.scan.batch":
		return s.runToolChainScan(ctx, step.Args), nil
	case "report.generate":
		return cycleStepExecution{
			Status: "success",
			Records: []CycleRecord{
				{Entity: "agent_report", Result: "success", Count: 1},
			},
		}, nil
	default:
		exec := cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{
				{
					Code:    issueCodeToolUnknown,
					Message: "unsupported tool: " + step.Tool,
					Source:  step.Tool,
					Count:   1,
				},
			},
		}
		return exec, fmt.Errorf("unsupported tool: %s", step.Tool)
	}
}

func (s *Service) runToolMarketSync(ctx context.Context) cycleStepExecution {
	if s.market == nil {
		return unavailableToolExec("market.sync.batch")
	}
	res, err := s.market.SyncMarkets(ctx)
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "market_sync_failed", Message: err.Error(), Source: "market.sync.batch", Count: 1}},
		}
	}
	exec := cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "markets", Result: "success", Count: res.MarketsUpserted},
			{Entity: "prices", Result: "success", Count: res.PriceSnapshots},
		},
		Freshness: map[string]float64{"markets": 0},
	}
	if len(res.Errors) > 0 {
		exec.Records = append(exec.Records, CycleRecord{Entity: "market_sync_errors", Result: "error", Count: len(res.Errors)})
		exec.Warnings = append(exec.Warnings, CycleIssue{
			Code:    "market_sync_partial",
			Message: fmt.Sprintf("market sync finished with %d warnings", len(res.Errors)),
			Source:  "market.sync.batch",
			Count:   len(res.Errors),
		})
	}
	exec.ExternalRequests = res.MarketsFetched
	return exec
}

func (s *Service) runToolMarketCityResolve(ctx context.Context, args map[string]any) cycleStepExecution {
	if s.signalSvc == nil {
		return unavailableToolExec("market.city.resolve.batch")
	}
	onlyMissing := getBoolArg(args, "only_missing", true)
	limit := getIntArg(args, "limit", defaultCycleMarketLimit)
	res, err := s.signalSvc.ResolveMarketCities(ctx, signal.ResolveCitiesOptions{
		Limit:       limit,
		OnlyMissing: onlyMissing,
	})
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "city_resolve_failed", Message: err.Error(), Source: "market.city.resolve.batch", Count: 1}},
		}
	}
	exec := cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "markets_spec_processed", Result: "success", Count: res.Processed},
			{Entity: "markets_spec_ready", Result: "success", Count: res.SpecReady},
			{Entity: "markets_city_resolved", Result: "success", Count: res.Resolved},
			{Entity: "markets_city_unresolved", Result: "skipped", Count: res.Unresolved},
		},
		Meta: map[string]any{
			"sources":      res.Sources,
			"skip_reasons": res.SkipReasons,
		},
	}
	if len(res.Errors) > 0 {
		exec.Warnings = append(exec.Warnings, CycleIssue{
			Code:    "city_resolve_partial",
			Message: fmt.Sprintf("city resolve produced %d errors", len(res.Errors)),
			Source:  "market.city.resolve.batch",
			Count:   len(res.Errors),
		})
	}
	return exec
}

func (s *Service) runToolMarketCitiesActive(ctx context.Context) cycleStepExecution {
	if s.signalSvc == nil {
		return unavailableToolExec("market.cities.active")
	}
	cities, err := s.signalSvc.ListActiveCities(ctx)
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "cities_active_failed", Message: err.Error(), Source: "market.cities.active", Count: 1}},
		}
	}
	if len(cities) == 0 {
		return cycleStepExecution{
			Status: "skipped",
			Records: []CycleRecord{
				{Entity: "cities", Result: "skipped", Count: 1},
			},
			Errors: []CycleIssue{{Code: issueCodeCitiesEmpty, Message: "no active cities", Source: "market.cities.active", Count: 1}},
			Meta:   map[string]any{"cities": []string{}},
		}
	}
	return cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "cities", Result: "success", Count: len(cities)},
		},
		Meta: map[string]any{"cities": cities},
	}
}

func (s *Service) runToolWeatherForecast(ctx context.Context, args map[string]any) cycleStepExecution {
	if s.weather == nil || s.signalSvc == nil {
		return unavailableToolExec("weather.forecast.batch")
	}
	cities, err := s.signalSvc.ListActiveCities(ctx)
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "cities_active_failed", Message: err.Error(), Source: "weather.forecast.batch", Count: 1}},
		}
	}
	if len(cities) == 0 {
		return cycleStepExecution{
			Status: "skipped",
			Records: []CycleRecord{
				{Entity: "cities", Result: "skipped", Count: 1},
			},
			Errors: []CycleIssue{{Code: issueCodeCitiesEmpty, Message: "weather forecast skipped: no active cities", Source: "weather.forecast.batch", Count: 1}},
		}
	}

	days := getIntArg(args, "days", 7)
	source := getStringArg(args, "source", "all")
	concurrency := 4
	var (
		mu            sync.Mutex
		successCity   int
		failedCity    int
		forecastRows  int
		providerError int
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, city := range cities {
		city := city
		g.Go(func() error {
			resp, callErr := s.weather.GetForecast(gctx, weather.ForecastRequest{
				Location: city,
				Source:   source,
				Days:     days,
			})
			mu.Lock()
			defer mu.Unlock()
			if callErr != nil {
				failedCity++
				return nil
			}
			successCity++
			forecastRows += len(resp.Forecasts)
			providerError += len(resp.Errors)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "weather_forecast_failed", Message: err.Error(), Source: "weather.forecast.batch", Count: 1}},
		}
	}
	exec := cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "cities", Result: "success", Count: successCity},
			{Entity: "cities", Result: "error", Count: failedCity},
			{Entity: "forecasts", Result: "success", Count: forecastRows},
			{Entity: "provider_errors", Result: "error", Count: providerError},
		},
		Freshness:        map[string]float64{"forecasts": 0},
		ExternalRequests: len(cities),
	}
	if successCity == 0 && failedCity > 0 {
		exec.Status = "error"
		exec.Errors = append(exec.Errors, CycleIssue{
			Code:    "weather_forecast_all_failed",
			Message: fmt.Sprintf("weather forecast failed for all %d cities", failedCity),
			Source:  "weather.forecast.batch",
			Count:   failedCity,
		})
	}
	return exec
}

func (s *Service) runToolSignalGenerate(ctx context.Context, args map[string]any, opts CycleOptions) cycleStepExecution {
	if s.signalSvc == nil {
		return unavailableToolExec("signal.generate.batch")
	}
	limit := getIntArg(args, "limit", opts.MarketLimit)
	res, err := s.signalSvc.GenerateSignals(ctx, signal.GenerateOptions{Limit: limit})
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "signal_generate_failed", Message: err.Error(), Source: "signal.generate.batch", Count: 1}},
		}
	}
	exec := cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "markets_total", Result: "success", Count: res.MarketsTotal},
			{Entity: "spec_ready", Result: "success", Count: res.SpecReady},
			{Entity: "forecast_ready", Result: "success", Count: res.ForecastReady},
			{Entity: "raw_edge_pass", Result: "success", Count: res.RawEdgePass},
			{Entity: "exec_edge_pass", Result: "success", Count: res.ExecEdgePass},
			{Entity: "signals_generated", Result: "success", Count: res.Generated},
			{Entity: "signals_skipped", Result: "skipped", Count: res.Skipped},
		},
		Freshness: map[string]float64{"signals": 0},
		Meta: map[string]any{
			"signal_funnel": map[string]any{
				"markets_total":     res.MarketsTotal,
				"spec_ready":        res.SpecReady,
				"forecast_ready":    res.ForecastReady,
				"raw_edge_pass":     res.RawEdgePass,
				"exec_edge_pass":    res.ExecEdgePass,
				"signals_generated": res.Generated,
				"skipped":           res.Skipped,
				"skip_reasons":      res.SkipReasons,
			},
		},
	}
	if len(res.Errors) > 0 {
		exec.Warnings = append(exec.Warnings, CycleIssue{
			Code:    "signal_generate_partial",
			Message: fmt.Sprintf("signal generate finished with %d market errors", len(res.Errors)),
			Source:  "signal.generate.batch",
			Count:   len(res.Errors),
		})
	}
	return exec
}

func (s *Service) runToolChainScan(ctx context.Context, args map[string]any) cycleStepExecution {
	if s.chainSvc == nil {
		return unavailableToolExec("chain.scan.batch")
	}
	lookback := uint64(getIntArg(args, "lookback_blocks", 0))
	maxTx := getIntArg(args, "max_tx", 0)
	res, err := s.chainSvc.Scan(ctx, chain.ScanOptions{
		LookbackBlocks: lookback,
		MaxTx:          maxTx,
	})
	if err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{Code: "chain_scan_failed", Message: err.Error(), Source: "chain.scan.batch", Count: 1}},
		}
	}
	return cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "observed_logs", Result: "success", Count: res.ObservedLogs},
			{Entity: "observed_tx", Result: "success", Count: res.ObservedTx},
			{Entity: "trades_inserted", Result: "success", Count: res.InsertedTrades},
			{Entity: "trades_duplicate", Result: "skipped", Count: res.DuplicateTrades},
		},
		Freshness: map[string]float64{"chain": 0},
	}
}

func unavailableToolExec(tool string) cycleStepExecution {
	return cycleStepExecution{
		Status: "error",
		Errors: []CycleIssue{
			{
				Code:    issueCodeToolUnavailable,
				Message: "tool service unavailable",
				Source:  tool,
				Count:   1,
			},
		},
	}
}

func (s *Service) persistCycleSession(ctx context.Context, result *CycleResult, input cyclePromptInput, plan cyclePlanOutput, funnel map[string]any) {
	if s.db == nil || result == nil {
		return
	}
	recordsSuccess, recordsError, recordsSkipped := summarizeCycleRecords(result.Records)
	inputJSON, _ := json.Marshal(input)
	planJSON, _ := json.Marshal(plan)
	summaryJSON, _ := json.Marshal(map[string]any{
		"funnel":   funnel,
		"errors":   result.Errors,
		"warnings": result.Warnings,
	})
	row := model.AgentSession{
		ID:               result.SessionID,
		CycleID:          result.CycleID,
		PromptVersion:    defaultCyclePromptVersion,
		RunMode:          result.RunMode,
		Model:            result.Model,
		Status:           result.Status,
		Decision:         result.Decision,
		LLMCalls:         result.LLMCalls,
		ToolCalls:        result.ToolCalls,
		RecordsSuccess:   recordsSuccess,
		RecordsError:     recordsError,
		RecordsSkipped:   recordsSkipped,
		ErrorCode:        firstIssueCode(result.Errors),
		ErrorMessage:     firstIssueMessage(result.Errors),
		InputContextJSON: datatypes.JSON(inputJSON),
		OutputPlanJSON:   datatypes.JSON(planJSON),
		SummaryJSON:      datatypes.JSON(summaryJSON),
		StartedAt:        result.StartedAt,
		FinishedAt:       result.FinishedAt,
		DurationMS:       int(result.DurationMS),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil && s.log != nil {
		s.log.Warn("persist agent session failed", zap.Error(err), zap.String("session_id", result.SessionID))
	}
}

func (s *Service) persistCycleStep(ctx context.Context, sessionID string, step cyclePlanStep, exec cycleStepExecution, stepErr error, startedAt, finishedAt time.Time) {
	if s.db == nil || sessionID == "" {
		return
	}
	argsJSON, _ := json.Marshal(step.Args)
	resultJSON, _ := json.Marshal(map[string]any{
		"records":  exec.Records,
		"errors":   exec.Errors,
		"warnings": exec.Warnings,
		"meta":     exec.Meta,
	})
	status := exec.Status
	if status == "" {
		if stepErr != nil {
			status = "error"
		} else {
			status = "success"
		}
	}
	row := model.AgentStep{
		SessionID:   sessionID,
		StepNo:      step.Step,
		Tool:        step.Tool,
		Status:      status,
		OnFail:      step.OnFail,
		ErrorCode:   firstIssueCode(exec.Errors),
		ErrorDetail: firstIssueMessage(exec.Errors),
		ArgsJSON:    datatypes.JSON(argsJSON),
		ResultJSON:  datatypes.JSON(resultJSON),
		StartedAt:   startedAt,
		FinishedAt:  finishedAt,
		DurationMS:  int(finishedAt.Sub(startedAt).Milliseconds()),
		CreatedAt:   finishedAt,
	}
	if stepErr != nil {
		row.ErrorDetail = stepErr.Error()
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil && s.log != nil {
		s.log.Warn("persist agent step failed", zap.Error(err), zap.String("session_id", sessionID), zap.String("tool", step.Tool))
	}
}

func (s *Service) persistCycleMemory(ctx context.Context, result *CycleResult, plan cyclePlanOutput, funnel map[string]any) {
	if s.db == nil || result == nil {
		return
	}
	keyFailures := make([]string, 0, len(result.Errors))
	for _, issue := range result.Errors {
		code := strings.TrimSpace(issue.Code)
		if code == "" {
			continue
		}
		keyFailures = append(keyFailures, code)
	}
	keyFailures = uniqueTopN(keyFailures, 5)

	keyFailuresJSON, _ := json.Marshal(keyFailures)
	funnelJSON, _ := json.Marshal(funnel)
	actionsJSON, _ := json.Marshal(plan.MemoryWriteback.Focus)
	execJSON, _ := json.Marshal(map[string]any{
		"status":      result.Status,
		"decision":    result.Decision,
		"tool_calls":  result.ToolCalls,
		"llm_calls":   result.LLMCalls,
		"records":     result.Records,
		"errors":      result.Errors,
		"warnings":    result.Warnings,
		"duration_ms": result.DurationMS,
		"skip_reason": result.SkipReason,
		"hypothesis":  plan.MemoryWriteback.Hypothesis,
	})
	row := model.AgentMemory{
		SessionID:      result.SessionID,
		CycleID:        result.CycleID,
		Outcome:        result.Status,
		KeyFailures:    datatypes.JSON(keyFailuresJSON),
		FunnelSummary:  datatypes.JSON(funnelJSON),
		ActionSuggests: datatypes.JSON(actionsJSON),
		ExecutionJSON:  datatypes.JSON(execJSON),
		CreatedAt:      time.Now().UTC(),
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil && s.log != nil {
		s.log.Warn("persist agent memory failed", zap.Error(err), zap.String("session_id", result.SessionID))
	}
}

func (s *Service) loadRecentMemories(ctx context.Context, window int) ([]cyclePromptMemoryBrief, error) {
	if s.db == nil || window <= 0 {
		return nil, nil
	}
	var rows []model.AgentMemory
	if err := s.db.WithContext(ctx).
		Order("created_at DESC").
		Limit(window).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]cyclePromptMemoryBrief, 0, len(rows))
	for _, row := range rows {
		item := cyclePromptMemoryBrief{
			CycleID: row.CycleID,
			Outcome: row.Outcome,
		}
		_ = json.Unmarshal(row.KeyFailures, &item.KeyFailures)
		out = append(out, item)
	}
	return out, nil
}

func (s *Service) countRows(ctx context.Context, table string, where string) (int, error) {
	var count int64
	query := s.db.WithContext(ctx).Table(table)
	if strings.TrimSpace(where) != "" {
		query = query.Where(where)
	}
	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

func (s *Service) maxTimeRFC3339(ctx context.Context, table string, column string, where string) string {
	if s.db == nil {
		return ""
	}
	var v sql.NullTime
	query := s.db.WithContext(ctx).Table(table).Select("MAX(" + column + ")")
	if strings.TrimSpace(where) != "" {
		query = query.Where(where)
	}
	if err := query.Scan(&v).Error; err != nil || !v.Valid {
		return ""
	}
	return v.Time.UTC().Format(time.RFC3339)
}

func summarizeCycleRecords(records []CycleRecord) (success, failed, skipped int) {
	for _, rec := range records {
		n := rec.Count
		if n < 0 {
			n = 0
		}
		switch strings.ToLower(strings.TrimSpace(rec.Result)) {
		case "error":
			failed += n
		case "skipped":
			skipped += n
		default:
			success += n
		}
	}
	return success, failed, skipped
}

func mergeFreshness(dst map[string]float64, src map[string]float64) {
	if dst == nil || len(src) == 0 {
		return
	}
	for k, v := range src {
		dst[k] = v
	}
}

func firstIssueCode(issues []CycleIssue) string {
	for _, issue := range issues {
		code := strings.TrimSpace(issue.Code)
		if code != "" {
			return code
		}
	}
	return ""
}

func firstIssueMessage(issues []CycleIssue) string {
	for _, issue := range issues {
		msg := strings.TrimSpace(issue.Message)
		if msg != "" {
			return msg
		}
	}
	return ""
}

func uniqueTopN(items []string, n int) []string {
	if len(items) == 0 || n <= 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, n)
	for _, item := range items {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
		if len(out) >= n {
			break
		}
	}
	return out
}

func getIntArg(args map[string]any, key string, fallback int) int {
	if args == nil {
		return fallback
	}
	raw, ok := args[key]
	if !ok {
		return fallback
	}
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	}
	return fallback
}

func getBoolArg(args map[string]any, key string, fallback bool) bool {
	if args == nil {
		return fallback
	}
	raw, ok := args[key]
	if !ok {
		return fallback
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	}
	return fallback
}

func getStringArg(args map[string]any, key string, fallback string) string {
	if args == nil {
		return fallback
	}
	raw, ok := args[key]
	if !ok {
		return fallback
	}
	v, ok := raw.(string)
	if !ok {
		return fallback
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}
