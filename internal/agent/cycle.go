package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/datatypes"
	"gorm.io/gorm"

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
	defaultCycleTokenBudget         = 12000
	defaultCycleDurationBudget      = 90 * time.Second
	defaultCycleMemoryWindow        = 3
	defaultCycleMarketLimit         = 300
	defaultCycleAdvisoryLockKey     = int64(739001)
	cyclePersistWriteTimeout        = 3 * time.Second
	cycleStatusOK                   = "success"
	cycleStatusDegraded             = "degraded"
	cycleStatusError                = "error"
	cycleStatusSkipped              = "skipped_no_input"
	cycleSkipLocked                 = "agent_cycle_locked"
	issueCodeToolUnavailable        = "tool_unavailable"
	issueCodeToolUnknown            = "tool_unknown"
	issueCodeCitiesEmpty            = "empty_city_set"
	issueCodePlanDecodeFailed       = "plan_decode_failed"
	issueCodePlanValidationFailed   = "plan_validation_failed"
	issueCodeAgentCycleNoExecutable = "agent_cycle_no_executable_step"
	issueCodeTokenBudgetExhausted   = "token_budget_exhausted"
	issueCodeCycleDurationExceeded  = "cycle_duration_exhausted"
	issueCodeUpstream429            = "upstream_429"
	strategyScopePromptRollout      = "prompt_rollout"
	strategyScopeAgentParamTuning   = "agent_param_tuning"
	strategySubjectAgentCycle       = "agent_cycle"
	strategyParamMaxToolCalls       = "max_tool_calls"
	strategyParamMaxExternalReqs    = "max_external_requests"
	strategyParamMarketLimit        = "market_limit"
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
	MaxTokensPerCycle   int `json:"max_tokens_per_cycle"`
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
	MaxTokensPerCycle   int  `json:"max_tokens_per_cycle"`
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

type cyclePromptBundle struct {
	Version string
	System  string
	Variant string
}

func isVertexBrainMode(runMode string) bool {
	mode := strings.ToLower(strings.TrimSpace(runMode))
	return mode == "vertex_brain" || mode == "vertex-controlled" || mode == "vertex_controlled" || mode == "event_driven"
}

func (s *Service) RunCycle(ctx context.Context, opts CycleOptions) (*CycleResult, error) {
	started := time.Now().UTC()
	options := normalizeCycleOptions(opts)
	options = s.applyAgentCycleParamOverrides(ctx, options)
	if options.CycleID == "" {
		options.CycleID = fmt.Sprintf("cycle-%s", started.Format("20060102T150405Z"))
	}
	unlock, locked, lockErr := s.acquireCycleLock(ctx)
	if lockErr != nil {
		return nil, lockErr
	}
	if !locked {
		finished := time.Now().UTC()
		res := &CycleResult{
			SessionID:  uuid.NewString(),
			CycleID:    options.CycleID,
			RunMode:    options.RunMode,
			Decision:   "skip",
			Status:     cycleStatusSkipped,
			SkipReason: cycleSkipLocked,
			Records:    []CycleRecord{{Entity: "agent_cycle", Result: "skipped", Count: 1}},
			Errors: []CycleIssue{{
				Code:    cycleSkipLocked,
				Message: "agent cycle lock is held by another runner",
				Source:  "agent_cycle",
				Count:   1,
			}},
			StartedAt:  started,
			FinishedAt: finished,
			DurationMS: finished.Sub(started).Milliseconds(),
		}
		s.observeCycleMetrics(res, "")
		return res, nil
	}
	defer unlock()

	promptBundle, err := s.loadPromptBundle(ctx, options.CycleID)
	if err != nil {
		if s.log != nil {
			s.log.Warn("load prompt bundle failed; fallback to built-in prompt", zap.Error(err))
		}
		promptBundle = cyclePromptBundle{Version: defaultCyclePromptVersion, System: cycleSystemPrompt, Variant: "builtin"}
	}

	input, err := s.buildCyclePromptInput(ctx, options)
	if err != nil {
		return nil, err
	}
	input.PromptVersion = promptBundle.Version
	if s.metrics != nil && len(input.MemorySummary) > 0 {
		s.metrics.AddAgentMemoryHits(len(input.MemorySummary))
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
		BrainTrace: []CycleStateEvent{
			{State: "cycle_started", At: started, Message: "vertex brain cycle started"},
		},
		StartedAt: started,
	}
	deadline := started.Add(time.Duration(options.MaxCycleDurationSec) * time.Second)

	plan := cyclePlanOutput{}
	fallbackReason := ""
	if isVertexBrainMode(options.RunMode) && s.vertexAI != nil {
		result.Decision = "run"
		result.Model = s.vertexAI.ModelName()
		conversation := buildVertexConversationInit(input, options.MaxToolCalls)
		funnel := map[string]any{}
		var reportID uint64
		executedSteps := make([]cyclePlanStep, 0, options.MaxToolCalls+2)
		externalUsed := 0
		stepNo := 1
		malformedRetryUsed := false
		result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
			State:   "vertex_session_started",
			At:      time.Now().UTC(),
			Message: "vertex-controlled loop entered",
		})
		for stepNo <= options.MaxToolCalls {
			if time.Now().UTC().After(deadline) {
				result.Warnings = append(result.Warnings, CycleIssue{
					Code:    issueCodeCycleDurationExceeded,
					Message: "max cycle duration budget reached",
					Source:  "agent_cycle",
					Count:   1,
				})
				result.Status = cycleStatusDegraded
				result.SkipReason = "max cycle duration reached"
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "cycle_stopped",
					At:        time.Now().UTC(),
					Message:   "max cycle duration budget reached",
					ErrorCode: issueCodeCycleDurationExceeded,
				})
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
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "cycle_stopped",
					At:        time.Now().UTC(),
					Message:   "max external request budget reached",
					ErrorCode: "external_budget_exhausted",
				})
				break
			}
			if result.LLMTokens >= options.MaxTokensPerCycle {
				result.Warnings = append(result.Warnings, CycleIssue{
					Code:    issueCodeTokenBudgetExhausted,
					Message: "max llm token budget reached",
					Source:  "agent_cycle",
					Count:   1,
				})
				result.Status = cycleStatusDegraded
				result.SkipReason = "max llm token budget reached"
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "cycle_stopped",
					At:        time.Now().UTC(),
					Message:   "max llm token budget reached",
					ErrorCode: issueCodeTokenBudgetExhausted,
				})
				break
			}
			result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
				State:   "vertex_next_action",
				At:      time.Now().UTC(),
				StepNo:  stepNo,
				Message: "requesting next action from vertex",
			})
			result.LLMCalls++
			action, tokensUsed, actionErr := s.vertexAI.NextCycleAction(ctx, stepNo, options.MaxToolCalls, conversation)
			result.LLMTokens += tokensUsed
			if actionErr != nil {
				code := classifyVertexActionErr(actionErr)
				result.Warnings = append(result.Warnings, CycleIssue{
					Code:    code,
					Message: actionErr.Error(),
					Source:  "vertex_ai",
					Count:   1,
				})
				result.Status = cycleStatusDegraded
				fallbackReason = "vertex_next_action_failed"
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "vertex_next_action_failed",
					At:        time.Now().UTC(),
					StepNo:    stepNo,
					Message:   actionErr.Error(),
					ErrorCode: code,
				})
				break
			}
			if result.LLMTokens > options.MaxTokensPerCycle {
				result.Warnings = append(result.Warnings, CycleIssue{
					Code:    issueCodeTokenBudgetExhausted,
					Message: "llm token budget exceeded after vertex action",
					Source:  "vertex_ai",
					Count:   1,
				})
				result.Status = cycleStatusDegraded
				result.SkipReason = "max llm token budget reached"
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "cycle_stopped",
					At:        time.Now().UTC(),
					StepNo:    stepNo,
					Message:   "llm token budget exceeded after vertex action",
					ErrorCode: issueCodeTokenBudgetExhausted,
				})
				break
			}
			if action.Decision == "finish" {
				if isMalformedFunctionCallSummary(action.Summary) && !malformedRetryUsed {
					malformedRetryUsed = true
					result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
						State:   "vertex_malformed_retry",
						At:      time.Now().UTC(),
						StepNo:  stepNo,
						Message: "malformed function call detected; retrying once with stricter instruction",
					})
					conversation = append(conversation, map[string]any{
						"role": "user",
						"parts": []map[string]any{
							{
								"text": "Your previous function call was malformed. Do NOT output code, print(), or prose. Call exactly one allowed function now: call_tool(...) OR finish_cycle(...).",
							},
						},
					})
					continue
				}
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:   "vertex_finish",
					At:      time.Now().UTC(),
					StepNo:  stepNo,
					Message: strings.TrimSpace(action.Summary),
				})
				break
			}
			if action.Decision != "call_tool" || strings.TrimSpace(action.Tool) == "" {
				result.Warnings = append(result.Warnings, CycleIssue{
					Code:    issueCodePlanValidationFailed,
					Message: "vertex action invalid decision/tool",
					Source:  "vertex_ai",
					Count:   1,
				})
				result.Status = cycleStatusDegraded
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "vertex_next_action_invalid",
					At:        time.Now().UTC(),
					StepNo:    stepNo,
					Message:   "vertex action invalid decision/tool",
					ErrorCode: issueCodePlanValidationFailed,
				})
				break
			}

			step := cyclePlanStep{
				Step:   stepNo,
				Tool:   strings.TrimSpace(action.Tool),
				Why:    strings.TrimSpace(action.Why),
				Args:   action.Args,
				OnFail: action.OnFail,
			}
			if step.OnFail == "" {
				step.OnFail = "continue"
			}
			if step.Args == nil {
				step.Args = map[string]any{}
			}
			executedSteps = append(executedSteps, step)
			conversation = append(conversation, map[string]any{
				"role": "model",
				"parts": []map[string]any{
					{
						"functionCall": map[string]any{
							"name": "call_tool",
							"args": map[string]any{
								"tool":    step.Tool,
								"args":    step.Args,
								"why":     step.Why,
								"on_fail": step.OnFail,
								"summary": strings.TrimSpace(action.Summary),
							},
						},
					},
				},
			})
			result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
				State:   "tool_call_started",
				At:      time.Now().UTC(),
				StepNo:  stepNo,
				Tool:    step.Tool,
				Message: strings.TrimSpace(step.Why),
			})

			exec, stepErr := s.executePlanStep(ctx, step, options, result, funnel)
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
			if rid, ok := exec.Meta["report_id"]; ok {
				switch v := rid.(type) {
				case uint64:
					reportID = v
				case int:
					if v > 0 {
						reportID = uint64(v)
					}
				case float64:
					if v > 0 {
						reportID = uint64(v)
					}
				}
			}
			now := time.Now().UTC()
			s.persistCycleStep(ctx, result.SessionID, step, exec, stepErr, now, now)
			traceMsg := "tool call finished"
			errCode := ""
			if stepErr != nil {
				traceMsg = stepErr.Error()
				errCode = firstIssueCode(exec.Errors)
			}
			result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
				State:     "tool_call_finished",
				At:        now,
				StepNo:    stepNo,
				Tool:      step.Tool,
				Message:   traceMsg,
				ErrorCode: errCode,
			})

			conversation = append(conversation, map[string]any{
				"role": "user",
				"parts": []map[string]any{
					{
						"functionResponse": map[string]any{
							"name": "call_tool",
							"response": map[string]any{
								"step":        stepNo,
								"tool":        step.Tool,
								"status":      exec.Status,
								"on_fail":     step.OnFail,
								"records":     exec.Records,
								"errors":      exec.Errors,
								"warnings":    exec.Warnings,
								"external":    exec.ExternalRequests,
								"step_error":  errorString(stepErr),
								"signal_funnel": func() map[string]any {
									if sf, ok := exec.Meta["signal_funnel"].(map[string]any); ok {
										return sf
									}
									return nil
								}(),
							},
						},
					},
				},
			})
			if stepErr != nil && strings.EqualFold(step.OnFail, "abort") {
				result.Status = cycleStatusError
				result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
					State:     "cycle_aborted",
					At:        time.Now().UTC(),
					StepNo:    stepNo,
					Tool:      step.Tool,
					Message:   stepErr.Error(),
					ErrorCode: "step_aborted",
				})
				break
			}
			if stepErr != nil && result.Status == cycleStatusOK {
				result.Status = cycleStatusDegraded
			}
			stepNo++
		}

		ensureStep := func(tool, why string) {
			if time.Now().UTC().After(deadline) {
				return
			}
			for _, st := range executedSteps {
				if strings.EqualFold(st.Tool, tool) {
					return
				}
			}
			step := cyclePlanStep{
				Step:   len(executedSteps) + 1,
				Tool:   tool,
				Why:    why,
				Args:   map[string]any{},
				OnFail: "continue",
			}
			executedSteps = append(executedSteps, step)
			result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
				State:   "tool_call_started",
				At:      time.Now().UTC(),
				StepNo:  step.Step,
				Tool:    step.Tool,
				Message: why,
			})
			exec, stepErr := s.executePlanStep(ctx, step, options, result, funnel)
			result.ToolCalls++
			result.Records = append(result.Records, exec.Records...)
			result.Errors = append(result.Errors, exec.Errors...)
			result.Warnings = append(result.Warnings, exec.Warnings...)
			mergeFreshness(result.Freshness, exec.Freshness)
			if rid, ok := exec.Meta["report_id"]; ok {
				if v, ok := rid.(uint64); ok {
					reportID = v
				}
			}
			now := time.Now().UTC()
			s.persistCycleStep(ctx, result.SessionID, step, exec, stepErr, now, now)
			traceMsg := "tool call finished"
			errCode := ""
			if stepErr != nil {
				traceMsg = stepErr.Error()
				errCode = firstIssueCode(exec.Errors)
			}
			result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
				State:     "tool_call_finished",
				At:        now,
				StepNo:    step.Step,
				Tool:      step.Tool,
				Message:   traceMsg,
				ErrorCode: errCode,
			})
		}
		ensureStep("report.generate", "persist cycle summary for ops and memory")
		ensureStep("report.validate.write", "validate cycle value and write structured verdict")

		plan = cyclePlanOutput{
			CycleGoal: "vertex-controlled execution",
			Decision:  result.Decision,
			Reasoning: "vertex brain loop with tool feedback",
			Plan:      executedSteps,
		}

		if result.Decision == "skip" && result.Status == cycleStatusOK {
			result.Status = cycleStatusSkipped
		}
		if result.Status == cycleStatusOK && len(result.Errors) > 0 {
			result.Status = cycleStatusDegraded
		}
		result.FinishedAt = time.Now().UTC()
		result.BrainTrace = append(result.BrainTrace, CycleStateEvent{
			State:   "cycle_finished",
			At:      result.FinishedAt,
			Message: "vertex brain cycle finished",
		})
		result.DurationMS = result.FinishedAt.Sub(result.StartedAt).Milliseconds()
		s.persistCycleSession(ctx, result, input, promptBundle, plan, funnel)
		s.persistCycleMemory(ctx, result, plan, funnel)
		s.finalizeCycleReport(ctx, reportID, result, funnel)
		s.evaluateStrategyEvolution(promptBundle, options, result, funnel)
		s.observeCycleMetrics(result, fallbackReason)
		return result, nil
	}

	var modelName string
	plan, modelName, fallbackReason = s.buildPlan(ctx, input, promptBundle.System, result)
	plan = appendMandatoryStep(plan, cyclePlanStep{
		Tool:            "report.validate.write",
		Why:             "validate cycle value and write structured verdict",
		Args:            map[string]any{},
		SuccessCriteria: "validation persisted",
		OnFail:          "continue",
	})
	if planErr := validatePlan(plan); planErr != nil {
		result.Warnings = append(result.Warnings, CycleIssue{
			Code:    issueCodePlanValidationFailed,
			Message: planErr.Error(),
			Source:  "agent_cycle",
			Count:   1,
		})
		plan = s.buildFallbackPlan(input)
		if fallbackReason == "" {
			fallbackReason = "plan_validation_failed"
		}
	}
	result.Decision = normalizeCycleDecision(plan.Decision)
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
	var reportID uint64
	for _, step := range sortPlanSteps(plan.Plan) {
		if time.Now().UTC().After(deadline) {
			result.Warnings = append(result.Warnings, CycleIssue{
				Code:    issueCodeCycleDurationExceeded,
				Message: "max cycle duration budget reached",
				Source:  "agent_cycle",
				Count:   1,
			})
			result.Status = cycleStatusDegraded
			result.SkipReason = "max cycle duration reached"
			break
		}
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
			exec, stepErr = s.executePlanStep(ctx, step, options, result, funnel)
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
		if rid, ok := exec.Meta["report_id"]; ok {
			switch v := rid.(type) {
			case uint64:
				reportID = v
			case int:
				if v > 0 {
					reportID = uint64(v)
				}
			case float64:
				if v > 0 {
					reportID = uint64(v)
				}
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

	s.persistCycleSession(ctx, result, input, promptBundle, plan, funnel)
	s.persistCycleMemory(ctx, result, plan, funnel)
	s.finalizeCycleReport(ctx, reportID, result, funnel)
	s.evaluateStrategyEvolution(promptBundle, options, result, funnel)
	s.observeCycleMetrics(result, fallbackReason)
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
	if out.MaxTokensPerCycle <= 0 {
		out.MaxTokensPerCycle = defaultCycleTokenBudget
	}
	if out.MaxCycleDurationSec <= 0 {
		out.MaxCycleDurationSec = int(defaultCycleDurationBudget.Seconds())
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
			MaxTokensPerCycle:   opts.MaxTokensPerCycle,
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

func (s *Service) buildPlan(ctx context.Context, input cyclePromptInput, systemPrompt string, result *CycleResult) (cyclePlanOutput, string, string) {
	modelName := ""
	if s.vertexAI == nil {
		return s.buildFallbackPlan(input), modelName, "vertex_unavailable"
	}
	result.LLMCalls++
	plan, tokenUsed, err := s.vertexAI.PlanCycle(ctx, input, systemPrompt)
	result.LLMTokens += tokenUsed
	if err != nil {
		result.Warnings = append(result.Warnings, CycleIssue{
			Code:    issueCodePlanDecodeFailed,
			Message: err.Error(),
			Source:  "vertex_ai",
			Count:   1,
		})
		return s.buildFallbackPlan(input), modelName, "plan_decode_failed"
	}
	modelName = s.vertexAI.ModelName()
	return *plan, modelName, ""
}

func (s *Service) buildFallbackPlan(input cyclePromptInput) cyclePlanOutput {
	steps := make([]cyclePlanStep, 0, 7)
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
	stepNo++
	steps = append(steps, cyclePlanStep{
		Step:            stepNo,
		Tool:            "report.validate.write",
		Why:             "validate cycle value and write structured verdict",
		Args:            map[string]any{},
		SuccessCriteria: "validation persisted",
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
			MaxTokensPerCycle:   input.Budgets.MaxTokensPerCycle,
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

func appendMandatoryStep(plan cyclePlanOutput, step cyclePlanStep) cyclePlanOutput {
	tool := strings.ToLower(strings.TrimSpace(step.Tool))
	if tool == "" {
		return plan
	}
	for _, it := range plan.Plan {
		if strings.ToLower(strings.TrimSpace(it.Tool)) == tool {
			return plan
		}
	}
	next := 1
	for _, it := range plan.Plan {
		if it.Step >= next {
			next = it.Step + 1
		}
	}
	step.Step = next
	if strings.TrimSpace(step.OnFail) == "" {
		step.OnFail = "continue"
	}
	if step.Args == nil {
		step.Args = map[string]any{}
	}
	plan.Plan = append(plan.Plan, step)
	return plan
}

func buildVertexConversationInit(input cyclePromptInput, maxSteps int) []map[string]any {
	b, _ := json.Marshal(map[string]any{
		"step":      1,
		"max_steps": maxSteps,
		"context":   input,
		"history":   []any{},
	})
	return []map[string]any{
		{
			"role": "user",
			"parts": []map[string]any{
				{"text": "Cycle control input JSON:\n" + string(b)},
			},
		},
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func isMalformedFunctionCallSummary(summary string) bool {
	msg := strings.ToLower(strings.TrimSpace(summary))
	return strings.Contains(msg, "malformed function call")
}

func classifyVertexActionErr(err error) string {
	if err == nil {
		return issueCodePlanDecodeFailed
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "status=429") ||
		strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "too many requests") {
		return issueCodeUpstream429
	}
	return issueCodePlanDecodeFailed
}

func (s *Service) executePlanStep(ctx context.Context, step cyclePlanStep, opts CycleOptions, result *CycleResult, funnel map[string]any) (cycleStepExecution, error) {
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
		return s.runToolReportGenerate(ctx, result, funnel), nil
	case "report.validate.write":
		return s.runToolReportValidateWrite(ctx, result, funnel), nil
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
	ctx = signal.WithCityResolverVertexDisabled(ctx)
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
			"items":        res.Items,
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
	ctx = signal.WithCityResolverVertexDisabled(ctx)
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

func (s *Service) runToolReportGenerate(ctx context.Context, result *CycleResult, funnel map[string]any) cycleStepExecution {
	if s.db == nil || result == nil {
		return unavailableToolExec("report.generate")
	}
	summaryJSON, _ := json.Marshal(map[string]any{
		"session_id":  result.SessionID,
		"cycle_id":    result.CycleID,
		"run_mode":    result.RunMode,
		"decision":    result.Decision,
		"status":      result.Status,
		"llm_calls":   result.LLMCalls,
		"llm_tokens":  result.LLMTokens,
		"tool_calls":  result.ToolCalls,
		"records":     result.Records,
		"errors":      result.Errors,
		"warnings":    result.Warnings,
		"freshness":   result.Freshness,
		"started_at":  result.StartedAt,
		"finished_at": result.FinishedAt,
		"duration_ms": result.DurationMS,
	})
	funnelJSON, _ := json.Marshal(funnel)

	row := model.AgentReport{
		SessionID:   result.SessionID,
		CycleID:     result.CycleID,
		RunMode:     result.RunMode,
		Decision:    result.Decision,
		Status:      "in_progress",
		SummaryJSON: datatypes.JSON(summaryJSON),
		FunnelJSON:  datatypes.JSON(funnelJSON),
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).Create(&row).Error; err != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{
				Code:    "report_persist_failed",
				Message: err.Error(),
				Source:  "report.generate",
				Count:   1,
			}},
		}
	}
	return cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "agent_report", Result: "success", Count: 1},
		},
		Meta: map[string]any{"report_id": row.ID},
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

func (s *Service) finalizeCycleReport(ctx context.Context, reportID uint64, result *CycleResult, funnel map[string]any) {
	if s.db == nil || reportID == 0 || result == nil {
		return
	}
	summaryJSON, _ := json.Marshal(map[string]any{
		"session_id":  result.SessionID,
		"cycle_id":    result.CycleID,
		"run_mode":    result.RunMode,
		"decision":    result.Decision,
		"status":      result.Status,
		"skip_reason": result.SkipReason,
		"llm_calls":   result.LLMCalls,
		"llm_tokens":  result.LLMTokens,
		"tool_calls":  result.ToolCalls,
		"records":     result.Records,
		"errors":      result.Errors,
		"warnings":    result.Warnings,
		"freshness":   result.Freshness,
		"started_at":  result.StartedAt,
		"finished_at": result.FinishedAt,
		"duration_ms": result.DurationMS,
	})
	funnelJSON, _ := json.Marshal(funnel)
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).
		Model(&model.AgentReport{}).
		Where("id = ?", reportID).
		Updates(map[string]any{
			"status":       result.Status,
			"decision":     result.Decision,
			"summary_json": datatypes.JSON(summaryJSON),
			"funnel_json":  datatypes.JSON(funnelJSON),
			"updated_at":   time.Now().UTC(),
		}).Error; err != nil && s.log != nil {
		s.log.Warn("finalize agent report failed", zap.Error(err), zap.Uint64("report_id", reportID))
	}
}

func (s *Service) runToolReportValidateWrite(ctx context.Context, result *CycleResult, funnel map[string]any) cycleStepExecution {
	if s.db == nil || result == nil {
		return unavailableToolExec("report.validate.write")
	}
	input := cycleValidationInput{
		SessionID: result.SessionID,
		CycleID:   result.CycleID,
		RunMode:   result.RunMode,
		Decision:  result.Decision,
		Status:    result.Status,
		Records:   result.Records,
		Errors:    result.Errors,
		Warnings:  result.Warnings,
		Funnel:    funnel,
	}
	output := buildLocalCycleValidation(result, funnel)
	rowID, persistErr := s.persistValidationReport(ctx, result, input, output)
	if persistErr != nil {
		return cycleStepExecution{
			Status: "error",
			Errors: []CycleIssue{{
				Code:    "validation_persist_failed",
				Message: persistErr.Error(),
				Source:  "report.validate.write",
				Count:   1,
			}},
		}
	}

	return cycleStepExecution{
		Status: "success",
		Records: []CycleRecord{
			{Entity: "agent_validation", Result: "success", Count: 1},
		},
		Meta: map[string]any{
			"validation_id": rowID,
			"verdict":       output.Verdict,
			"score":         output.Score,
			"mode":          "local_self_check",
		},
	}
}

func (s *Service) persistValidationReport(ctx context.Context, result *CycleResult, input cycleValidationInput, output *cycleValidationOutput) (uint64, error) {
	if s.db == nil || result == nil || output == nil {
		return 0, fmt.Errorf("invalid validation input")
	}
	inJSON, _ := json.Marshal(input)
	outJSON, _ := json.Marshal(output)
	strengthsJSON, _ := json.Marshal(output.Strengths)
	risksJSON, _ := json.Marshal(output.Risks)
	actionsJSON, _ := json.Marshal(output.Actions)

	row := model.AgentValidation{
		SessionID:      result.SessionID,
		CycleID:        result.CycleID,
		ValidatorModel: "local-self-check-v1",
		Verdict:        output.Verdict,
		Score:          output.Score,
		Summary:        output.Summary,
		StrengthsJSON:  datatypes.JSON(strengthsJSON),
		RisksJSON:      datatypes.JSON(risksJSON),
		ActionsJSON:    datatypes.JSON(actionsJSON),
		InputJSON:      datatypes.JSON(inJSON),
		OutputJSON:     datatypes.JSON(outJSON),
		CreatedAt:      time.Now().UTC(),
	}
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).Create(&row).Error; err != nil {
		return 0, err
	}
	return row.ID, nil
}

func buildLocalCycleValidation(result *CycleResult, funnel map[string]any) *cycleValidationOutput {
	if result == nil {
		return &cycleValidationOutput{
			Verdict: "warning",
			Score:   0,
			Summary: "cycle result is empty",
			Risks:   []string{"missing cycle result"},
			Actions: []string{"inspect agent_cycle execution flow"},
		}
	}

	verdict := "pass"
	score := 90.0
	strengths := []string{}
	risks := []string{}
	actions := []string{}

	if result.Status == cycleStatusError {
		verdict = "fail"
		score = 20
		risks = append(risks, "cycle ended with error status")
		actions = append(actions, "inspect agent_cycle errors and failing step")
	} else if result.Status == cycleStatusDegraded || len(result.Errors) > 0 {
		verdict = "warning"
		score = 60
		risks = append(risks, "cycle completed with degraded status or errors")
		actions = append(actions, "review warnings/errors and reduce tool failures")
	}

	if result.LLMCalls > 1 {
		verdict = "warning"
		if score > 70 {
			score = 70
		}
		risks = append(risks, "llm calls exceed single-call target")
		actions = append(actions, "ensure cycle uses one vertex call for planning")
	} else if result.LLMCalls == 1 {
		strengths = append(strengths, "llm call budget met (1 per cycle)")
	}

	if generated := extractFunnelInt(funnel, "signals_generated"); generated > 0 {
		strengths = append(strengths, fmt.Sprintf("signals generated: %d", generated))
	} else {
		risks = append(risks, "no signals generated in this cycle")
		actions = append(actions, "improve market spec/forecast coverage for signal pipeline")
		if verdict == "pass" {
			verdict = "warning"
			score = min(score, 75.0)
		}
	}

	if len(strengths) == 0 {
		strengths = append(strengths, "cycle executed and persisted records")
	}
	if len(actions) == 0 {
		actions = append(actions, "continue monitoring /ops/status and /ops/agent/validations")
	}
	summary := fmt.Sprintf("local self-check verdict=%s score=%.0f llm_calls=%d tool_calls=%d", verdict, score, result.LLMCalls, result.ToolCalls)
	return &cycleValidationOutput{
		Verdict:   verdict,
		Score:     score,
		Summary:   summary,
		Strengths: uniqueTopN(strengths, 5),
		Risks:     uniqueTopN(risks, 5),
		Actions:   uniqueTopN(actions, 5),
	}
}

func extractFunnelInt(funnel map[string]any, key string) int {
	if len(funnel) == 0 || strings.TrimSpace(key) == "" {
		return 0
	}
	v, ok := funnel[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func (s *Service) persistCycleSession(ctx context.Context, result *CycleResult, input cyclePromptInput, promptBundle cyclePromptBundle, plan cyclePlanOutput, funnel map[string]any) {
	if s.db == nil || result == nil {
		return
	}
	recordsSuccess, recordsError, recordsSkipped := summarizeCycleRecords(result.Records)
	inputJSON, _ := json.Marshal(input)
	planJSON, _ := json.Marshal(plan)
	summaryJSON, _ := json.Marshal(map[string]any{
		"funnel":         funnel,
		"errors":         result.Errors,
		"warnings":       result.Warnings,
		"brain_trace":    result.BrainTrace,
		"no_op_reason":   result.SkipReason,
		"llm_calls":      result.LLMCalls,
		"llm_tokens":     result.LLMTokens,
		"cycle_status":   result.Status,
		"cycle_decision": result.Decision,
	})
	row := model.AgentSession{
		ID:               result.SessionID,
		CycleID:          result.CycleID,
		PromptVersion:    promptBundle.Version,
		PromptVariant:    promptBundle.Variant,
		RunMode:          result.RunMode,
		Model:            result.Model,
		Status:           result.Status,
		Decision:         result.Decision,
		LLMCalls:         result.LLMCalls,
		LLMTokens:        result.LLMTokens,
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
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).Create(&row).Error; err != nil && s.log != nil {
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
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).Create(&row).Error; err != nil && s.log != nil {
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
		"llm_tokens":  result.LLMTokens,
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
	writeCtx, cancel := detachedWriteContext()
	defer cancel()
	if err := s.db.WithContext(writeCtx).Create(&row).Error; err != nil && s.log != nil {
		s.log.Warn("persist agent memory failed", zap.Error(err), zap.String("session_id", result.SessionID))
	}
}

func detachedWriteContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), cyclePersistWriteTimeout)
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

func (s *Service) loadPromptBundle(ctx context.Context, cycleID string) (cyclePromptBundle, error) {
	bundle := cyclePromptBundle{
		Version: defaultCyclePromptVersion,
		System:  cycleSystemPrompt,
		Variant: "builtin",
	}
	if s.db == nil {
		return bundle, nil
	}
	var row model.PromptVersion
	err := s.db.WithContext(ctx).
		Where("is_active = ?", true).
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return bundle, nil
		}
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such table") || strings.Contains(msg, "does not exist") {
			return bundle, nil
		}
		return bundle, err
	}
	version := strings.TrimSpace(row.Version)
	if version != "" {
		bundle.Version = version
	}
	system := strings.TrimSpace(row.SystemPrompt)
	if len(system) >= 32 {
		bundle.System = system
	}
	bundle.Variant = "active"

	// Candidate prompt gray rollout: if candidate exists and hash hits rollout bucket,
	// use candidate this cycle; otherwise keep active.
	var candidate model.PromptVersion
	candidateErr := s.db.WithContext(ctx).
		Where("COALESCE(stage, '') = ? AND COALESCE(rollout_pct, 0) > 0", "candidate").
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&candidate).Error
	if candidateErr != nil && !errors.Is(candidateErr, gorm.ErrRecordNotFound) {
		msg := strings.ToLower(candidateErr.Error())
		if !strings.Contains(msg, "no such table") && !strings.Contains(msg, "does not exist") {
			return bundle, candidateErr
		}
		return bundle, nil
	}
	if candidateErr == nil && shouldUseCandidatePrompt(cycleID, candidate.RolloutPct) {
		cVersion := strings.TrimSpace(candidate.Version)
		cSystem := strings.TrimSpace(candidate.SystemPrompt)
		if cVersion != "" && len(cSystem) >= 32 {
			bundle.Version = cVersion
			bundle.System = cSystem
			bundle.Variant = "candidate"
		}
	}
	return bundle, nil
}

func shouldUseCandidatePrompt(cycleID string, rolloutPct float64) bool {
	if rolloutPct <= 0 {
		return false
	}
	if rolloutPct >= 100 {
		return true
	}
	key := strings.TrimSpace(cycleID)
	if key == "" {
		key = time.Now().UTC().Format("20060102150405")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	bucket := float64(h.Sum32()%10000) / 100.0 // 0.00 ~ 99.99
	return bucket < rolloutPct
}

type paramTuningSnapshot struct {
	SignalsGenerated int
	SpecReady        int
	ForecastReady    int
	ErrorRate        float64
	Had429           bool
}

type paramAdjustmentProposal struct {
	Param        string
	TargetMetric string
	Reason       string
	OldValue     float64
	NewValue     float64
}

func (s *Service) evaluateAgentParamTuning(options CycleOptions, result *CycleResult, funnel map[string]any) {
	if s == nil || s.db == nil || result == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	snapshot := buildParamTuningSnapshot(result, funnel)
	if err := s.reconcileMonitoringParamChanges(ctx, snapshot); err != nil && s.log != nil {
		s.log.Warn("reconcile param tuning failed", zap.Error(err))
	}

	proposals := s.buildParamAdjustmentProposals(options, snapshot)
	if len(proposals) == 0 {
		return
	}
	applied := 0
	for _, proposal := range proposals {
		if applied >= 2 {
			break
		}
		hasMonitoring, err := s.hasMonitoringParamChange(ctx, proposal.Param)
		if err != nil {
			if s.log != nil {
				s.log.Warn("check param monitoring status failed", zap.Error(err), zap.String("param", proposal.Param))
			}
			continue
		}
		if hasMonitoring {
			continue
		}
		if err := s.applyParamAdjustmentProposal(ctx, proposal, snapshot); err != nil {
			if s.log != nil {
				s.log.Warn("apply param proposal failed", zap.Error(err), zap.String("param", proposal.Param))
			}
			continue
		}
		applied++
	}
}

func buildParamTuningSnapshot(result *CycleResult, funnel map[string]any) paramTuningSnapshot {
	s := paramTuningSnapshot{
		SignalsGenerated: extractFunnelInt(funnel, "signals_generated"),
		SpecReady:        extractFunnelInt(funnel, "spec_ready"),
		ForecastReady:    extractFunnelInt(funnel, "forecast_ready"),
	}
	if result != nil {
		if result.Status == cycleStatusError || result.Status == cycleStatusDegraded || len(result.Errors) > 0 {
			s.ErrorRate = 1
		}
		for _, issue := range append(append([]CycleIssue{}, result.Errors...), result.Warnings...) {
			msg := strings.ToLower(strings.TrimSpace(issue.Message))
			code := strings.ToLower(strings.TrimSpace(issue.Code))
			if strings.Contains(code, "429") || strings.Contains(msg, "429") || strings.Contains(msg, "rate limit") {
				s.Had429 = true
				break
			}
		}
	}
	return s
}

func (s *Service) buildParamAdjustmentProposals(options CycleOptions, snap paramTuningSnapshot) []paramAdjustmentProposal {
	proposals := make([]paramAdjustmentProposal, 0, 2)
	adjusted := s.enforceAgentParamConstraints(options)
	if snap.Had429 {
		newExternal := clampFloat(float64(adjusted.MaxExternalRequests)*0.8, 20, 400)
		newToolCalls := clampFloat(float64(adjusted.MaxToolCalls)-1, 4, 24)
		newOpts := adjusted
		newOpts.MaxExternalRequests = int(newExternal)
		newOpts.MaxToolCalls = int(newToolCalls)
		newOpts = s.enforceAgentParamConstraints(newOpts)
		if newOpts.MaxExternalRequests != adjusted.MaxExternalRequests {
			proposals = append(proposals, paramAdjustmentProposal{
				Param:        strategyParamMaxExternalReqs,
				TargetMetric: "error_rate",
				Reason:       "rpc/upstream 429 observed; reduce request pressure",
				OldValue:     float64(adjusted.MaxExternalRequests),
				NewValue:     float64(newOpts.MaxExternalRequests),
			})
		}
		if len(proposals) < 2 && newOpts.MaxToolCalls != adjusted.MaxToolCalls {
			proposals = append(proposals, paramAdjustmentProposal{
				Param:        strategyParamMaxToolCalls,
				TargetMetric: "error_rate",
				Reason:       "429 pressure protection; lower tool fan-out",
				OldValue:     float64(adjusted.MaxToolCalls),
				NewValue:     float64(newOpts.MaxToolCalls),
			})
		}
		return proposals
	}
	if snap.SignalsGenerated == 0 && snap.SpecReady > 0 && snap.ForecastReady > 0 {
		newOpts := adjusted
		newOpts.MarketLimit = int(clampFloat(float64(adjusted.MarketLimit)+50, 50, 500))
		newOpts.MaxExternalRequests = int(clampFloat(float64(adjusted.MaxExternalRequests)+20, 20, 400))
		newOpts = s.enforceAgentParamConstraints(newOpts)
		if newOpts.MarketLimit != adjusted.MarketLimit {
			proposals = append(proposals, paramAdjustmentProposal{
				Param:        strategyParamMarketLimit,
				TargetMetric: "signals_per_run",
				Reason:       "funnel ready but no signals; broaden candidate search",
				OldValue:     float64(adjusted.MarketLimit),
				NewValue:     float64(newOpts.MarketLimit),
			})
		}
		if len(proposals) < 2 && newOpts.MaxExternalRequests != adjusted.MaxExternalRequests {
			proposals = append(proposals, paramAdjustmentProposal{
				Param:        strategyParamMaxExternalReqs,
				TargetMetric: "signals_per_run",
				Reason:       "support wider candidate scan for signal discovery",
				OldValue:     float64(adjusted.MaxExternalRequests),
				NewValue:     float64(newOpts.MaxExternalRequests),
			})
		}
	}
	return proposals
}

func (s *Service) reconcileMonitoringParamChanges(ctx context.Context, snap paramTuningSnapshot) error {
	var changes []model.AgentStrategyChange
	if err := s.db.WithContext(ctx).
		Where("scope = ? AND subject = ? AND status = ?", strategyScopeAgentParamTuning, strategySubjectAgentCycle, "monitoring").
		Order("updated_at ASC, id ASC").
		Find(&changes).Error; err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such table") || strings.Contains(msg, "does not exist") {
			return nil
		}
		return err
	}
	if len(changes) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for i := range changes {
		change := &changes[i]
		latest := tuningObservedMetric(change.TargetMetric, snap)
		improved := isTuningMetricImproved(change.TargetMetric, change.BaselineValue, latest)
		change.ObservedRuns++
		change.LatestValue = latest
		if improved {
			change.NoImproveStreak = 0
		} else {
			change.NoImproveStreak++
		}
		change.UpdatedAt = now
		if change.NoImproveStreak >= 3 {
			if err := s.upsertAgentStrategyParam(ctx, change.ParamName, change.OldValue, "auto rollback: no improvement for 3 runs"); err != nil {
				return err
			}
			change.Status = "rolled_back"
			change.Decision = "rollback"
			change.Reason = "no improvement for 3 consecutive runs"
			change.FinalizedAt = &now
			if s.metrics != nil {
				s.metrics.AddAgentStrategyRollback(strategyScopeAgentParamTuning, "no_improve_3x", 1)
			}
		} else if improved && change.ObservedRuns >= 3 {
			change.Status = "kept"
			change.Decision = "keep"
			change.Reason = "target metric improved in evaluation window"
			change.FinalizedAt = &now
		}
		if err := s.db.WithContext(ctx).Save(change).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) hasMonitoringParamChange(ctx context.Context, param string) (bool, error) {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&model.AgentStrategyChange{}).
		Where("scope = ? AND subject = ? AND param_name = ? AND status = ?", strategyScopeAgentParamTuning, strategySubjectAgentCycle, param, "monitoring").
		Count(&count).Error
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such table") || strings.Contains(msg, "does not exist") {
			return false, nil
		}
		return false, err
	}
	return count > 0, nil
}

func (s *Service) applyParamAdjustmentProposal(ctx context.Context, proposal paramAdjustmentProposal, snap paramTuningSnapshot) error {
	now := time.Now().UTC()
	if err := s.upsertAgentStrategyParam(ctx, proposal.Param, proposal.NewValue, proposal.Reason); err != nil {
		return err
	}
	change := model.AgentStrategyChange{
		Scope:         strategyScopeAgentParamTuning,
		Subject:       strategySubjectAgentCycle,
		ParamName:     proposal.Param,
		OldValue:      proposal.OldValue,
		NewValue:      proposal.NewValue,
		TargetMetric:  proposal.TargetMetric,
		BaselineValue: tuningObservedMetric(proposal.TargetMetric, snap),
		LatestValue:   tuningObservedMetric(proposal.TargetMetric, snap),
		Status:        "monitoring",
		Decision:      "pending",
		Reason:        proposal.Reason,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	return s.db.WithContext(ctx).Create(&change).Error
}

func tuningObservedMetric(target string, snap paramTuningSnapshot) float64 {
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "error_rate":
		return snap.ErrorRate
	default:
		return float64(snap.SignalsGenerated)
	}
}

func isTuningMetricImproved(target string, baseline, latest float64) bool {
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "error_rate":
		return latest < baseline
	default:
		return latest > baseline
	}
}

func (s *Service) upsertAgentStrategyParam(ctx context.Context, key string, value float64, reason string) error {
	now := time.Now().UTC()
	var row model.AgentStrategyParam
	err := s.db.WithContext(ctx).Where("\"key\" = ?", key).Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			row = model.AgentStrategyParam{
				Key:       key,
				Scope:     strategySubjectAgentCycle,
				Value:     value,
				Enabled:   true,
				Reason:    reason,
				CreatedAt: now,
				UpdatedAt: now,
			}
			return s.db.WithContext(ctx).Create(&row).Error
		}
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "no such table") || strings.Contains(msg, "does not exist") {
			return nil
		}
		return err
	}
	row.Value = value
	row.Enabled = true
	row.Reason = reason
	row.UpdatedAt = now
	return s.db.WithContext(ctx).Save(&row).Error
}

func (s *Service) applyAgentCycleParamOverrides(ctx context.Context, options CycleOptions) CycleOptions {
	if s == nil || s.db == nil {
		return s.enforceAgentParamConstraints(options)
	}
	var rows []model.AgentStrategyParam
	err := s.db.WithContext(ctx).
		Where("scope = ? AND enabled = ?", strategySubjectAgentCycle, true).
		Find(&rows).Error
	if err != nil {
		msg := strings.ToLower(err.Error())
		if s.log != nil && !strings.Contains(msg, "no such table") && !strings.Contains(msg, "does not exist") {
			s.log.Warn("load strategy param overrides failed", zap.Error(err))
		}
		return s.enforceAgentParamConstraints(options)
	}
	for _, row := range rows {
		switch strings.TrimSpace(row.Key) {
		case strategyParamMaxToolCalls:
			options.MaxToolCalls = int(row.Value)
		case strategyParamMaxExternalReqs:
			options.MaxExternalRequests = int(row.Value)
		case strategyParamMarketLimit:
			options.MarketLimit = int(row.Value)
		}
	}
	return s.enforceAgentParamConstraints(options)
}

func (s *Service) enforceAgentParamConstraints(options CycleOptions) CycleOptions {
	out := normalizeCycleOptions(options)
	out.MaxToolCalls = int(clampFloat(float64(out.MaxToolCalls), 4, 24))
	out.MaxExternalRequests = int(clampFloat(float64(out.MaxExternalRequests), 20, 400))
	out.MarketLimit = int(clampFloat(float64(out.MarketLimit), 50, 500))
	minExternal := out.MaxToolCalls * 3
	if out.MaxExternalRequests < minExternal {
		out.MaxExternalRequests = minExternal
	}
	if out.MaxExternalRequests > 400 {
		out.MaxExternalRequests = 400
	}
	return out
}

func clampFloat(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func (s *Service) evaluateStrategyEvolution(promptBundle cyclePromptBundle, options CycleOptions, result *CycleResult, funnel map[string]any) {
	if s == nil || s.db == nil {
		return
	}
	s.evaluateAgentParamTuning(options, result, funnel)
	version := strings.TrimSpace(promptBundle.Version)
	if version == "" || strings.TrimSpace(promptBundle.Variant) != "candidate" {
		return
	}
	observed := extractFunnelInt(funnel, "signals_generated")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.evaluateCandidatePromptRollout(ctx, version, observed); err != nil && s.log != nil {
		s.log.Warn("evaluate strategy evolution failed",
			zap.Error(err),
			zap.String("scope", strategyScopePromptRollout),
			zap.String("subject", version),
			zap.String("session_id", result.SessionID),
		)
	}
}

func (s *Service) evaluateCandidatePromptRollout(ctx context.Context, version string, observedSignals int) error {
	var prompt model.PromptVersion
	if err := s.db.WithContext(ctx).
		Where("version = ? AND COALESCE(stage,'') = ?", version, "candidate").
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&prompt).Error; err != nil {
		return err
	}

	var change model.AgentStrategyChange
	err := s.db.WithContext(ctx).
		Where("scope = ? AND subject = ? AND status = ?", strategyScopePromptRollout, version, "monitoring").
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&change).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		baseline, bErr := s.activePromptSignalsBaseline(ctx, 24)
		if bErr != nil {
			return bErr
		}
		change = model.AgentStrategyChange{
			Scope:         strategyScopePromptRollout,
			Subject:       version,
			ParamName:     "rollout_pct",
			OldValue:      0,
			NewValue:      prompt.RolloutPct,
			TargetMetric:  "signals_per_run",
			BaselineValue: baseline,
			Status:        "monitoring",
			Decision:      "pending",
			CreatedAt:     time.Now().UTC(),
		}
		if err := s.db.WithContext(ctx).Create(&change).Error; err != nil {
			return err
		}
	}

	obsv := float64(observedSignals)
	improved := obsv > change.BaselineValue
	change.ObservedRuns++
	change.LatestValue = obsv
	if improved {
		change.NoImproveStreak = 0
	} else {
		change.NoImproveStreak++
	}
	change.UpdatedAt = time.Now().UTC()

	if change.NoImproveStreak >= 3 {
		now := time.Now().UTC()
		if err := s.db.WithContext(ctx).
			Model(&model.PromptVersion{}).
			Where("version = ? AND COALESCE(stage,'') = ?", version, "candidate").
			Updates(map[string]any{
				"rollout_pct": 0,
				"updated_at":  now,
			}).Error; err != nil {
			return err
		}
		change.Status = "rolled_back"
		change.Decision = "rollback"
		change.Reason = "no improvement for 3 consecutive candidate cycles"
		change.FinalizedAt = &now
		if s.metrics != nil {
			s.metrics.AddAgentStrategyRollback(strategyScopePromptRollout, "no_improve_3x", 1)
		}
	} else if improved && change.ObservedRuns >= 3 {
		now := time.Now().UTC()
		change.Status = "kept"
		change.Decision = "keep"
		change.Reason = "candidate improved target metric in evaluation window"
		change.FinalizedAt = &now
	}

	return s.db.WithContext(ctx).Save(&change).Error
}

func (s *Service) activePromptSignalsBaseline(ctx context.Context, windowHours int) (float64, error) {
	if windowHours <= 0 {
		windowHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(windowHours) * time.Hour)
	var rows []model.AgentSession
	if err := s.db.WithContext(ctx).
		Where("started_at >= ? AND COALESCE(prompt_variant, '') = ?", cutoff, "active").
		Order("started_at DESC").
		Limit(500).
		Find(&rows).Error; err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	var sum int64
	for _, row := range rows {
		sum += extractSignalsGeneratedFromSummaryJSON(row.SummaryJSON)
	}
	return float64(sum) / float64(len(rows)), nil
}

func extractSignalsGeneratedFromSummaryJSON(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	funnel, ok := obj["funnel"].(map[string]any)
	if !ok {
		return 0
	}
	return int64(extractFunnelInt(funnel, "signals_generated"))
}

func (s *Service) acquireCycleLock(ctx context.Context) (unlock func(), acquired bool, err error) {
	if s.db == nil {
		if !s.cycleMu.TryLock() {
			return nil, false, nil
		}
		return s.cycleMu.Unlock, true, nil
	}
	if strings.EqualFold(s.db.Dialector.Name(), "postgres") {
		sqlDB, dbErr := s.db.DB()
		if dbErr != nil {
			return nil, false, dbErr
		}
		conn, connErr := sqlDB.Conn(ctx)
		if connErr != nil {
			return nil, false, connErr
		}
		var ok bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", defaultCycleAdvisoryLockKey).Scan(&ok); err != nil {
			_ = conn.Close()
			return nil, false, err
		}
		if !ok {
			_ = conn.Close()
			return nil, false, nil
		}
		unlockFn := func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var released bool
			_ = conn.QueryRowContext(releaseCtx, "SELECT pg_advisory_unlock($1)", defaultCycleAdvisoryLockKey).Scan(&released)
			_ = conn.Close()
		}
		return unlockFn, true, nil
	}

	if !s.cycleMu.TryLock() {
		return nil, false, nil
	}
	return s.cycleMu.Unlock, true, nil
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
	allowed := map[string]map[string]struct{}{
		"markets":           {"updated_at": {}},
		"market_prices":     {"captured_at": {}},
		"forecasts":         {"fetched_at": {}},
		"competitor_trades": {"timestamp": {}},
	}
	columns, ok := allowed[table]
	if !ok {
		return ""
	}
	if _, ok := columns[column]; !ok {
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

func (s *Service) observeCycleMetrics(result *CycleResult, fallbackReason string) {
	if s == nil || s.metrics == nil || result == nil {
		return
	}
	s.metrics.ObserveAgentCycle(result.Status, result.Decision, result.FinishedAt.Sub(result.StartedAt), result.LLMCalls, result.LLMTokens)
	if fallbackReason != "" {
		s.metrics.AddAgentCycleFallback(fallbackReason, 1)
	}
	for _, issue := range result.Errors {
		s.metrics.AddAgentCycleToolError(issue.Source, issue.Code, max(issue.Count, 1))
	}
	for _, issue := range result.Warnings {
		// warnings are informative; only count explicit tool issues with code.
		if strings.TrimSpace(issue.Code) == "" {
			continue
		}
		s.metrics.AddAgentCycleToolError(issue.Source, issue.Code, max(issue.Count, 1))
	}
}

func normalizeCycleDecision(input string) string {
	d := strings.ToLower(strings.TrimSpace(input))
	if d == "" {
		return "run"
	}
	switch d {
	case "run", "skip":
		return d
	}
	if strings.Contains(d, "skip") || strings.Contains(d, "hold") || strings.Contains(d, "wait") {
		return "skip"
	}
	if strings.Contains(d, "run") || strings.Contains(d, "proceed") || strings.Contains(d, "continue") {
		return "run"
	}
	return "run"
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
