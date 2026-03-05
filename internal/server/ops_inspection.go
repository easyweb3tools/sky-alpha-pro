package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/metrics"
)

type opsInspectionResponse struct {
	Timestamp         time.Time                   `json:"timestamp"`
	Scheduler         scheduler.ManagerSnapshot   `json:"scheduler"`
	Summary           opsSummary                  `json:"summary"`
	Freshness         map[string]float64          `json:"freshness"`
	SchedulerLedger   inspectionLedger            `json:"scheduler_ledger"`
	DataSnapshot      inspectionDataSnapshot      `json:"data_snapshot"`
	ValueDashboard    inspectionValueDashboard    `json:"value_dashboard"`
	SpecFillTrend     inspectionSpecFillTrend     `json:"spec_fill_trend"`
	SpecFillDiag      inspectionSpecFillDiag      `json:"spec_fill_diagnostics"`
	Profitability     profitabilityConclusion     `json:"profitability"`
	ValidationStatus  inspectionValidationStats   `json:"validation_status"`
	StrategyEvolution inspectionStrategyEvolution `json:"strategy_evolution"`
	ParamTuning       inspectionParamTuning       `json:"param_tuning"`
	PromptRollout     inspectionPromptRollout     `json:"prompt_rollout"`
	EventPipeline     inspectionEventPipeline     `json:"event_pipeline"`
	CandidateFunnel   inspectionCandidateFunnel   `json:"candidate_funnel"`
	OpportunityWin    inspectionOpportunityWin    `json:"opportunity_windows"`
	VertexBrainBudget inspectionVertexBrainBudget `json:"vertex_brain_budget"`
	ControlPlane      inspectionControlPlane      `json:"control_plane"`
	Alerts            []inspectionAlert           `json:"alerts"`
	ImmediateActions  []string                    `json:"immediate_actions"`
	H48Actions        []string                    `json:"h48_actions"`
	Actions           []string                    `json:"actions"`
}

type inspectionLedger struct {
	RecentRuns     []inspectionRunAgg  `json:"recent_runs"`
	HourlyByStatus []inspectionGroupKV `json:"hourly_by_status"`
	HourlyByError  []inspectionGroupKV `json:"hourly_by_error"`
}

type inspectionRunAgg struct {
	ID         uint64    `json:"id"`
	JobName    string    `json:"job_name"`
	Status     string    `json:"status"`
	ErrorCode  string    `json:"error_code,omitempty"`
	ErrorMsg   string    `json:"error_message,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationMS int       `json:"duration_ms"`
	RecSuccess int       `json:"records_success"`
	RecError   int       `json:"records_error"`
	RecSkipped int       `json:"records_skipped"`
}

type inspectionGroupKV struct {
	Key1  string `json:"key1"`
	Key2  string `json:"key2,omitempty"`
	Count int64  `json:"count"`
}

type inspectionDataSnapshot struct {
	TableCounts map[string]int64   `json:"table_counts"`
	LastUpdated map[string]*string `json:"last_updated"`
}

type inspectionValueDashboard struct {
	Signals7D               int64   `json:"signals_7d"`
	Signals30D              int64   `json:"signals_30d"`
	Trades7D                int64   `json:"trades_7d"`
	Trades30D               int64   `json:"trades_30d"`
	SettledTrades7D         int64   `json:"settled_trades_7d"`
	SettledTrades30D        int64   `json:"settled_trades_30d"`
	NetPnL7D                float64 `json:"net_pnl_7d"`
	NetPnL30D               float64 `json:"net_pnl_30d"`
	WinRate7DPct            float64 `json:"win_rate_7d_pct"`
	WinRate30DPct           float64 `json:"win_rate_30d_pct"`
	SignalTradeConversion30 float64 `json:"signal_trade_conversion_30d_pct"`
	AvgEdge30D              float64 `json:"avg_edge_30d"`
	P50Edge30D              float64 `json:"p50_edge_30d"`
	P90Edge30D              float64 `json:"p90_edge_30d"`
	MaxDrawdown30D          float64 `json:"max_drawdown_30d"`
	LatestMarketsTotal      int64   `json:"latest_markets_total"`
	LatestSpecReady         int64   `json:"latest_spec_ready"`
	LatestForecastReady     int64   `json:"latest_forecast_ready"`
	LatestSignalsGenerated  int64   `json:"latest_signals_generated"`
	LatestSkipped           int64   `json:"latest_skipped"`
}

type inspectionSpecFillTrend struct {
	WindowHours             int                         `json:"window_hours"`
	Runs24H                 int64                       `json:"runs_24h"`
	Success24H              int64                       `json:"success_24h"`
	Error24H                int64                       `json:"error_24h"`
	Skipped24H              int64                       `json:"skipped_24h"`
	SuccessRate24H          float64                     `json:"success_rate_24h"`
	EffectiveSuccess24H     int64                       `json:"effective_success_24h"`
	EffectiveSuccessRate24H float64                     `json:"effective_success_rate_24h"`
	NoProgress24H           int64                       `json:"no_progress_24h"`
	Hourly                  []inspectionSpecFillHourAgg `json:"hourly"`
}

type inspectionSpecFillHourAgg struct {
	Hour                 string  `json:"hour"`
	Runs                 int64   `json:"runs"`
	Success              int64   `json:"success"`
	Error                int64   `json:"error"`
	Skipped              int64   `json:"skipped"`
	SuccessRate          float64 `json:"success_rate"`
	EffectiveSuccess     int64   `json:"effective_success"`
	EffectiveSuccessRate float64 `json:"effective_success_rate"`
	NoProgress           int64   `json:"no_progress"`
}

type inspectionSpecFillDiag struct {
	ActiveMarkets         int64               `json:"active_markets"`
	SupportedMarkets      int64               `json:"supported_markets"`
	UnsupportedTopReasons []inspectionGroupKV `json:"unsupported_top_reasons"`
	SpecReady             int64               `json:"spec_ready"`
	CityMissing           int64               `json:"city_missing"`
	ThresholdMissing      int64               `json:"threshold_missing"`
	ComparatorMissing     int64               `json:"comparator_missing"`
	TargetDateMissing     int64               `json:"target_date_missing"`
	BySpecStatus          map[string]int64    `json:"by_spec_status"`
}

type profitabilityConclusion struct {
	HealthReady bool   `json:"health_ready"`
	ValueReady  bool   `json:"value_ready"`
	Reason      string `json:"reason"`
}

type inspectionValidationStats struct {
	Count24H    int64   `json:"count_24h"`
	LastVerdict string  `json:"last_verdict,omitempty"`
	LastScore   float64 `json:"last_score,omitempty"`
	LastAt      *string `json:"last_at,omitempty"`
}

type inspectionStrategyEvolution struct {
	WindowHours         int64   `json:"window_hours"`
	CandidateVersion    string  `json:"candidate_version,omitempty"`
	CandidateRolloutPct float64 `json:"candidate_rollout_pct"`
	Changes24H          int64   `json:"changes_24h"`
	Monitoring24H       int64   `json:"monitoring_24h"`
	Kept24H             int64   `json:"kept_24h"`
	RolledBack24H       int64   `json:"rolled_back_24h"`
	LastDecision        string  `json:"last_decision,omitempty"`
	LastReason          string  `json:"last_reason,omitempty"`
	LastSubject         string  `json:"last_subject,omitempty"`
	LastUpdatedAt       *string `json:"last_updated_at,omitempty"`
}

type inspectionPromptRollout struct {
	WindowHours            int64   `json:"window_hours"`
	ActiveRuns             int64   `json:"active_runs"`
	CandidateRuns          int64   `json:"candidate_runs"`
	ActiveSignalsPerRun    float64 `json:"active_signals_per_run"`
	CandidateSignalsPerRun float64 `json:"candidate_signals_per_run"`
	ImprovementPct         float64 `json:"improvement_pct"`
}

type inspectionParamTuning struct {
	WindowHours    int64              `json:"window_hours"`
	ParamsCount    int64              `json:"params_count"`
	EnabledParams  int64              `json:"enabled_params"`
	Monitoring24H  int64              `json:"monitoring_24h"`
	Kept24H        int64              `json:"kept_24h"`
	RolledBack24H  int64              `json:"rolled_back_24h"`
	LastDecision   string             `json:"last_decision,omitempty"`
	LastReason     string             `json:"last_reason,omitempty"`
	LastUpdatedAt  *string            `json:"last_updated_at,omitempty"`
	CurrentParams  map[string]float64 `json:"current_params"`
	CurrentEnabled map[string]bool    `json:"current_enabled"`
}

type inspectionAlert struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type inspectionEventPipeline struct {
	WindowHours                     int64   `json:"window_hours"`
	TotalEvents24                   int64   `json:"total_events_24h"`
	PendingEvents                   int64   `json:"pending_events"`
	AgentCycles24                   int64   `json:"agent_cycles_24h"`
	AgentCycleSuccess24             int64   `json:"agent_cycle_success_24h"`
	AgentCycleDegraded24            int64   `json:"agent_cycle_degraded_24h"`
	AgentCycleError24               int64   `json:"agent_cycle_error_24h"`
	SignalsFromEventCycles24        int64   `json:"signals_from_event_cycles_24h"`
	TriggerToSignalLatencyAvgSecond float64 `json:"trigger_to_signal_latency_avg_seconds"`
	TriggerToSignalLatencyP50Second float64 `json:"trigger_to_signal_latency_p50_seconds"`
	TriggerToSignalLatencyP90Second float64 `json:"trigger_to_signal_latency_p90_seconds"`
	LastEventAt                     *string `json:"last_event_at,omitempty"`
}

type inspectionCandidateFunnel struct {
	Cold     int64 `json:"cold"`
	Watch    int64 `json:"watch"`
	Hot      int64 `json:"hot"`
	Tradable int64 `json:"tradable"`
	CoolDown int64 `json:"cool_down"`
}

type inspectionOpportunityWin struct {
	WindowHours            int64   `json:"window_hours"`
	TotalCycles24          int64   `json:"total_cycles_24h"`
	TriggeredCycles24      int64   `json:"triggered_cycles_24h"`
	CyclesWithSignals24    int64   `json:"cycles_with_signals_24h"`
	SignalHitRate24        float64 `json:"signal_hit_rate_24h"`
	AvgSignalsPerTriggered float64 `json:"avg_signals_per_triggered_cycle_24h"`
}

type inspectionVertexBrainBudget struct {
	WindowHours               int64   `json:"window_hours"`
	VertexRuns24              int64   `json:"vertex_runs_24h"`
	AvgLLMCalls               float64 `json:"avg_llm_calls"`
	AvgLLMTokens              float64 `json:"avg_llm_tokens"`
	MaxLLMTokens              int64   `json:"max_llm_tokens"`
	TokenBudgetExhausted24    int64   `json:"token_budget_exhausted_24h"`
	DurationBudgetExceeded24  int64   `json:"duration_budget_exceeded_24h"`
	ExternalBudgetExhausted24 int64   `json:"external_budget_exhausted_24h"`
	ToolBudgetExhausted24     int64   `json:"tool_budget_exhausted_24h"`
	PlanDecodeFailed24        int64   `json:"plan_decode_failed_24h"`
	PlanDecodeFailed1H        int64   `json:"plan_decode_failed_1h"`
	LastPlanDecodeFailedAt    *string `json:"last_plan_decode_failed_at,omitempty"`
	BudgetPressureRate24      float64 `json:"budget_pressure_rate_24h"`
}

type inspectionControlPlane struct {
	Mode                         string `json:"mode"`
	LegacyVertexAnalyzeEnabled   bool   `json:"legacy_vertex_analyze_enabled"`
	LegacyVertexAnalyzeBypass24H int64  `json:"legacy_vertex_analyze_bypass_24h"`
}

func OpsInspectionHandler(db *gorm.DB, metricReg *metrics.Registry, schedulerMgr *scheduler.Manager) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		if db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_DB_UNAVAILABLE", "message": "database is unavailable"},
			})
			return
		}

		resp := opsInspectionResponse{
			Timestamp: time.Now().UTC(),
			Freshness: map[string]float64{},
			Summary: opsSummary{
				Blockers: make([]opsBlocker, 0),
			},
			SchedulerLedger: inspectionLedger{
				RecentRuns:     make([]inspectionRunAgg, 0),
				HourlyByStatus: make([]inspectionGroupKV, 0),
				HourlyByError:  make([]inspectionGroupKV, 0),
			},
			DataSnapshot: inspectionDataSnapshot{
				TableCounts: map[string]int64{},
				LastUpdated: map[string]*string{},
			},
			ValueDashboard: inspectionValueDashboard{},
			SpecFillTrend: inspectionSpecFillTrend{
				WindowHours: 24,
				Hourly:      make([]inspectionSpecFillHourAgg, 0),
			},
			SpecFillDiag: inspectionSpecFillDiag{
				BySpecStatus:          map[string]int64{},
				UnsupportedTopReasons: make([]inspectionGroupKV, 0),
			},
			Profitability: profitabilityConclusion{
				HealthReady: false,
				ValueReady:  false,
			},
			Alerts:           make([]inspectionAlert, 0),
			ImmediateActions: make([]string, 0, 6),
			H48Actions:       make([]string, 0, 6),
			Actions:          make([]string, 0, 12),
			StrategyEvolution: inspectionStrategyEvolution{
				WindowHours: 24,
			},
			ParamTuning: inspectionParamTuning{
				WindowHours:    24,
				CurrentParams:  map[string]float64{},
				CurrentEnabled: map[string]bool{},
			},
			PromptRollout: inspectionPromptRollout{
				WindowHours: 24,
			},
			EventPipeline: inspectionEventPipeline{
				WindowHours: 24,
			},
			OpportunityWin: inspectionOpportunityWin{
				WindowHours: 24,
			},
			VertexBrainBudget: inspectionVertexBrainBudget{
				WindowHours: 24,
			},
			ControlPlane: inspectionControlPlane{
				Mode: "vertex_brain",
			},
		}
		if schedulerMgr != nil {
			resp.Scheduler = schedulerMgr.Snapshot()
			resp.Summary = summarizeSchedulerSnapshot(resp.Scheduler)
		}
		if metricReg != nil {
			resp.Freshness = metricReg.SnapshotDataFreshness()
		}

		if err := fillInspectionLedger(c.Request.Context(), db, &resp.SchedulerLedger); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_LEDGER_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionDataSnapshot(c.Request.Context(), db, &resp.DataSnapshot); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_SNAPSHOT_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionValueDashboard(c.Request.Context(), db, &resp.ValueDashboard); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_VALUE_DASHBOARD_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionValidationStats(c.Request.Context(), db, &resp.ValidationStatus); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_VALIDATION_STATS_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionStrategyEvolution(c.Request.Context(), db, &resp.StrategyEvolution); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_STRATEGY_EVOLUTION_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionParamTuning(c.Request.Context(), db, &resp.ParamTuning); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_PARAM_TUNING_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionPromptRollout(c.Request.Context(), db, &resp.PromptRollout); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_PROMPT_ROLLOUT_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionEventPipeline(c.Request.Context(), db, &resp.EventPipeline); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_EVENT_PIPELINE_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionCandidateFunnel(c.Request.Context(), db, &resp.CandidateFunnel); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_CANDIDATE_FUNNEL_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionOpportunityWindow(c.Request.Context(), db, &resp.OpportunityWin); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_OPPORTUNITY_WINDOW_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionVertexBrainBudget(c.Request.Context(), db, &resp.VertexBrainBudget); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_VERTEX_BUDGET_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillInspectionControlPlane(c.Request.Context(), db, &resp.ControlPlane); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_CONTROL_PLANE_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillSpecFillTrend(c.Request.Context(), db, &resp.SpecFillTrend); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_SPEC_FILL_TREND_FAILED", "message": err.Error()},
			})
			return
		}
		if err := fillSpecFillDiagnostics(c.Request.Context(), db, &resp.SpecFillDiag); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_SPEC_FILL_DIAG_FAILED", "message": err.Error()},
			})
			return
		}

		resp.Profitability = concludeProfitability(resp)
		resp.Alerts = buildInspectionAlerts(resp)
		resp.ImmediateActions, resp.H48Actions = buildInspectionActionPlan(resp)
		resp.Actions = append(resp.Actions, resp.ImmediateActions...)
		resp.Actions = append(resp.Actions, resp.H48Actions...)
		c.JSON(http.StatusOK, resp)
	}
}

func fillInspectionLedger(ctx context.Context, db *gorm.DB, out *inspectionLedger) error {
	var runs []model.SchedulerRun
	if err := db.WithContext(ctx).Order("id DESC").Limit(30).Find(&runs).Error; err != nil {
		return err
	}
	for _, row := range runs {
		out.RecentRuns = append(out.RecentRuns, inspectionRunAgg{
			ID:         row.ID,
			JobName:    row.JobName,
			Status:     row.Status,
			ErrorCode:  row.ErrorCode,
			ErrorMsg:   row.ErrorMessage,
			StartedAt:  row.StartedAt,
			FinishedAt: row.FinishedAt,
			DurationMS: row.DurationMS,
			RecSuccess: row.RecordsSuccess,
			RecError:   row.RecordsError,
			RecSkipped: row.RecordsSkipped,
		})
	}
	cutoff := time.Now().UTC().Add(-1 * time.Hour)
	type byStatus struct {
		JobName string
		Status  string
		Count   int64
	}
	var statusRows []byStatus
	if err := db.WithContext(ctx).
		Table("scheduler_runs").
		Select("job_name, status, COUNT(*) AS count").
		Where("started_at >= ?", cutoff).
		Group("job_name, status").
		Order("job_name ASC, status ASC").
		Scan(&statusRows).Error; err != nil {
		return err
	}
	for _, row := range statusRows {
		out.HourlyByStatus = append(out.HourlyByStatus, inspectionGroupKV{
			Key1:  row.JobName,
			Key2:  row.Status,
			Count: row.Count,
		})
	}

	type byErr struct {
		JobName   string
		ErrorCode string
		Count     int64
	}
	var errRows []byErr
	if err := db.WithContext(ctx).
		Table("scheduler_runs").
		Select("job_name, error_code, COUNT(*) AS count").
		Where("started_at >= ?", cutoff).
		Where("COALESCE(error_code, '') <> ''").
		Group("job_name, error_code").
		Order("count DESC").
		Scan(&errRows).Error; err != nil {
		return err
	}
	for _, row := range errRows {
		out.HourlyByError = append(out.HourlyByError, inspectionGroupKV{
			Key1:  row.JobName,
			Key2:  row.ErrorCode,
			Count: row.Count,
		})
	}
	return nil
}

func fillInspectionDataSnapshot(ctx context.Context, db *gorm.DB, out *inspectionDataSnapshot) error {
	tables := []string{"markets", "market_prices", "forecasts", "observations", "competitors", "competitor_trades", "signals", "trades"}
	for _, table := range tables {
		var cnt int64
		if err := db.WithContext(ctx).Table(table).Count(&cnt).Error; err != nil {
			return err
		}
		out.TableCounts[table] = cnt
	}

	setMax := func(key, table, col string) error {
		var v sql.NullTime
		if err := db.WithContext(ctx).Table(table).Select("MAX(" + col + ")").Scan(&v).Error; err != nil {
			return err
		}
		if !v.Valid || v.Time.IsZero() {
			out.LastUpdated[key] = nil
			return nil
		}
		s := v.Time.UTC().Format(time.RFC3339)
		out.LastUpdated[key] = &s
		return nil
	}
	if err := setMax("market_prices_last_at", "market_prices", "captured_at"); err != nil {
		return err
	}
	if err := setMax("forecasts_last_at", "forecasts", "fetched_at"); err != nil {
		return err
	}
	if err := setMax("observations_last_at", "observations", "observed_at"); err != nil {
		return err
	}
	if err := setMax("competitor_trades_last_at", "competitor_trades", "timestamp"); err != nil {
		return err
	}
	if err := setMax("signals_last_at", "signals", "created_at"); err != nil {
		return err
	}
	if err := setMax("trades_last_at", "trades", "created_at"); err != nil {
		return err
	}
	return nil
}

func fillInspectionValueDashboard(ctx context.Context, db *gorm.DB, out *inspectionValueDashboard) error {
	cutoff7 := time.Now().UTC().Add(-7 * 24 * time.Hour)
	cutoff30 := time.Now().UTC().Add(-30 * 24 * time.Hour)

	if err := db.WithContext(ctx).Table("signals").Where("created_at >= ?", cutoff7).Count(&out.Signals7D).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("signals").Where("created_at >= ?", cutoff30).Count(&out.Signals30D).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("trades").Where("created_at >= ?", cutoff7).Count(&out.Trades7D).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("trades").Where("created_at >= ?", cutoff30).Count(&out.Trades30D).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("trades").Where("created_at >= ? AND UPPER(status) = ?", cutoff7, "SETTLED").Count(&out.SettledTrades7D).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("trades").Where("created_at >= ? AND UPPER(status) = ?", cutoff30, "SETTLED").Count(&out.SettledTrades30D).Error; err != nil {
		return err
	}

	type pnlAgg struct {
		Net decimal.NullDecimal `gorm:"column:net"`
	}
	var pnl7 pnlAgg
	if err := db.WithContext(ctx).Table("trades").
		Select("COALESCE(SUM(pnl_usdc), 0) AS net").
		Where("created_at >= ? AND UPPER(status) = ?", cutoff7, "SETTLED").
		Scan(&pnl7).Error; err != nil {
		return err
	}
	if pnl7.Net.Valid {
		out.NetPnL7D = pnl7.Net.Decimal.InexactFloat64()
	}
	var pnl30 pnlAgg
	if err := db.WithContext(ctx).Table("trades").
		Select("COALESCE(SUM(pnl_usdc), 0) AS net").
		Where("created_at >= ? AND UPPER(status) = ?", cutoff30, "SETTLED").
		Scan(&pnl30).Error; err != nil {
		return err
	}
	if pnl30.Net.Valid {
		out.NetPnL30D = pnl30.Net.Decimal.InexactFloat64()
	}

	out.WinRate7DPct = calcWinRate(ctx, db, cutoff7)
	out.WinRate30DPct = calcWinRate(ctx, db, cutoff30)

	if out.Signals30D > 0 {
		out.SignalTradeConversion30 = roundTo(100.0*float64(out.Trades30D)/float64(out.Signals30D), 2)
	}

	edges, err := loadSignalEdges(ctx, db, cutoff30)
	if err != nil {
		return err
	}
	if len(edges) > 0 {
		sort.Float64s(edges)
		sum := 0.0
		for _, e := range edges {
			sum += e
		}
		out.AvgEdge30D = roundTo(sum/float64(len(edges)), 4)
		out.P50Edge30D = roundTo(percentile(edges, 0.5), 4)
		out.P90Edge30D = roundTo(percentile(edges, 0.9), 4)
	}

	dd, err := calcMaxDrawdown30D(ctx, db, cutoff30)
	if err != nil {
		return err
	}
	out.MaxDrawdown30D = roundTo(dd, 4)

	type latestRun struct {
		MarketsTotal     int64 `gorm:"column:markets_total"`
		SpecReady        int64 `gorm:"column:spec_ready"`
		ForecastReady    int64 `gorm:"column:forecast_ready"`
		SignalsGenerated int64 `gorm:"column:signals_generated"`
		Skipped          int64 `gorm:"column:skipped"`
	}
	var lr latestRun
	if err := db.WithContext(ctx).Table("signal_runs").
		Select("markets_total, spec_ready, forecast_ready, signals_generated, skipped").
		Order("id DESC").
		Limit(1).
		Scan(&lr).Error; err != nil {
		return err
	}
	out.LatestMarketsTotal = lr.MarketsTotal
	out.LatestSpecReady = lr.SpecReady
	out.LatestForecastReady = lr.ForecastReady
	out.LatestSignalsGenerated = lr.SignalsGenerated
	out.LatestSkipped = lr.Skipped
	return nil
}

func fillInspectionValidationStats(ctx context.Context, db *gorm.DB, out *inspectionValidationStats) error {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	if err := db.WithContext(ctx).Table("agent_validations").Where("created_at >= ?", cutoff).Count(&out.Count24H).Error; err != nil {
		return err
	}
	var row model.AgentValidation
	if err := db.WithContext(ctx).Order("id DESC").Limit(1).Take(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	out.LastVerdict = row.Verdict
	out.LastScore = row.Score
	ts := row.CreatedAt.UTC().Format(time.RFC3339)
	out.LastAt = &ts
	return nil
}

func fillInspectionStrategyEvolution(ctx context.Context, db *gorm.DB, out *inspectionStrategyEvolution) error {
	if out.WindowHours <= 0 {
		out.WindowHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(out.WindowHours) * time.Hour)

	var pv model.PromptVersion
	if err := db.WithContext(ctx).
		Where("COALESCE(stage,'') = ? AND COALESCE(rollout_pct,0) > 0", "candidate").
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&pv).Error; err == nil {
		out.CandidateVersion = strings.TrimSpace(pv.Version)
		out.CandidateRolloutPct = roundTo(pv.RolloutPct, 2)
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && !strings.Contains(strings.ToLower(err.Error()), "does not exist") {
		return err
	}

	type agg struct {
		Changes    int64 `gorm:"column:changes"`
		Monitoring int64 `gorm:"column:monitoring"`
		Kept       int64 `gorm:"column:kept"`
		RolledBack int64 `gorm:"column:rolled_back"`
	}
	var stats agg
	if err := db.WithContext(ctx).
		Table("agent_strategy_changes").
		Select(
			"COUNT(*) AS changes, "+
				"COALESCE(SUM(CASE WHEN status = 'monitoring' THEN 1 ELSE 0 END),0) AS monitoring, "+
				"COALESCE(SUM(CASE WHEN status = 'kept' THEN 1 ELSE 0 END),0) AS kept, "+
				"COALESCE(SUM(CASE WHEN status = 'rolled_back' THEN 1 ELSE 0 END),0) AS rolled_back",
		).
		Where("created_at >= ?", cutoff).
		Scan(&stats).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	out.Changes24H = stats.Changes
	out.Monitoring24H = stats.Monitoring
	out.Kept24H = stats.Kept
	out.RolledBack24H = stats.RolledBack

	var latest model.AgentStrategyChange
	if err := db.WithContext(ctx).
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&latest).Error; err == nil {
		out.LastDecision = strings.TrimSpace(latest.Decision)
		out.LastReason = strings.TrimSpace(latest.Reason)
		out.LastSubject = strings.TrimSpace(latest.Subject)
		if !latest.UpdatedAt.IsZero() {
			ts := latest.UpdatedAt.UTC().Format(time.RFC3339)
			out.LastUpdatedAt = &ts
		} else if !latest.CreatedAt.IsZero() {
			ts := latest.CreatedAt.UTC().Format(time.RFC3339)
			out.LastUpdatedAt = &ts
		}
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && !strings.Contains(strings.ToLower(err.Error()), "does not exist") {
		return err
	}
	return nil
}

func fillInspectionParamTuning(ctx context.Context, db *gorm.DB, out *inspectionParamTuning) error {
	if out == nil {
		return nil
	}
	if out.WindowHours <= 0 {
		out.WindowHours = 24
	}
	if out.CurrentParams == nil {
		out.CurrentParams = map[string]float64{}
	}
	if out.CurrentEnabled == nil {
		out.CurrentEnabled = map[string]bool{}
	}

	var params []model.AgentStrategyParam
	if err := db.WithContext(ctx).
		Where("scope = ?", "agent_cycle").
		Order("updated_at DESC, created_at DESC").
		Find(&params).Error; err != nil {
		return err
	}
	out.ParamsCount = int64(len(params))
	for _, p := range params {
		key := strings.TrimSpace(p.Key)
		if key == "" {
			continue
		}
		out.CurrentParams[key] = p.Value
		out.CurrentEnabled[key] = p.Enabled
		if p.Enabled {
			out.EnabledParams++
		}
	}

	cutoff := time.Now().UTC().Add(-time.Duration(out.WindowHours) * time.Hour)
	var agg struct {
		Monitoring int64
		Kept       int64
		RolledBack int64
	}
	err := db.WithContext(ctx).Model(&model.AgentStrategyChange{}).
		Select(`
			COUNT(*) FILTER (WHERE status = 'monitoring') AS monitoring,
			COUNT(*) FILTER (WHERE status = 'kept') AS kept,
			COUNT(*) FILTER (WHERE status = 'rolled_back') AS rolled_back`).
		Where("scope = ? AND created_at >= ?", "agent_param_tuning", cutoff).
		Scan(&agg).Error
	if err != nil {
		return err
	}
	out.Monitoring24H = agg.Monitoring
	out.Kept24H = agg.Kept
	out.RolledBack24H = agg.RolledBack

	var latest model.AgentStrategyChange
	if err := db.WithContext(ctx).
		Where("scope = ?", "agent_param_tuning").
		Order("updated_at DESC, id DESC").
		Limit(1).
		Take(&latest).Error; err == nil {
		out.LastDecision = strings.TrimSpace(latest.Decision)
		out.LastReason = strings.TrimSpace(latest.Reason)
		if !latest.UpdatedAt.IsZero() {
			v := latest.UpdatedAt.UTC().Format(time.RFC3339)
			out.LastUpdatedAt = &v
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return nil
}

func fillInspectionPromptRollout(ctx context.Context, db *gorm.DB, out *inspectionPromptRollout) error {
	if out.WindowHours <= 0 {
		out.WindowHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(out.WindowHours) * time.Hour)
	var rows []model.AgentSession
	if err := db.WithContext(ctx).
		Where("started_at >= ?", cutoff).
		Order("started_at DESC").
		Limit(10000).
		Find(&rows).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	var activeSignals, candidateSignals int64
	for _, row := range rows {
		variant := strings.TrimSpace(strings.ToLower(row.PromptVariant))
		if variant == "" {
			variant = "active"
		}
		signals := extractSignalsGeneratedFromSummaryForInspection(row.SummaryJSON)
		switch variant {
		case "candidate":
			out.CandidateRuns++
			candidateSignals += signals
		default:
			out.ActiveRuns++
			activeSignals += signals
		}
	}
	if out.ActiveRuns > 0 {
		out.ActiveSignalsPerRun = roundTo(float64(activeSignals)/float64(out.ActiveRuns), 4)
	}
	if out.CandidateRuns > 0 {
		out.CandidateSignalsPerRun = roundTo(float64(candidateSignals)/float64(out.CandidateRuns), 4)
	}
	if out.ActiveSignalsPerRun > 0 {
		out.ImprovementPct = roundTo((out.CandidateSignalsPerRun-out.ActiveSignalsPerRun)/out.ActiveSignalsPerRun*100.0, 2)
	}
	return nil
}

func fillInspectionEventPipeline(ctx context.Context, db *gorm.DB, out *inspectionEventPipeline) error {
	if out.WindowHours <= 0 {
		out.WindowHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(out.WindowHours) * time.Hour)
	if err := db.WithContext(ctx).Table("opportunity_events").Where("occurred_at >= ?", cutoff).Count(&out.TotalEvents24).Error; err != nil {
		// table may not exist on old envs before migration; keep backward-compatible
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	if err := db.WithContext(ctx).Table("opportunity_events").Where("status = ?", "pending").Count(&out.PendingEvents).Error; err != nil {
		return err
	}
	var t sql.NullTime
	if err := db.WithContext(ctx).Table("opportunity_events").Select("MAX(occurred_at)").Scan(&t).Error; err != nil {
		return err
	}
	if t.Valid && !t.Time.IsZero() {
		s := t.Time.UTC().Format(time.RFC3339)
		out.LastEventAt = &s
	}

	type cycleAgg struct {
		Total        int64 `gorm:"column:total"`
		Success      int64 `gorm:"column:success"`
		Degraded     int64 `gorm:"column:degraded"`
		Error        int64 `gorm:"column:error"`
		SignalsTotal int64 `gorm:"column:signals_total"`
	}
	var agg cycleAgg
	if err := db.WithContext(ctx).
		Table("agent_event_cycles").
		Select(
			"COUNT(*) AS total, "+
				"COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),0) AS success, "+
				"COALESCE(SUM(CASE WHEN status = 'degraded' THEN 1 ELSE 0 END),0) AS degraded, "+
				"COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END),0) AS error, "+
				"COALESCE(SUM(signals_generated),0) AS signals_total",
		).
		Where("started_at >= ?", cutoff).
		Scan(&agg).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	out.AgentCycles24 = agg.Total
	out.AgentCycleSuccess24 = agg.Success
	out.AgentCycleDegraded24 = agg.Degraded
	out.AgentCycleError24 = agg.Error
	out.SignalsFromEventCycles24 = agg.SignalsTotal

	type latencyRow struct {
		LatencySeconds float64 `gorm:"column:latency_seconds"`
	}
	latencies := make([]latencyRow, 0, 64)
	if err := db.WithContext(ctx).
		Table("agent_event_cycles").
		Select("EXTRACT(EPOCH FROM (first_signal_at - first_event_at)) AS latency_seconds").
		Where("started_at >= ?", cutoff).
		Where("first_event_at IS NOT NULL AND first_signal_at IS NOT NULL").
		Where("first_signal_at >= first_event_at").
		Scan(&latencies).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	if len(latencies) > 0 {
		values := make([]float64, 0, len(latencies))
		sum := 0.0
		for _, row := range latencies {
			if row.LatencySeconds < 0 {
				continue
			}
			values = append(values, row.LatencySeconds)
			sum += row.LatencySeconds
		}
		if len(values) > 0 {
			sort.Float64s(values)
			out.TriggerToSignalLatencyAvgSecond = roundTo(sum/float64(len(values)), 2)
			out.TriggerToSignalLatencyP50Second = roundTo(percentile(values, 0.5), 2)
			out.TriggerToSignalLatencyP90Second = roundTo(percentile(values, 0.9), 2)
		}
	}
	return nil
}

func fillInspectionCandidateFunnel(ctx context.Context, db *gorm.DB, out *inspectionCandidateFunnel) error {
	type row struct {
		State string `gorm:"column:state"`
		Count int64  `gorm:"column:count"`
	}
	var rows []row
	if err := db.WithContext(ctx).
		Table("candidate_markets").
		Select("state, COUNT(*) AS count").
		Group("state").
		Scan(&rows).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	for _, r := range rows {
		switch strings.TrimSpace(strings.ToLower(r.State)) {
		case "cold":
			out.Cold = r.Count
		case "watch":
			out.Watch = r.Count
		case "hot":
			out.Hot = r.Count
		case "tradable":
			out.Tradable = r.Count
		case "cool_down":
			out.CoolDown = r.Count
		}
	}
	return nil
}

func fillInspectionOpportunityWindow(ctx context.Context, db *gorm.DB, out *inspectionOpportunityWin) error {
	if out.WindowHours <= 0 {
		out.WindowHours = 24
	}
	cutoff := time.Now().UTC().Add(-time.Duration(out.WindowHours) * time.Hour)
	type agg struct {
		Total        int64 `gorm:"column:total"`
		Triggered    int64 `gorm:"column:triggered"`
		WithSignals  int64 `gorm:"column:with_signals"`
		SignalsTotal int64 `gorm:"column:signals_total"`
	}
	var row agg
	if err := db.WithContext(ctx).
		Table("agent_event_cycles").
		Select(
			"COUNT(*) AS total, "+
				"COALESCE(SUM(CASE WHEN events_consumed > 0 THEN 1 ELSE 0 END),0) AS triggered, "+
				"COALESCE(SUM(CASE WHEN signals_generated > 0 THEN 1 ELSE 0 END),0) AS with_signals, "+
				"COALESCE(SUM(signals_generated),0) AS signals_total",
		).
		Where("started_at >= ?", cutoff).
		Scan(&row).Error; err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	out.TotalCycles24 = row.Total
	out.TriggeredCycles24 = row.Triggered
	out.CyclesWithSignals24 = row.WithSignals
	if row.Triggered > 0 {
		out.SignalHitRate24 = roundTo(float64(row.WithSignals)/float64(row.Triggered), 4)
		out.AvgSignalsPerTriggered = roundTo(float64(row.SignalsTotal)/float64(row.Triggered), 4)
	}
	return nil
}

func fillInspectionVertexBrainBudget(ctx context.Context, db *gorm.DB, out *inspectionVertexBrainBudget) error {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	cutoff1h := time.Now().UTC().Add(-1 * time.Hour)
	var rows []model.AgentSession
	if err := db.WithContext(ctx).
		Where("started_at >= ?", cutoff).
		Where("LOWER(run_mode) IN ?", []string{"vertex_brain", "event_driven", "vertex-controlled", "vertex_controlled"}).
		Order("started_at DESC").
		Find(&rows).Error; err != nil {
		return err
	}
	out.VertexRuns24 = int64(len(rows))
	if len(rows) == 0 {
		return nil
	}
	var sumCalls int64
	var sumTokens int64
	var maxTokens int64
	var lastDecodeAt time.Time
	for _, row := range rows {
		sumCalls += int64(row.LLMCalls)
		sumTokens += int64(row.LLMTokens)
		if int64(row.LLMTokens) > maxTokens {
			maxTokens = int64(row.LLMTokens)
		}
		for _, code := range extractWarningCodesFromSummary(row.SummaryJSON) {
			switch strings.ToLower(strings.TrimSpace(code)) {
			case "token_budget_exhausted":
				out.TokenBudgetExhausted24++
			case "cycle_duration_exhausted":
				out.DurationBudgetExceeded24++
			case "external_budget_exhausted":
				out.ExternalBudgetExhausted24++
			case "tool_budget_exhausted":
				out.ToolBudgetExhausted24++
			case "plan_decode_failed":
				out.PlanDecodeFailed24++
				if row.StartedAt.After(cutoff1h) {
					out.PlanDecodeFailed1H++
				}
				if row.StartedAt.After(lastDecodeAt) {
					lastDecodeAt = row.StartedAt
				}
			}
		}
	}
	if !lastDecodeAt.IsZero() {
		s := lastDecodeAt.UTC().Format(time.RFC3339)
		out.LastPlanDecodeFailedAt = &s
	}
	out.AvgLLMCalls = roundTo(float64(sumCalls)/float64(len(rows)), 2)
	out.AvgLLMTokens = roundTo(float64(sumTokens)/float64(len(rows)), 2)
	out.MaxLLMTokens = maxTokens
	pressure := out.TokenBudgetExhausted24 + out.DurationBudgetExceeded24 + out.ExternalBudgetExhausted24 + out.ToolBudgetExhausted24
	out.BudgetPressureRate24 = roundTo(float64(pressure)/float64(len(rows)), 4)
	return nil
}

func fillInspectionControlPlane(ctx context.Context, db *gorm.DB, out *inspectionControlPlane) error {
	if out == nil {
		return nil
	}
	out.Mode = "vertex_brain"
	out.LegacyVertexAnalyzeEnabled = strings.EqualFold(strings.TrimSpace(getEnvOrDefault("SKY_ALPHA_AGENT_ALLOW_LEGACY_VERTEX_ANALYZE", "false")), "true")

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	if err := db.WithContext(ctx).
		Table("agent_logs").
		Where("created_at >= ?", cutoff).
		Where("action = ?", "analyze").
		Where("model <> ''").
		Where("model <> ?", "rule-based-fallback-v1").
		Count(&out.LegacyVertexAnalyzeBypass24H).Error; err != nil {
		return err
	}
	return nil
}

func getEnvOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(strings.TrimSpace(key)))
	if v == "" {
		return fallback
	}
	return v
}

func fillSpecFillTrend(ctx context.Context, db *gorm.DB, out *inspectionSpecFillTrend) error {
	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	type runRow struct {
		StartedAt time.Time `gorm:"column:started_at"`
		Status    string    `gorm:"column:status"`
	}
	var rows []runRow
	if err := db.WithContext(ctx).
		Table("scheduler_runs").
		Select("started_at, status").
		Where("job_name = ? AND started_at >= ?", "market_spec_fill", cutoff).
		Order("started_at ASC").
		Scan(&rows).Error; err != nil {
		return err
	}

	hourlyMap := make(map[string]*inspectionSpecFillHourAgg, 24)
	for _, row := range rows {
		out.Runs24H++
		hourKey := row.StartedAt.UTC().Format("2006-01-02T15:00:00Z")
		hour, ok := hourlyMap[hourKey]
		if !ok {
			hour = &inspectionSpecFillHourAgg{Hour: hourKey}
			hourlyMap[hourKey] = hour
		}
		hour.Runs++

		status := strings.ToLower(strings.TrimSpace(row.Status))
		switch {
		case status == "success":
			out.Success24H++
			out.EffectiveSuccess24H++
			hour.Success++
			hour.EffectiveSuccess++
		case status == "error" || status == "timeout":
			out.Error24H++
			hour.Error++
		case strings.HasPrefix(status, "skipped"):
			out.Skipped24H++
			hour.Skipped++
			if strings.TrimSpace(row.Status) == "skipped_no_input" {
				out.NoProgress24H++
				hour.NoProgress++
			}
		default:
			out.Error24H++
			hour.Error++
		}
	}
	if out.Runs24H > 0 {
		out.SuccessRate24H = roundTo(float64(out.Success24H)/float64(out.Runs24H), 4)
		out.EffectiveSuccessRate24H = roundTo(float64(out.EffectiveSuccess24H)/float64(out.Runs24H), 4)
	}
	out.Hourly = out.Hourly[:0]
	for _, item := range hourlyMap {
		if item.Runs > 0 {
			item.SuccessRate = roundTo(float64(item.Success)/float64(item.Runs), 4)
			item.EffectiveSuccessRate = roundTo(float64(item.EffectiveSuccess)/float64(item.Runs), 4)
		}
		out.Hourly = append(out.Hourly, *item)
	}
	sort.Slice(out.Hourly, func(i, j int) bool {
		return out.Hourly[i].Hour < out.Hourly[j].Hour
	})
	return nil
}

func fillSpecFillDiagnostics(ctx context.Context, db *gorm.DB, out *inspectionSpecFillDiag) error {
	var rows []model.Market
	if err := db.WithContext(ctx).Model(&model.Market{}).
		Select("id, market_type, question, city, spec_status, threshold_f, comparator, weather_target_date").
		Where("is_active = ?", true).
		Find(&rows).Error; err != nil {
		return err
	}
	out.ActiveMarkets = int64(len(rows))
	out.BySpecStatus = make(map[string]int64, 8)
	unsupportedReasons := make(map[string]int64, 8)
	for _, row := range rows {
		reason := signal.UnsupportedReasonForSignal(row)
		if reason != "" {
			unsupportedReasons[reason]++
			continue
		}
		out.SupportedMarkets++
		if strings.TrimSpace(row.City) == "" {
			out.CityMissing++
		}
		if !row.ThresholdF.Valid {
			out.ThresholdMissing++
		}
		if strings.TrimSpace(row.Comparator) == "" {
			out.ComparatorMissing++
		}
		if row.WeatherTargetDate == nil || row.WeatherTargetDate.IsZero() {
			out.TargetDateMissing++
		}
		status := strings.TrimSpace(row.SpecStatus)
		if strings.EqualFold(status, "ready") {
			out.SpecReady++
		}
		key := status
		if key == "" {
			key = "unknown"
		}
		out.BySpecStatus[key]++
	}
	reasonRows := make([]inspectionGroupKV, 0, len(unsupportedReasons))
	for reason, count := range unsupportedReasons {
		reasonRows = append(reasonRows, inspectionGroupKV{
			Key1:  reason,
			Count: count,
		})
	}
	sort.Slice(reasonRows, func(i, j int) bool {
		if reasonRows[i].Count == reasonRows[j].Count {
			return reasonRows[i].Key1 < reasonRows[j].Key1
		}
		return reasonRows[i].Count > reasonRows[j].Count
	})
	if len(reasonRows) > 5 {
		reasonRows = reasonRows[:5]
	}
	out.UnsupportedTopReasons = reasonRows
	return nil
}

func calcWinRate(ctx context.Context, db *gorm.DB, cutoff time.Time) float64 {
	var total int64
	if err := db.WithContext(ctx).Table("trades").Where("created_at >= ? AND UPPER(status) = ?", cutoff, "SETTLED").Count(&total).Error; err != nil || total == 0 {
		return 0
	}
	var wins int64
	_ = db.WithContext(ctx).Table("trades").Where("created_at >= ? AND UPPER(status) = ? AND pnl_usdc > 0", cutoff, "SETTLED").Count(&wins).Error
	return roundTo(100.0*float64(wins)/float64(total), 2)
}

func loadSignalEdges(ctx context.Context, db *gorm.DB, cutoff time.Time) ([]float64, error) {
	type edgeRow struct {
		Edge float64 `gorm:"column:edge_exec_pct"`
	}
	var rows []edgeRow
	if err := db.WithContext(ctx).Table("signals").
		Select("edge_exec_pct").
		Where("created_at >= ?", cutoff).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Edge)
	}
	return out, nil
}

func calcMaxDrawdown30D(ctx context.Context, db *gorm.DB, cutoff time.Time) (float64, error) {
	type dailyRow struct {
		Day string              `gorm:"column:day"`
		PnL decimal.NullDecimal `gorm:"column:pnl"`
	}
	var rows []dailyRow
	if err := db.WithContext(ctx).Table("trades").
		Select("DATE(created_at) AS day, COALESCE(SUM(pnl_usdc),0) AS pnl").
		Where("created_at >= ? AND UPPER(status) = ?", cutoff, "SETTLED").
		Group("DATE(created_at)").
		Order("day ASC").
		Scan(&rows).Error; err != nil {
		return 0, err
	}
	cum := 0.0
	peak := 0.0
	maxDD := 0.0
	for _, row := range rows {
		p := 0.0
		if row.PnL.Valid {
			p = row.PnL.Decimal.InexactFloat64()
		}
		cum += p
		if cum > peak {
			peak = cum
		}
		dd := cum - peak
		if dd < maxDD {
			maxDD = dd
		}
	}
	return maxDD, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	w := idx - float64(lo)
	return sorted[lo]*(1-w) + sorted[hi]*w
}

func roundTo(v float64, digits int) float64 {
	pow := math.Pow(10, float64(digits))
	return math.Round(v*pow) / pow
}

func concludeProfitability(resp opsInspectionResponse) profitabilityConclusion {
	healthReady := !resp.Summary.Degraded
	valueReady := resp.ValueDashboard.Signals7D > 0 &&
		resp.ValueDashboard.Trades7D > 0 &&
		resp.ValueDashboard.NetPnL30D > 0 &&
		resp.ValueDashboard.WinRate30DPct >= 50

	reasons := make([]string, 0, 4)
	if !healthReady {
		reasons = append(reasons, "system degraded")
	}
	if resp.ValueDashboard.Signals7D == 0 {
		reasons = append(reasons, "signals_7d=0")
	}
	if resp.ValueDashboard.Trades7D == 0 {
		reasons = append(reasons, "trades_7d=0")
	}
	if resp.ValueDashboard.NetPnL30D <= 0 {
		reasons = append(reasons, "net_pnl_30d<=0")
	}
	if resp.ValueDashboard.WinRate30DPct < 50 {
		reasons = append(reasons, "win_rate_30d<50%")
	}
	reason := "health ok and value metrics pass"
	if len(reasons) > 0 {
		reason = strings.Join(reasons, "; ")
	}
	return profitabilityConclusion{
		HealthReady: healthReady,
		ValueReady:  valueReady,
		Reason:      reason,
	}
}

func buildInspectionAlerts(resp opsInspectionResponse) []inspectionAlert {
	out := make([]inspectionAlert, 0, 4)
	if resp.Summary.Degraded {
		out = append(out, inspectionAlert{
			Code:     "system_degraded",
			Severity: "critical",
			Message:  "scheduler summary reports degraded state",
		})
	}
	// Real-time funnel blocker should rely on current market diagnostics, not stale signal_runs snapshots.
	if resp.SpecFillDiag.SupportedMarkets > 0 && resp.SpecFillDiag.SpecReady == 0 {
		out = append(out, inspectionAlert{
			Code:     "signal_funnel_spec_blocked",
			Severity: "critical",
			Message:  "supported markets exist but current spec_ready is 0",
		})
	}
	// Surface stale signal generation separately to avoid confusing stale snapshots with current blocker state.
	if resp.ValueDashboard.LatestMarketsTotal == 0 {
		out = append(out, inspectionAlert{
			Code:     "signal_run_stale",
			Severity: "warning",
			Message:  "no recent signal run snapshot available",
		})
	}
	if resp.SpecFillTrend.NoProgress24H > 0 {
		out = append(out, inspectionAlert{
			Code:     "spec_fill_no_progress",
			Severity: "warning",
			Message:  "market_spec_fill had skipped_no_input runs in last 24h",
		})
	}
	if resp.ValueDashboard.Signals7D == 0 {
		out = append(out, inspectionAlert{
			Code:     "signals_zero_7d",
			Severity: "warning",
			Message:  "no signals generated in the last 7 days",
		})
	}
	if resp.ValueDashboard.Trades7D == 0 {
		out = append(out, inspectionAlert{
			Code:     "trades_zero_7d",
			Severity: "warning",
			Message:  "no trades executed in the last 7 days",
		})
	}
	if resp.StrategyEvolution.RolledBack24H > 0 {
		out = append(out, inspectionAlert{
			Code:     "strategy_auto_rollback",
			Severity: "warning",
			Message:  "auto rollback triggered in strategy evolution within last 24h",
		})
	}
	if resp.StrategyEvolution.CandidateRolloutPct > 0 && resp.StrategyEvolution.Changes24H == 0 {
		out = append(out, inspectionAlert{
			Code:     "strategy_rollout_untracked",
			Severity: "warning",
			Message:  "candidate rollout is active but no strategy change records in last 24h",
		})
	}
	if resp.PromptRollout.CandidateRuns >= 3 &&
		resp.PromptRollout.ActiveRuns >= 3 &&
		resp.PromptRollout.CandidateSignalsPerRun < resp.PromptRollout.ActiveSignalsPerRun {
		out = append(out, inspectionAlert{
			Code:     "prompt_candidate_underperforming",
			Severity: "warning",
			Message:  "candidate prompt performs worse than active on signals_per_run in current window",
		})
	}
	if resp.StrategyEvolution.CandidateRolloutPct > 0 && resp.PromptRollout.CandidateRuns == 0 {
		out = append(out, inspectionAlert{
			Code:     "prompt_rollout_no_sample",
			Severity: "warning",
			Message:  "candidate rollout enabled but no candidate runs observed in current window",
		})
	}
	if resp.OpportunityWin.TriggeredCycles24 > 0 && resp.OpportunityWin.CyclesWithSignals24 == 0 {
		out = append(out, inspectionAlert{
			Code:     "opportunity_windows_no_signal",
			Severity: "warning",
			Message:  "opportunity cycles were triggered in 24h but no signals were generated",
		})
	}
	if resp.VertexBrainBudget.BudgetPressureRate24 >= 0.2 {
		out = append(out, inspectionAlert{
			Code:     "vertex_budget_pressure",
			Severity: "warning",
			Message:  "vertex brain cycles show high budget pressure in last 24h",
		})
	}
	if resp.VertexBrainBudget.PlanDecodeFailed1H > 0 {
		out = append(out, inspectionAlert{
			Code:     "vertex_plan_decode_failed",
			Severity: "warning",
			Message:  "vertex plan/action decode failures observed in last 1h",
		})
	}
	if !resp.ControlPlane.LegacyVertexAnalyzeEnabled && resp.ControlPlane.LegacyVertexAnalyzeBypass24H > 0 {
		out = append(out, inspectionAlert{
			Code:     "control_plane_bypass_detected",
			Severity: "warning",
			Message:  "legacy vertex analyze bypass calls detected in last 24h",
		})
	}
	return out
}

func buildInspectionActionPlan(resp opsInspectionResponse) ([]string, []string) {
	immediate := make([]string, 0, 6)
	h48 := make([]string, 0, 6)
	if resp.SpecFillDiag.SupportedMarkets > 0 && resp.SpecFillDiag.SpecReady == 0 {
		immediate = append(immediate, "P0: 排查 market_spec_fill 与 city/spec 解析链路，先让 spec_ready > 0。")
	}
	if resp.StrategyEvolution.RolledBack24H > 0 {
		immediate = append(immediate, "P1: 查看 /ops/agent/strategy-changes 最近回滚原因，修正 candidate prompt 后再小流量灰度。")
	}
	if resp.PromptRollout.CandidateRuns >= 3 &&
		resp.PromptRollout.ActiveRuns >= 3 &&
		resp.PromptRollout.CandidateSignalsPerRun < resp.PromptRollout.ActiveSignalsPerRun {
		immediate = append(immediate, "P1: candidate prompt 当前劣化，建议继续降 rollout 或暂停 candidate。")
	}
	if resp.ValueDashboard.Signals7D == 0 {
		immediate = append(immediate, "P1: 近7天无信号，优先检查 candidate_funnel 与 signal_runs.skip_reasons。")
	}
	if resp.VertexBrainBudget.PlanDecodeFailed1H > 0 {
		immediate = append(immediate, "P1: 存在 plan_decode_failed，检查 prompt 输出约束与 max_tokens_per_cycle 配置。")
	}
	if !resp.ControlPlane.LegacyVertexAnalyzeEnabled && resp.ControlPlane.LegacyVertexAnalyzeBypass24H > 0 {
		immediate = append(immediate, "P1: 检测到 legacy analyze 旁路 Vertex 调用，需统一收口到 vertex_brain control plane。")
	}
	if resp.VertexBrainBudget.BudgetPressureRate24 >= 0.2 {
		immediate = append(immediate, "P1: Vertex 预算压力偏高，收敛单轮 tool 数并提高 token/duration 预算上限。")
	}
	if resp.ValueDashboard.Trades7D == 0 && resp.ValueDashboard.Signals7D > 0 {
		immediate = append(immediate, "P2: 有信号无交易，检查 exec_edge 门限、流动性门槛与交易风控。")
	}

	if resp.SpecFillDiag.SpecReady > 0 {
		h48 = append(h48, "48h: 观察 spec_ready/forecast_ready 是否持续提升，避免漏斗回退到 blocked。")
	}
	if resp.ValueDashboard.Signals7D == 0 || resp.ValueDashboard.Trades7D == 0 {
		h48 = append(h48, "48h: 按 6h 粒度跟踪 signals/trades/net_pnl，确认是否从“可运行”走向“可盈利”。")
	}
	if resp.PromptRollout.CandidateRuns > 0 {
		h48 = append(h48, "48h: 对比 prompt rollout active/candidate 的 signals_per_run 与 error_rate，决定 keep/rollback。")
	}
	if resp.OpportunityWin.TriggeredCycles24 > 0 && resp.OpportunityWin.CyclesWithSignals24 == 0 {
		h48 = append(h48, "48h: 复盘 opportunity_windows 到 signals 的转化链路，定位触发有效性与门限设置。")
	}
	if resp.VertexBrainBudget.VertexRuns24 > 0 {
		h48 = append(h48, "48h: 持续跟踪 vertex_brain_budget，确保 budget_pressure_rate 与 decode_failures 持续下降。")
	}

	if len(immediate) == 0 {
		immediate = append(immediate, "系统运行正常，当前无需紧急处置。")
	}
	if len(h48) == 0 {
		h48 = append(h48, "48h: 继续常规巡检，关注 signals/trades/net_pnl 与关键错误码变化。")
	}
	return immediate, h48
}

func extractWarningCodesFromSummary(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	rawWarnings, ok := obj["warnings"]
	if !ok {
		return nil
	}
	items, ok := rawWarnings.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		code, _ := m["code"].(string)
		code = strings.TrimSpace(code)
		if code != "" {
			out = append(out, code)
		}
	}
	return out
}

func extractSignalsGeneratedFromSummaryForInspection(raw []byte) int64 {
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
	v, ok := funnel["signals_generated"]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}
