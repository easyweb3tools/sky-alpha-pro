package server

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/pkg/metrics"
)

type opsInspectionResponse struct {
	Timestamp        time.Time                 `json:"timestamp"`
	Scheduler        scheduler.ManagerSnapshot `json:"scheduler"`
	Summary          opsSummary                `json:"summary"`
	Freshness        map[string]float64        `json:"freshness"`
	SchedulerLedger  inspectionLedger          `json:"scheduler_ledger"`
	DataSnapshot     inspectionDataSnapshot    `json:"data_snapshot"`
	ValueDashboard   inspectionValueDashboard  `json:"value_dashboard"`
	SpecFillTrend    inspectionSpecFillTrend   `json:"spec_fill_trend"`
	SpecFillDiag     inspectionSpecFillDiag    `json:"spec_fill_diagnostics"`
	Profitability    profitabilityConclusion   `json:"profitability"`
	ValidationStatus inspectionValidationStats `json:"validation_status"`
	Alerts           []inspectionAlert         `json:"alerts"`
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
	WindowHours    int                         `json:"window_hours"`
	Runs24H        int64                       `json:"runs_24h"`
	Success24H     int64                       `json:"success_24h"`
	Error24H       int64                       `json:"error_24h"`
	Skipped24H     int64                       `json:"skipped_24h"`
	SuccessRate24H float64                     `json:"success_rate_24h"`
	EffectiveSuccess24H     int64              `json:"effective_success_24h"`
	EffectiveSuccessRate24H float64            `json:"effective_success_rate_24h"`
	NoProgress24H           int64              `json:"no_progress_24h"`
	Hourly         []inspectionSpecFillHourAgg `json:"hourly"`
}

type inspectionSpecFillHourAgg struct {
	Hour        string  `json:"hour"`
	Runs        int64   `json:"runs"`
	Success     int64   `json:"success"`
	Error       int64   `json:"error"`
	Skipped     int64   `json:"skipped"`
	SuccessRate float64 `json:"success_rate"`
	EffectiveSuccess     int64   `json:"effective_success"`
	EffectiveSuccessRate float64 `json:"effective_success_rate"`
	NoProgress           int64   `json:"no_progress"`
}

type inspectionSpecFillDiag struct {
	ActiveMarkets       int64            `json:"active_markets"`
	SpecReady           int64            `json:"spec_ready"`
	CityMissing         int64            `json:"city_missing"`
	ThresholdMissing    int64            `json:"threshold_missing"`
	ComparatorMissing   int64            `json:"comparator_missing"`
	TargetDateMissing   int64            `json:"target_date_missing"`
	BySpecStatus        map[string]int64 `json:"by_spec_status"`
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

type inspectionAlert struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
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
				BySpecStatus: map[string]int64{},
			},
			Profitability: profitabilityConclusion{
				HealthReady: false,
				ValueReady:  false,
			},
			Alerts: make([]inspectionAlert, 0),
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
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ?", true).Count(&out.ActiveMarkets).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ? AND spec_status = 'ready'", true).Count(&out.SpecReady).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ? AND COALESCE(city,'') = ''", true).Count(&out.CityMissing).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ? AND threshold_f IS NULL", true).Count(&out.ThresholdMissing).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ? AND COALESCE(comparator,'') = ''", true).Count(&out.ComparatorMissing).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Table("markets").Where("is_active = ? AND weather_target_date IS NULL", true).Count(&out.TargetDateMissing).Error; err != nil {
		return err
	}

	type statusRow struct {
		SpecStatus string `gorm:"column:spec_status"`
		Count      int64  `gorm:"column:count"`
	}
	var rows []statusRow
	if err := db.WithContext(ctx).Table("markets").
		Select("COALESCE(spec_status,'') AS spec_status, COUNT(*) AS count").
		Where("is_active = ?", true).
		Group("COALESCE(spec_status,'')").
		Order("count DESC").
		Scan(&rows).Error; err != nil {
		return err
	}
	out.BySpecStatus = make(map[string]int64, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.SpecStatus)
		if key == "" {
			key = "unknown"
		}
		out.BySpecStatus[key] = row.Count
	}
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
	if resp.ValueDashboard.LatestMarketsTotal > 0 && resp.ValueDashboard.LatestSpecReady == 0 {
		out = append(out, inspectionAlert{
			Code:     "signal_funnel_spec_blocked",
			Severity: "critical",
			Message:  "latest signal run has markets_total>0 but spec_ready=0",
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
	return out
}
