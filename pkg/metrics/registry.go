package metrics

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sky-alpha-pro/pkg/config"
)

type Registry struct {
	enabled bool
	path    string
	reg     *prometheus.Registry

	jobRunsTotal                 *prometheus.CounterVec
	jobDurationSeconds           *prometheus.HistogramVec
	jobErrorsTotal               *prometheus.CounterVec
	jobInflight                  *prometheus.GaugeVec
	jobLastSuccessTimestamp      *prometheus.GaugeVec
	jobLastErrorTimestamp        *prometheus.GaugeVec
	jobNextRunTimestamp          *prometheus.GaugeVec
	jobConsecutiveFailures       *prometheus.GaugeVec
	schedulerConsecutiveFailures *prometheus.GaugeVec
	fetchRecordsTotal            *prometheus.CounterVec
	dataFreshnessSeconds         *prometheus.GaugeVec
	fetchDataFreshnessSeconds    *prometheus.GaugeVec
	chainRPCRequestsTotal        *prometheus.CounterVec
	chainRPCDurationSeconds      *prometheus.HistogramVec
	chainRPCRequestsPerSec       *prometheus.GaugeVec
	agentCycleRunsTotal          *prometheus.CounterVec
	agentCycleDuration           *prometheus.HistogramVec
	agentCycleLLMCalls           prometheus.Histogram
	agentCycleToolErrors         *prometheus.CounterVec
	agentCycleFallbackTotal      *prometheus.CounterVec
	agentMemoryHitTotal          prometheus.Counter
	marketSpecReadyTotal         prometheus.Gauge
	marketCityMissingTotal       prometheus.Gauge
	marketSpecFillSuccessRate    prometheus.Gauge

	cacheMu               sync.RWMutex
	dataFreshnessSnapshot map[string]float64
	chainRPCSecState      map[string]rpcSecState
}

type rpcSecState struct {
	second int64
	count  int
}

func New(cfg config.MetricsConfig) *Registry {
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}
	if !cfg.Enabled {
		return &Registry{enabled: false, path: path}
	}

	r := &Registry{
		enabled: cfg.Enabled,
		path:    path,
		reg:     prometheus.NewRegistry(),
		jobRunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_runs_total",
			Help:      "Total scheduler job runs grouped by status.",
		}, []string{"job", "status"}),
		jobDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_duration_seconds",
			Help:      "Scheduler job run duration in seconds grouped by status.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120},
		}, []string{"job", "status"}),
		jobErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_errors_total",
			Help:      "Structured scheduler job errors grouped by error_code.",
		}, []string{"job", "error_code"}),
		jobInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_inflight",
			Help:      "Current inflight count for each scheduler job.",
		}, []string{"job"}),
		jobLastSuccessTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_last_success_timestamp_seconds",
			Help:      "Unix timestamp of the latest successful run for each scheduler job.",
		}, []string{"job"}),
		jobLastErrorTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_last_error_timestamp_seconds",
			Help:      "Unix timestamp of the latest failed run for each scheduler job.",
		}, []string{"job"}),
		jobNextRunTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_next_run_timestamp_seconds",
			Help:      "Unix timestamp of the next scheduled run for each scheduler job.",
		}, []string{"job"}),
		jobConsecutiveFailures: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "job_consecutive_failures",
			Help:      "Consecutive failure count for each scheduler job.",
		}, []string{"job"}),
		schedulerConsecutiveFailures: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "scheduler",
			Name:      "consecutive_failures",
			Help:      "Deprecated alias of scheduler job consecutive failures.",
		}, []string{"job"}),
		fetchRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "fetch",
			Name:      "records_total",
			Help:      "Fetched records by job/entity/result.",
		}, []string{"job", "entity", "result"}),
		chainRPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "chain",
			Name:      "rpc_requests_total",
			Help:      "Total chain RPC requests grouped by method and status.",
		}, []string{"method", "status"}),
		chainRPCDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sky_alpha",
			Subsystem: "chain",
			Name:      "rpc_duration_seconds",
			Help:      "Chain RPC request duration in seconds grouped by method and status.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}, []string{"method", "status"}),
		chainRPCRequestsPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "chain",
			Name:      "rpc_requests_per_second",
			Help:      "In-process per-second chain RPC request count grouped by method and status.",
		}, []string{"method", "status"}),
		agentCycleRunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "cycle_runs_total",
			Help:      "Total agent cycle runs grouped by status and decision.",
		}, []string{"status", "decision"}),
		agentCycleDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "cycle_duration_seconds",
			Help:      "Agent cycle duration in seconds grouped by status.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120},
		}, []string{"status"}),
		agentCycleLLMCalls: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "cycle_llm_calls",
			Help:      "LLM calls per agent cycle.",
			Buckets:   []float64{0, 1, 2, 3, 5, 8},
		}),
		agentCycleToolErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "cycle_tool_errors_total",
			Help:      "Agent cycle tool errors grouped by tool and error_code.",
		}, []string{"tool", "error_code"}),
		agentCycleFallbackTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "cycle_fallback_total",
			Help:      "Fallback planning count grouped by reason.",
		}, []string{"reason"}),
		agentMemoryHitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "agent",
			Name:      "memory_hit_total",
			Help:      "Total memory summaries injected into agent cycle context.",
		}),
		marketSpecReadyTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "signal",
			Name:      "markets_spec_ready_total",
			Help:      "Active markets with spec_status=ready.",
		}),
		marketCityMissingTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "signal",
			Name:      "markets_city_missing_total",
			Help:      "Active markets with missing city.",
		}),
		marketSpecFillSuccessRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "signal",
			Name:      "spec_fill_success_rate",
			Help:      "Spec fill success rate among active markets (0~1).",
		}),
		dataFreshnessSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "data",
			Name:      "freshness_seconds",
			Help:      "Dataset freshness in seconds.",
		}, []string{"dataset"}),
		fetchDataFreshnessSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "fetch",
			Name:      "data_freshness_seconds",
			Help:      "Deprecated alias of dataset freshness in seconds.",
		}, []string{"dataset"}),
		dataFreshnessSnapshot: make(map[string]float64),
		chainRPCSecState:      make(map[string]rpcSecState),
	}

	r.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.jobRunsTotal,
		r.jobDurationSeconds,
		r.jobErrorsTotal,
		r.jobInflight,
		r.jobLastSuccessTimestamp,
		r.jobLastErrorTimestamp,
		r.jobNextRunTimestamp,
		r.jobConsecutiveFailures,
		r.schedulerConsecutiveFailures,
		r.fetchRecordsTotal,
		r.chainRPCRequestsTotal,
		r.chainRPCDurationSeconds,
		r.chainRPCRequestsPerSec,
		r.agentCycleRunsTotal,
		r.agentCycleDuration,
		r.agentCycleLLMCalls,
		r.agentCycleToolErrors,
		r.agentCycleFallbackTotal,
		r.agentMemoryHitTotal,
		r.marketSpecReadyTotal,
		r.marketCityMissingTotal,
		r.marketSpecFillSuccessRate,
		r.dataFreshnessSeconds,
		r.fetchDataFreshnessSeconds,
	)
	return r
}

func (r *Registry) Enabled() bool {
	return r != nil && r.enabled
}

func (r *Registry) Path() string {
	path := "/metrics"
	if r != nil && r.path != "" {
		path = r.path
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func (r *Registry) Handler() http.Handler {
	if !r.Enabled() {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

func (r *Registry) ObserveJobRun(job, status string, duration time.Duration) {
	if !r.Enabled() {
		return
	}
	r.jobRunsTotal.WithLabelValues(job, status).Inc()
	r.jobDurationSeconds.WithLabelValues(job, status).Observe(duration.Seconds())
}

func (r *Registry) SetJobInflight(job string, n float64) {
	if !r.Enabled() {
		return
	}
	r.jobInflight.WithLabelValues(job).Set(n)
}

func (r *Registry) SetJobLastSuccess(job string, at time.Time) {
	if !r.Enabled() {
		return
	}
	r.jobLastSuccessTimestamp.WithLabelValues(job).Set(float64(at.Unix()))
}

func (r *Registry) SetJobLastError(job string, at time.Time) {
	if !r.Enabled() {
		return
	}
	r.jobLastErrorTimestamp.WithLabelValues(job).Set(float64(at.Unix()))
}

func (r *Registry) SetJobNextRun(job string, at time.Time) {
	if !r.Enabled() {
		return
	}
	r.jobNextRunTimestamp.WithLabelValues(job).Set(float64(at.Unix()))
}

func (r *Registry) InitSchedulerJob(job string) {
	if !r.Enabled() {
		return
	}
	name := strings.TrimSpace(job)
	if name == "" {
		return
	}
	r.jobInflight.WithLabelValues(name).Set(0)
	r.jobConsecutiveFailures.WithLabelValues(name).Set(0)
	r.schedulerConsecutiveFailures.WithLabelValues(name).Set(0)
	r.jobRunsTotal.WithLabelValues(name, "success").Add(0)
	r.jobRunsTotal.WithLabelValues(name, "error").Add(0)
	r.jobRunsTotal.WithLabelValues(name, "timeout").Add(0)
	r.jobRunsTotal.WithLabelValues(name, "skipped_no_input").Add(0)
	r.jobErrorsTotal.WithLabelValues(name, "unknown_error").Add(0)
}

func (r *Registry) SetJobConsecutiveFailures(job string, n float64) {
	if !r.Enabled() {
		return
	}
	r.jobConsecutiveFailures.WithLabelValues(job).Set(n)
	r.schedulerConsecutiveFailures.WithLabelValues(job).Set(n)
}

func (r *Registry) AddJobError(job, errorCode string, n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	code := strings.TrimSpace(errorCode)
	if code == "" {
		code = "unknown_error"
	}
	r.jobErrorsTotal.WithLabelValues(job, code).Add(float64(n))
}

func (r *Registry) AddFetchRecords(job, entity, result string, n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	r.fetchRecordsTotal.WithLabelValues(job, entity, result).Add(float64(n))
}

func (r *Registry) AddChainRPCRequest(method string, status string, duration time.Duration) {
	if !r.Enabled() {
		return
	}
	m := strings.TrimSpace(method)
	if m == "" {
		m = "unknown"
	}
	s := strings.TrimSpace(status)
	if s == "" {
		s = "unknown"
	}

	r.chainRPCRequestsTotal.WithLabelValues(m, s).Inc()
	r.chainRPCDurationSeconds.WithLabelValues(m, s).Observe(duration.Seconds())

	key := m + "|" + s
	nowSec := time.Now().Unix()
	r.cacheMu.Lock()
	state := r.chainRPCSecState[key]
	if state.second != nowSec {
		state.second = nowSec
		state.count = 0
	}
	state.count++
	r.chainRPCSecState[key] = state
	r.cacheMu.Unlock()
	r.chainRPCRequestsPerSec.WithLabelValues(m, s).Set(float64(state.count))
}

func (r *Registry) SetDataFreshness(dataset string, seconds float64) {
	if !r.Enabled() {
		return
	}
	r.dataFreshnessSeconds.WithLabelValues(dataset).Set(seconds)
	r.fetchDataFreshnessSeconds.WithLabelValues(dataset).Set(seconds)
	r.cacheMu.Lock()
	r.dataFreshnessSnapshot[dataset] = seconds
	r.cacheMu.Unlock()
}

func (r *Registry) ObserveAgentCycle(status, decision string, duration time.Duration, llmCalls int) {
	if !r.Enabled() {
		return
	}
	s := strings.TrimSpace(status)
	if s == "" {
		s = "unknown"
	}
	d := strings.TrimSpace(decision)
	if d == "" {
		d = "unknown"
	}
	r.agentCycleRunsTotal.WithLabelValues(s, d).Inc()
	r.agentCycleDuration.WithLabelValues(s).Observe(duration.Seconds())
	r.agentCycleLLMCalls.Observe(float64(llmCalls))
}

func (r *Registry) AddAgentCycleToolError(tool, errorCode string, n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	t := strings.TrimSpace(tool)
	if t == "" {
		t = "unknown_tool"
	}
	code := strings.TrimSpace(errorCode)
	if code == "" {
		code = "unknown_error"
	}
	r.agentCycleToolErrors.WithLabelValues(t, code).Add(float64(n))
}

func (r *Registry) AddAgentCycleFallback(reason string, n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	rsn := strings.TrimSpace(reason)
	if rsn == "" {
		rsn = "unknown"
	}
	r.agentCycleFallbackTotal.WithLabelValues(rsn).Add(float64(n))
}

func (r *Registry) AddAgentMemoryHits(n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	r.agentMemoryHitTotal.Add(float64(n))
}

func (r *Registry) SetMarketSpecCoverage(specReady, cityMissing, successRate float64) {
	if !r.Enabled() {
		return
	}
	r.marketSpecReadyTotal.Set(specReady)
	r.marketCityMissingTotal.Set(cityMissing)
	if successRate < 0 {
		successRate = 0
	}
	if successRate > 1 {
		successRate = 1
	}
	r.marketSpecFillSuccessRate.Set(successRate)
}

func (r *Registry) SnapshotDataFreshness() map[string]float64 {
	out := make(map[string]float64)
	if r == nil {
		return out
	}
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()
	for k, v := range r.dataFreshnessSnapshot {
		out[k] = v
	}
	return out
}
