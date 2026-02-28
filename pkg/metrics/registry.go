package metrics

import (
	"net/http"
	"strings"
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

	jobRunsTotal            *prometheus.CounterVec
	jobDurationSeconds      *prometheus.HistogramVec
	jobInflight             *prometheus.GaugeVec
	jobLastSuccessTimestamp *prometheus.GaugeVec
	jobLastErrorTimestamp   *prometheus.GaugeVec
	jobNextRunTimestamp     *prometheus.GaugeVec
	fetchRecordsTotal       *prometheus.CounterVec
	dataFreshnessSeconds    *prometheus.GaugeVec
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
		fetchRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "sky_alpha",
			Subsystem: "fetch",
			Name:      "records_total",
			Help:      "Fetched records by job/entity/result.",
		}, []string{"job", "entity", "result"}),
		dataFreshnessSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "sky_alpha",
			Subsystem: "data",
			Name:      "freshness_seconds",
			Help:      "Dataset freshness in seconds.",
		}, []string{"dataset"}),
	}

	r.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.jobRunsTotal,
		r.jobDurationSeconds,
		r.jobInflight,
		r.jobLastSuccessTimestamp,
		r.jobLastErrorTimestamp,
		r.jobNextRunTimestamp,
		r.fetchRecordsTotal,
		r.dataFreshnessSeconds,
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

func (r *Registry) AddFetchRecords(job, entity, result string, n int) {
	if !r.Enabled() || n <= 0 {
		return
	}
	r.fetchRecordsTotal.WithLabelValues(job, entity, result).Add(float64(n))
}

func (r *Registry) SetDataFreshness(dataset string, seconds float64) {
	if !r.Enabled() {
		return
	}
	r.dataFreshnessSeconds.WithLabelValues(dataset).Set(seconds)
}
