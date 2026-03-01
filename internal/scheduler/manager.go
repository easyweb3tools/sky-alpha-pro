package scheduler

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

const (
	statusSuccess        = "success"
	statusError          = "error"
	statusTimeout        = "timeout"
	statusSkippedLocked  = "skipped_locked"
	statusSkippedNoInput = "skipped_no_input"

	errCodeUnknown           = "unknown_error"
	errCodeSkippedNoInput    = "skipped_no_input"
	errCodeEmptyCitySet      = "empty_city_set"
	errCodeEmptyMarketSet    = "empty_market_set"
	errCodeChainRPCEmpty     = "chain_rpc_empty"
	errCodeUpstream429       = "upstream_429"
	errCodeUpstream5xx       = "upstream_5xx"
	errCodeRPCRange          = "rpc_range_limit"
	errCodeDBWrite           = "db_write_failed"
	errCodeMarketSyncPartial = "market_sync_partial"

	defaultRetryAttempts = 3
)

type FetchRecord struct {
	Entity string
	Result string
	Count  int
}

type JobIssue struct {
	Code    string
	Message string
	Source  string
	Count   int
}

type JobResult struct {
	Records    []FetchRecord
	Freshness  map[string]float64
	Errors     []JobIssue
	Warnings   []JobIssue
	SkipReason string
}

type Job struct {
	Name      string
	Interval  time.Duration
	Timeout   time.Duration
	Immediate bool
	Run       func(ctx context.Context) (JobResult, error)
}

type RunRecord struct {
	JobName        string
	StartedAt      time.Time
	FinishedAt     time.Time
	Duration       time.Duration
	Status         string
	RecordsSuccess int
	RecordsError   int
	RecordsSkipped int
	ErrorCode      string
	ErrorMessage   string
	Meta           map[string]any
}

type RunRecorder interface {
	RecordSchedulerRun(ctx context.Context, rec RunRecord) error
}

type ManagerSnapshot struct {
	Enabled   bool                 `json:"enabled"`
	StartedAt *time.Time           `json:"started_at,omitempty"`
	Jobs      []JobRuntimeSnapshot `json:"jobs"`
}

type JobRuntimeSnapshot struct {
	Name                string     `json:"name"`
	IntervalSeconds     float64    `json:"interval_seconds"`
	TimeoutSeconds      float64    `json:"timeout_seconds"`
	Immediate           bool       `json:"immediate"`
	NextRunAt           *time.Time `json:"next_run_at,omitempty"`
	Inflight            bool       `json:"inflight"`
	LastStatus          string     `json:"last_status,omitempty"`
	LastSuccessAt       *time.Time `json:"last_success_at,omitempty"`
	LastErrorAt         *time.Time `json:"last_error_at,omitempty"`
	LastErrorCode       string     `json:"last_error_code,omitempty"`
	LastErrorMessage    string     `json:"last_error_message,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastDurationMS      int64      `json:"last_duration_ms"`
}

type Manager struct {
	cfg      config.SchedulerConfig
	log      *zap.Logger
	metrics  *metrics.Registry
	recorder RunRecorder
	jobs     []*jobRunner
	jobsMu   sync.Mutex
	wg       sync.WaitGroup
	started  bool
	startAt  time.Time
}

type jobRunner struct {
	name      string
	interval  time.Duration
	timeout   time.Duration
	immediate bool
	jitter    float64
	log       *zap.Logger
	metrics   *metrics.Registry
	run       func(ctx context.Context) (JobResult, error)
	recorder  RunRecorder
	mu        sync.Mutex
	wg        *sync.WaitGroup

	stateMu sync.RWMutex
	state   jobRuntimeState
}

type jobRuntimeState struct {
	nextRunAt           time.Time
	inflight            bool
	lastStatus          string
	lastSuccessAt       time.Time
	lastErrorAt         time.Time
	lastErrorCode       string
	lastErrorMessage    string
	consecutiveFailures int
	lastDuration        time.Duration
}

func NewManager(cfg config.SchedulerConfig, log *zap.Logger, m *metrics.Registry) *Manager {
	if log != nil {
		lockMode := cfg.LockMode
		if lockMode == "" {
			lockMode = "local"
		}
		if lockMode != "local" {
			log.Warn("scheduler lock_mode is not implemented yet, fallback to local single-instance lock",
				zap.String("lock_mode", lockMode),
				zap.Int64("lock_key_prefix", cfg.LockKeyPrefix))
		}
	}
	return &Manager{cfg: cfg, log: log, metrics: m, jobs: make([]*jobRunner, 0)}
}

func (m *Manager) SetRunRecorder(recorder RunRecorder) {
	m.jobsMu.Lock()
	defer m.jobsMu.Unlock()
	m.recorder = recorder
	// Keep existing jobs in sync when recorder is hot-swapped after registrations.
	for _, job := range m.jobs {
		job.recorder = recorder
	}
}

func (m *Manager) Register(job Job) {
	if job.Name == "" || job.Interval <= 0 || job.Run == nil {
		return
	}
	timeout := job.Timeout
	if timeout <= 0 {
		timeout = m.cfg.DefaultTimeout
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	j := &jobRunner{
		name:      job.Name,
		interval:  job.Interval,
		timeout:   timeout,
		immediate: job.Immediate,
		jitter:    m.cfg.DefaultJitterRatio,
		log:       m.log,
		metrics:   m.metrics,
		run:       job.Run,
		recorder:  m.recorder,
		wg:        &m.wg,
	}
	m.jobsMu.Lock()
	m.jobs = append(m.jobs, j)
	m.jobsMu.Unlock()
}

func (m *Manager) Start(ctx context.Context) {
	if !m.cfg.Enabled {
		if m.log != nil {
			m.log.Info("scheduler disabled")
		}
		return
	}

	m.jobsMu.Lock()
	if m.started {
		m.jobsMu.Unlock()
		return
	}
	m.started = true
	m.startAt = time.Now().UTC()
	jobs := make([]*jobRunner, len(m.jobs))
	copy(jobs, m.jobs)
	m.jobsMu.Unlock()

	if m.log != nil {
		m.log.Info("scheduler started", zap.Int("jobs", len(jobs)))
	}
	for _, job := range jobs {
		m.wg.Add(1)
		go func(runner *jobRunner) {
			defer m.wg.Done()
			runner.loop(ctx, m.cfg.RunOnStart)
		}(job)
	}
}

func (m *Manager) Wait() {
	if m == nil {
		return
	}
	m.wg.Wait()
}

func (m *Manager) Snapshot() ManagerSnapshot {
	s := ManagerSnapshot{
		Enabled: m.cfg.Enabled,
		Jobs:    make([]JobRuntimeSnapshot, 0),
	}
	m.jobsMu.Lock()
	jobs := make([]*jobRunner, len(m.jobs))
	copy(jobs, m.jobs)
	started := m.started
	startAt := m.startAt
	m.jobsMu.Unlock()

	if started && !startAt.IsZero() {
		t := startAt
		s.StartedAt = &t
	}
	for _, j := range jobs {
		s.Jobs = append(s.Jobs, j.snapshot())
	}
	sort.Slice(s.Jobs, func(i, k int) bool { return s.Jobs[i].Name < s.Jobs[k].Name })
	return s
}

func (j *jobRunner) loop(ctx context.Context, runOnStart bool) {
	if runOnStart && j.immediate {
		j.trigger(ctx)
	}
	next := j.withJitter(j.interval)
	timer := time.NewTimer(next)
	defer timer.Stop()

	for {
		j.updateNextRun(time.Now().Add(next))
		j.metrics.SetJobNextRun(j.name, time.Now().Add(next))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			j.trigger(ctx)
			next = j.withJitter(j.interval)
			timer.Reset(next)
		}
	}
}

func (j *jobRunner) trigger(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if !j.mu.TryLock() {
		now := time.Now().UTC()
		j.finishRun(now, now, statusSkippedLocked, "", "", JobResult{}, nil)
		return
	}
	if ctx.Err() != nil {
		j.mu.Unlock()
		return
	}

	if j.wg != nil {
		j.wg.Add(1)
	}
	go func() {
		if j.wg != nil {
			defer j.wg.Done()
		}
		defer j.mu.Unlock()
		j.metrics.SetJobInflight(j.name, 1)
		defer j.metrics.SetJobInflight(j.name, 0)

		start := time.Now().UTC()
		j.setInflight(true)
		runCtx := ctx
		cancel := func() {}
		if j.timeout > 0 {
			runCtx, cancel = context.WithTimeout(ctx, j.timeout)
		}
		defer cancel()

		result, err := j.runWithRetry(runCtx)
		finished := time.Now().UTC()
		status := statusSuccess
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				status = statusTimeout
			} else {
				status = statusError
			}
		} else if result.SkipReason != "" {
			status = statusSkippedNoInput
		}
		code, message := pickIssue(result, err)
		j.finishRun(start, finished, status, code, message, result, err)
	}()
}

func (j *jobRunner) finishRun(start, finished time.Time, status, code, message string, result JobResult, err error) {
	duration := finished.Sub(start)
	j.updateStateAfterRun(status, code, message, finished, duration)

	recordsSuccess, recordsError, recordsSkipped := summarizeRecords(result.Records)
	j.metrics.ObserveJobRun(j.name, status, duration)
	j.metrics.SetJobConsecutiveFailures(j.name, float64(j.snapshot().ConsecutiveFailures))
	if status == statusSuccess {
		j.metrics.SetJobLastSuccess(j.name, finished)
	} else if status == statusError || status == statusTimeout || status == statusSkippedNoInput {
		j.metrics.SetJobLastError(j.name, finished)
	}
	for _, rec := range result.Records {
		j.metrics.AddFetchRecords(j.name, rec.Entity, rec.Result, rec.Count)
	}
	for dataset, seconds := range result.Freshness {
		j.metrics.SetDataFreshness(dataset, seconds)
	}
	for _, issue := range result.Errors {
		j.metrics.AddJobError(j.name, issue.Code, issue.Count)
	}
	if (status == statusError || status == statusTimeout || status == statusSkippedNoInput) && code != "" {
		j.metrics.AddJobError(j.name, code, 1)
	}

	meta := map[string]any{
		"skip_reason":   result.SkipReason,
		"error_count":   len(result.Errors),
		"warning_count": len(result.Warnings),
	}
	if len(result.Freshness) > 0 {
		meta["freshness"] = result.Freshness
	}
	if err != nil {
		meta["run_error"] = err.Error()
	}
	recordCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	j.recordRun(recordCtx, RunRecord{
		JobName:        j.name,
		StartedAt:      start,
		FinishedAt:     finished,
		Duration:       duration,
		Status:         status,
		RecordsSuccess: recordsSuccess,
		RecordsError:   recordsError,
		RecordsSkipped: recordsSkipped,
		ErrorCode:      code,
		ErrorMessage:   message,
		Meta:           meta,
	})

	if (status == statusError || status == statusTimeout) && j.log != nil {
		j.log.Warn("scheduler job failed",
			zap.String("job", j.name),
			zap.String("status", status),
			zap.String("error_code", code),
			zap.String("error_message", message),
		)
	}
	if status == statusSkippedNoInput && j.log != nil {
		j.log.Info("scheduler job skipped due to no input",
			zap.String("job", j.name),
			zap.String("reason", result.SkipReason),
			zap.String("error_code", code),
		)
	}
}

func (j *jobRunner) runWithRetry(ctx context.Context) (JobResult, error) {
	var lastErr error
	var lastResult JobResult
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= defaultRetryAttempts; attempt++ {
		result, err := j.run(ctx)
		lastResult = result
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt == defaultRetryAttempts || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}
		wait := backoff
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return JobResult{}, ctx.Err()
		case <-t.C:
		}
		backoff *= 2
	}
	return lastResult, lastErr
}

func (j *jobRunner) withJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	ratio := j.jitter
	if ratio <= 0 {
		return base
	}
	if ratio > 0.5 {
		ratio = 0.5
	}
	delta := float64(base) * ratio
	if delta < 1 {
		return base
	}
	offset := rand.Float64()*2*delta - delta
	return time.Duration(float64(base) + offset)
}

func (j *jobRunner) updateNextRun(at time.Time) {
	j.stateMu.Lock()
	defer j.stateMu.Unlock()
	j.state.nextRunAt = at.UTC()
}

func (j *jobRunner) setInflight(on bool) {
	j.stateMu.Lock()
	defer j.stateMu.Unlock()
	j.state.inflight = on
}

func (j *jobRunner) updateStateAfterRun(status, code, message string, finished time.Time, duration time.Duration) {
	j.stateMu.Lock()
	defer j.stateMu.Unlock()
	j.state.inflight = false
	j.state.lastStatus = status
	j.state.lastDuration = duration
	if status == statusSuccess {
		j.state.lastSuccessAt = finished
		j.state.consecutiveFailures = 0
		j.state.lastErrorCode = ""
		j.state.lastErrorMessage = ""
		return
	}
	if status == statusError || status == statusTimeout {
		j.state.lastErrorAt = finished
		j.state.lastErrorCode = code
		j.state.lastErrorMessage = message
		j.state.consecutiveFailures++
		return
	}
	if status == statusSkippedNoInput {
		j.state.lastErrorAt = finished
		j.state.lastErrorCode = code
		j.state.lastErrorMessage = message
		j.state.consecutiveFailures++
	}
}

func (j *jobRunner) snapshot() JobRuntimeSnapshot {
	j.stateMu.RLock()
	defer j.stateMu.RUnlock()
	s := JobRuntimeSnapshot{
		Name:                j.name,
		IntervalSeconds:     j.interval.Seconds(),
		TimeoutSeconds:      j.timeout.Seconds(),
		Immediate:           j.immediate,
		Inflight:            j.state.inflight,
		LastStatus:          j.state.lastStatus,
		LastErrorCode:       j.state.lastErrorCode,
		LastErrorMessage:    j.state.lastErrorMessage,
		ConsecutiveFailures: j.state.consecutiveFailures,
		LastDurationMS:      j.state.lastDuration.Milliseconds(),
	}
	if !j.state.nextRunAt.IsZero() {
		v := j.state.nextRunAt
		s.NextRunAt = &v
	}
	if !j.state.lastSuccessAt.IsZero() {
		v := j.state.lastSuccessAt
		s.LastSuccessAt = &v
	}
	if !j.state.lastErrorAt.IsZero() {
		v := j.state.lastErrorAt
		s.LastErrorAt = &v
	}
	return s
}

func (j *jobRunner) recordRun(ctx context.Context, rec RunRecord) {
	if j.recorder == nil {
		return
	}
	if err := j.recorder.RecordSchedulerRun(ctx, rec); err != nil && j.log != nil {
		j.log.Warn("persist scheduler run failed", zap.String("job", j.name), zap.Error(err))
	}
}

func summarizeRecords(records []FetchRecord) (success int, failed int, skipped int) {
	for _, rec := range records {
		n := rec.Count
		if n <= 0 {
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

func pickIssue(result JobResult, err error) (code string, message string) {
	for _, issue := range result.Errors {
		if strings.TrimSpace(issue.Code) != "" || strings.TrimSpace(issue.Message) != "" {
			if issue.Code != "" {
				return issue.Code, issue.Message
			}
			return errCodeUnknown, issue.Message
		}
	}
	if err != nil {
		return classifyErrorCode(err), err.Error()
	}
	if result.SkipReason != "" {
		return classifySkipReason(result.SkipReason), result.SkipReason
	}
	return "", ""
}

func classifyErrorCode(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "rpc_url is empty"):
		return errCodeChainRPCEmpty
	case strings.Contains(msg, "too many requests"),
		strings.Contains(msg, " 429"),
		strings.Contains(msg, "status: 429"),
		strings.Contains(msg, "rate limit"):
		return errCodeUpstream429
	case strings.Contains(msg, "up to a 10 block range"),
		strings.Contains(msg, "block range should work"),
		strings.Contains(msg, "compute units capacity"):
		return errCodeRPCRange
	case strings.Contains(msg, "status: 5"):
		return errCodeUpstream5xx
	case strings.Contains(msg, "sqlstate"),
		strings.Contains(msg, "sql:"),
		strings.Contains(msg, "gorm"),
		strings.Contains(msg, "constraint"),
		strings.Contains(msg, "relation "):
		return errCodeDBWrite
	default:
		return errCodeUnknown
	}
}

func classifySkipReason(reason string) string {
	v := strings.ToLower(strings.TrimSpace(reason))
	switch {
	case strings.Contains(v, "chain.rpc_url is empty"):
		return errCodeChainRPCEmpty
	case strings.Contains(v, "city"), strings.Contains(v, "cities"):
		return errCodeEmptyCitySet
	case strings.Contains(v, "market"), strings.Contains(v, "markets"):
		return errCodeEmptyMarketSet
	default:
		return errCodeSkippedNoInput
	}
}
