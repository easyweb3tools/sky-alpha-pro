package scheduler

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

const (
	statusSuccess       = "success"
	statusError         = "error"
	statusTimeout       = "timeout"
	statusSkippedLocked = "skipped_locked"

	defaultRetryAttempts = 3
)

type FetchRecord struct {
	Entity string
	Result string
	Count  int
}

type JobResult struct {
	Records   []FetchRecord
	Freshness map[string]float64
}

type Job struct {
	Name      string
	Interval  time.Duration
	Timeout   time.Duration
	Immediate bool
	Run       func(ctx context.Context) (JobResult, error)
}

type Manager struct {
	cfg     config.SchedulerConfig
	log     *zap.Logger
	metrics *metrics.Registry
	jobs    []*jobRunner
	jobsMu  sync.Mutex
	wg      sync.WaitGroup
	started bool
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
	mu        sync.Mutex
	wg        *sync.WaitGroup
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

func (j *jobRunner) loop(ctx context.Context, runOnStart bool) {
	if runOnStart && j.immediate {
		j.trigger(ctx)
	}
	next := j.withJitter(j.interval)
	timer := time.NewTimer(next)
	defer timer.Stop()

	for {
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
		j.metrics.ObserveJobRun(j.name, statusSkippedLocked, 0)
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

		start := time.Now()
		runCtx := ctx
		cancel := func() {}
		if j.timeout > 0 {
			runCtx, cancel = context.WithTimeout(ctx, j.timeout)
		}
		defer cancel()

		result, err := j.runWithRetry(runCtx)
		status := statusSuccess
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				status = statusTimeout
			} else {
				status = statusError
			}
			j.metrics.SetJobLastError(j.name, time.Now().UTC())
			if j.log != nil {
				j.log.Warn("scheduler job failed", zap.String("job", j.name), zap.String("status", status), zap.Error(err))
			}
		} else {
			j.metrics.SetJobLastSuccess(j.name, time.Now().UTC())
			for _, rec := range result.Records {
				j.metrics.AddFetchRecords(j.name, rec.Entity, rec.Result, rec.Count)
			}
			for dataset, seconds := range result.Freshness {
				j.metrics.SetDataFreshness(dataset, seconds)
			}
		}
		j.metrics.ObserveJobRun(j.name, status, time.Since(start))
	}()
}

func (j *jobRunner) runWithRetry(ctx context.Context) (JobResult, error) {
	var lastErr error
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= defaultRetryAttempts; attempt++ {
		result, err := j.run(ctx)
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
	return JobResult{}, lastErr
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
