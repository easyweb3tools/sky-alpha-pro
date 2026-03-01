package scheduler

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

type testRunRecorder struct {
	mu   sync.Mutex
	runs []RunRecord
}

func (r *testRunRecorder) RecordSchedulerRun(_ context.Context, rec RunRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, rec)
	return nil
}

func TestManagerStartRunsImmediateJob(t *testing.T) {
	mgr := NewManager(config.SchedulerConfig{
		Enabled:            true,
		RunOnStart:         true,
		DefaultTimeout:     2 * time.Second,
		DefaultJitterRatio: 0,
	}, nil, nil)

	var runs int32
	mgr.Register(Job{
		Name:      "test_job",
		Interval:  200 * time.Millisecond,
		Timeout:   time.Second,
		Immediate: true,
		Run: func(ctx context.Context) (JobResult, error) {
			atomic.AddInt32(&runs, 1)
			return JobResult{}, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&runs) > 0 {
			cancel()
			mgr.Wait()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected immediate job run")
}

func TestManagerWaitReturnsAfterCancel(t *testing.T) {
	mgr := NewManager(config.SchedulerConfig{
		Enabled:            true,
		RunOnStart:         true,
		DefaultTimeout:     time.Second,
		DefaultJitterRatio: 0,
	}, nil, nil)

	var runs int32
	mgr.Register(Job{
		Name:      "wait_job",
		Interval:  50 * time.Millisecond,
		Immediate: true,
		Run: func(ctx context.Context) (JobResult, error) {
			atomic.AddInt32(&runs, 1)
			select {
			case <-ctx.Done():
				return JobResult{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
				return JobResult{}, nil
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.Wait()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("manager wait timeout")
	}
	if atomic.LoadInt32(&runs) == 0 {
		t.Fatalf("expected job to run at least once")
	}
}

func TestManagerSnapshotAndRunRecorder(t *testing.T) {
	mgr := NewManager(config.SchedulerConfig{
		Enabled:            true,
		RunOnStart:         true,
		DefaultTimeout:     time.Second,
		DefaultJitterRatio: 0,
	}, nil, nil)
	recorder := &testRunRecorder{}
	mgr.SetRunRecorder(recorder)

	mgr.Register(Job{
		Name:      "skip_job",
		Interval:  200 * time.Millisecond,
		Immediate: true,
		Run: func(ctx context.Context) (JobResult, error) {
			return JobResult{
				SkipReason: "no input",
				Errors: []JobIssue{{
					Code:    errCodeEmptyCitySet,
					Message: "no active cities",
					Source:  "test",
					Count:   1,
				}},
			}, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := mgr.Snapshot()
		if len(s.Jobs) == 1 && s.Jobs[0].LastStatus == statusSkippedNoInput {
			cancel()
			mgr.Wait()
			if len(recorder.runs) == 0 {
				t.Fatalf("expected scheduler run to be recorded")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	mgr.Wait()
	t.Fatalf("expected snapshot last status=%s", statusSkippedNoInput)
}

func TestRunWithRetrySkipsRapidRetriesOnRateLimit(t *testing.T) {
	var attempts int32
	runner := &jobRunner{
		name: "chain_scan",
		run: func(ctx context.Context) (JobResult, error) {
			atomic.AddInt32(&attempts, 1)
			return JobResult{}, errors.New("429 Too Many Requests")
		},
	}
	_, err := runner.runWithRetry(context.Background())
	if err == nil {
		t.Fatalf("expected rate-limit error")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("expected one attempt on rate-limit error, got %d", got)
	}
}

func TestComputeJobBackoffForRateLimit(t *testing.T) {
	base := 15 * time.Second
	backoff1 := computeJobBackoff(base, errCodeUpstream429, 1)
	backoff2 := computeJobBackoff(base, errCodeUpstream429, 2)
	backoff5 := computeJobBackoff(base, errCodeUpstream429, 5)

	if backoff1 < 30*time.Second {
		t.Fatalf("expected first backoff >= 30s, got %s", backoff1)
	}
	if backoff2 <= backoff1 {
		t.Fatalf("expected second backoff > first, got first=%s second=%s", backoff1, backoff2)
	}
	if backoff5 > 15*time.Minute {
		t.Fatalf("expected capped backoff <= 15m, got %s", backoff5)
	}
}

func TestFinishRunEmitsStructuredErrorMetricOnHardFailure(t *testing.T) {
	reg := metrics.New(config.MetricsConfig{Enabled: true, Path: "/metrics"})
	runner := &jobRunner{
		name:    "chain_scan",
		metrics: reg,
	}
	start := time.Now().UTC()
	finish := start.Add(100 * time.Millisecond)
	runner.finishRun(start, finish, statusError, "", "429 Too Many Requests", JobResult{}, errors.New("429 Too Many Requests"))

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	if !strings.Contains(text, `sky_alpha_scheduler_job_errors_total{error_code="unknown_error",job="chain_scan"} 1`) {
		t.Fatalf("expected structured error metric emitted for chain_scan failure, got: %s", text)
	}
}
