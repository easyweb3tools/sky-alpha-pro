package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sky-alpha-pro/pkg/config"
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
