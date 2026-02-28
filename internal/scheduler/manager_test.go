package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sky-alpha-pro/pkg/config"
)

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
