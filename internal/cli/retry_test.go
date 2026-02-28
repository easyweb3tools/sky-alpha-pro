package cli

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunWithRetryEventuallySuccess(t *testing.T) {
	var calls int
	err := runWithRetry(context.Background(), retryOptions{
		Attempts:       3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		IsRetryable:    func(err error) bool { return true },
	}, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got error=%v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRunWithRetryStopOnNonRetryable(t *testing.T) {
	var calls int
	expected := errors.New("permanent")
	err := runWithRetry(context.Background(), retryOptions{
		Attempts:       5,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		IsRetryable: func(err error) bool {
			return false
		},
	}, func(context.Context) error {
		calls++
		return expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected single call for non-retryable error, got %d", calls)
	}
}

func TestRunWithRetryContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runWithRetry(ctx, retryOptions{
		Attempts:       3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}, func(context.Context) error {
		return errors.New("should not run")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}
