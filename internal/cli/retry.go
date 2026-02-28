package cli

import (
	"context"
	"time"
)

type retryOptions struct {
	Attempts       int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	IsRetryable    func(error) bool
	OnRetry        func(attempt int, err error, wait time.Duration)
}

func runWithRetry(ctx context.Context, opts retryOptions, fn func(context.Context) error) error {
	attempts := opts.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	backoff := opts.InitialBackoff
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}
	isRetryable := opts.IsRetryable
	if isRetryable == nil {
		isRetryable = func(err error) bool { return err != nil }
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == attempts || !isRetryable(err) {
			return err
		}

		wait := backoff
		if wait > maxBackoff {
			wait = maxBackoff
		}
		if opts.OnRetry != nil {
			opts.OnRetry(attempt, err, wait)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		next := backoff * 2
		if next > maxBackoff {
			next = maxBackoff
		}
		backoff = next
	}
	return lastErr
}
