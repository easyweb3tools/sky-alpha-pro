package market

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

func doRequestWithRetry(ctx context.Context, client *http.Client, reqFactory func() (*http.Request, error), maxAttempts int) (*http.Response, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastErr error
	backoff := 350 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := reqFactory()
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			if !shouldRetryStatus(resp.StatusCode) || attempt == maxAttempts {
				return resp, nil
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = errors.New(resp.Status)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 3*time.Second {
			backoff = 3 * time.Second
		}
	}

	return nil, lastErr
}

func shouldRetryStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}
