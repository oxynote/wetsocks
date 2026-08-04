// Package httpclient provides modified http client logic.
package httpclient

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// New creates a new http client with safe default settings.
func New(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &RetryTransport{
			orig: http.DefaultTransport,
		},
	}
}

// RetryTransport wraps another round tripper and applies retry strategy.
type RetryTransport struct {
	orig http.RoundTripper
}

// RoundTrip calls original round tripper's RoundTrip method and if it fails,
// it applies retry strategy in the context (if present).
func (rt *RetryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()

	strat, ok := ctx.Value(_retryKey).(backoff.BackOff)
	if !ok {
		strat = backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 0)
	}

	// we need to ensure that retrying is stopped when the context is
	// cancelled
	strat = backoff.WithContext(strat, ctx)

	var resp *http.Response

	fn := func() error {
		var err error

		resp, err = rt.orig.RoundTrip(r) //nolint:bodyclose // response is passed along
		if err != nil {
			return err
		}

		if resp.StatusCode >= 500 {
			err = errors.New(strings.ToLower(http.StatusText(resp.StatusCode)))
			resp = nil

			return err
		}

		return nil
	}

	if err := backoff.Retry(fn, strat); err != nil {
		return nil, err
	}

	return resp, nil
}

const _retryKey ctxKey = 1

type ctxKey int

// WithRetryContext adds the provided retry strategy into the context.
func WithRetryContext(ctx context.Context, strategy backoff.BackOff) context.Context {
	return context.WithValue(ctx, _retryKey, strategy)
}
