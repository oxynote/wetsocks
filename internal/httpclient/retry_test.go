package httpclient

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oxynote/wetsocks/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_New(t *testing.T) {
	c := New(time.Hour)
	require.NotNil(t, c)
	assert.Equal(t, time.Hour, c.Timeout)
	require.IsType(t, &RetryTransport{}, c.Transport)
	assert.Equal(t, http.DefaultTransport, c.Transport.(*RetryTransport).orig)
}

// stubRoundTripper returns each response in order, repeating the last one,
// and records the requests it receives.
type stubRoundTripper struct {
	responses []stubResponse
	calls     []*http.Request
}

type stubResponse struct {
	resp *http.Response
	err  error
}

func (s *stubRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	i := len(s.calls)
	if i >= len(s.responses) {
		i = len(s.responses) - 1
	}

	s.calls = append(s.calls, r)

	return s.responses[i].resp, s.responses[i].err
}

func Test_RetryTransport_RoundTrip(t *testing.T) {
	cc := map[string]struct {
		Context   context.Context
		Responses []stubResponse
		Calls     int
		Response  *http.Response
		Err       error
	}{
		"Internal server error": {
			Context: WithRetryContext(context.Background(),
				backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 1)),
			Responses: []stubResponse{
				{
					err: assert.AnError,
				},
				{
					resp: &http.Response{
						StatusCode: http.StatusInternalServerError,
					},
				},
			},
			Calls: 2,
			Err:   errors.New(strings.ToLower(http.StatusText(http.StatusInternalServerError))),
		},
		"Successful execution with default strategy": {
			Context: context.Background(),
			Responses: []stubResponse{
				{
					resp: &http.Response{
						StatusCode: http.StatusOK,
					},
				},
			},
			Calls: 1,
			Response: &http.Response{
				StatusCode: http.StatusOK,
			},
		},
		"Successful execution": {
			Context: WithRetryContext(context.Background(),
				backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 1)),
			Responses: []stubResponse{
				{
					err: assert.AnError,
				},
				{
					resp: &http.Response{
						StatusCode: http.StatusOK,
					},
				},
			},
			Calls: 2,
			Response: &http.Response{
				StatusCode: http.StatusOK,
			},
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			stub := &stubRoundTripper{responses: c.Responses}
			rt := &RetryTransport{
				orig: stub,
			}

			req, err := http.NewRequestWithContext(c.Context, http.MethodGet, "http://example.com", http.NoBody)
			require.NoError(t, err)

			resp, err := rt.RoundTrip(req) //nolint:bodyclose // body is not used

			if assert.Len(t, stub.calls, c.Calls) {
				for _, r := range stub.calls {
					assert.Equal(t, req, r)
				}
			}

			testutil.AssertEqualError(t, c.Err, err)

			if err != nil {
				return
			}

			assert.Equal(t, c.Response, resp)
		})
	}
}

func Test_WithRetryContext(t *testing.T) {
	strategy := backoff.NewConstantBackOff(0)
	ctx := WithRetryContext(context.Background(), strategy)
	assert.Equal(t, strategy, ctx.Value(_retryKey))
}
