package wsreconn

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/davseby/purse/util/testutil"
	"github.com/davseby/purse/util/timeutil"
	"github.com/davseby/wetsocks/wsclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_NewClient(t *testing.T) {
	var respErr bool

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if respErr {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		for {
			_, _, err := conn.Read(r.Context())
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(hs.Close)

	// error
	var baseCtxCalled bool

	respErr = true
	keeper := &SubKeeperMock{}
	s, err := NewClient(
		context.Background(),
		slog.New(slog.DiscardHandler),
		&ClientMetricsMock{},
		ClientOptions{
			Options: wsclient.Options{
				URL: hs.URL,
			},
			BaseContext: func() context.Context {
				baseCtxCalled = true
				return context.Background()
			},
		},
		keeper,
	)
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.True(t, baseCtxCalled)

	// success with defaults
	baseCtxCalled = false
	respErr = false
	keeper = &SubKeeperMock{}
	s, err = NewClient(
		context.Background(),
		slog.New(slog.DiscardHandler),
		&ClientMetricsMock{},
		ClientOptions{
			Options: wsclient.Options{
				URL: hs.URL,
			},
		},
		keeper,
	)
	assert.NoError(t, err)
	require.NotNil(t, s)
	assert.NotZero(t, s.opts)
	assert.NotNil(t, s.metrics)
	assert.NotNil(t, s.opts.RecoveryFunc)
	assert.NotNil(t, s.opts.BaseContext)
	assert.Equal(t, time.Second*30, s.opts.CheckAfter)
	assert.NotZero(t, s.log)
	assert.Same(t, keeper, s.keeper)
	require.NotNil(t, s.ws)
	s.ws.Close()
	require.NotNil(t, s.proc)
	assert.False(t, baseCtxCalled)

	s.proc.Stop()

	ch := make(chan struct{})
	s.proc = timeutil.NewPeriodicExec(time.Hour, time.Millisecond, func(_ context.Context) {
		ch <- struct{}{}
	}, func(_ any) {}, false)

	go s.proc.Start()

	ff := keeper.OnChangeCalls()
	require.Len(t, ff, 1)
	ff[0].Fn(context.Background())

	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}, time.Millisecond*300, time.Millisecond*50)

	s.proc.Stop()

	// success without defaults
	baseCtxCalled = false
	keeper = &SubKeeperMock{}
	s, err = NewClient(
		context.Background(),
		slog.New(slog.DiscardHandler),
		&ClientMetricsMock{},
		ClientOptions{
			Options: wsclient.Options{
				URL:          hs.URL,
				RecoveryFunc: func(_ any) {},
			},
			BaseContext: func() context.Context {
				baseCtxCalled = true
				return context.Background()
			},
			CheckAfter: time.Hour,
		},
		keeper,
	)
	assert.NoError(t, err)
	require.NotNil(t, s)
	assert.NotZero(t, s.opts)
	assert.NotNil(t, s.metrics)
	assert.NotNil(t, s.opts.RecoveryFunc)
	assert.NotNil(t, s.opts.BaseContext)
	assert.Equal(t, time.Hour, s.opts.CheckAfter)
	assert.NotZero(t, s.log)
	assert.Same(t, keeper, s.keeper)
	require.NotNil(t, s.ws)
	s.ws.Close()
	require.NotNil(t, s.proc)
	assert.True(t, baseCtxCalled)

	s.proc.Stop()

	s.proc = timeutil.NewPeriodicExec(time.Hour, time.Millisecond, func(_ context.Context) {
		ch <- struct{}{}
	}, func(_ any) {}, false)

	go s.proc.Start()

	ff = keeper.OnChangeCalls()
	require.Len(t, ff, 1)
	ff[0].Fn(context.Background())

	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}, time.Millisecond*300, time.Millisecond*50)

	s.proc.Stop()
}

func Test_Client_Close(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Read(r.Context()) //nolint:gosec,errcheck // error is meaningless
	}))
	t.Cleanup(hs.Close)

	s := Client{
		ws: wsclient.New(slog.New(slog.DiscardHandler), &ClientMetricsMock{}, wsclient.Options{
			URL: hs.URL,
		}),
	}
	s.proc = timeutil.NewPeriodicExec(time.Hour, time.Second, func(_ context.Context) {}, func(_ any) {}, false)

	go s.proc.Start()

	require.NoError(t, s.ws.Open(context.Background()))
	s.Close()
}

func Test_Client_OnRead(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			r.Context(),
			websocket.MessageText,
			[]byte(`{"hi":"bye"}`),
		)

		conn.Read(r.Context()) //nolint:gosec,errcheck // error is meaningless
	}))
	t.Cleanup(hs.Close)

	s := Client{
		ws: wsclient.New(slog.New(slog.DiscardHandler), &ClientMetricsMock{}, wsclient.Options{
			URL: hs.URL,
		}),
	}

	ch1, ch2 := make(chan json.RawMessage), make(chan json.RawMessage)

	s.OnRead(func(_ context.Context, d json.RawMessage) {
		ch1 <- d
	})
	s.OnRead(func(_ context.Context, d json.RawMessage) {
		ch2 <- d
	})

	require.NoError(t, s.ws.Open(context.Background()))
	assert.Eventually(t, func() bool {
		select {
		case d := <-ch1:
			assert.JSONEq(t, `{"hi":"bye"}`, string(d))
			return true
		default:
			return false
		}
	}, time.Millisecond*300, time.Millisecond*50)
	assert.Eventually(t, func() bool {
		select {
		case d := <-ch2:
			assert.JSONEq(t, `{"hi":"bye"}`, string(d))
			return true
		default:
			return false
		}
	}, time.Millisecond*300, time.Millisecond*50)
	s.ws.Close()
}

func Test_Client_OnReconnect(t *testing.T) {
	s := Client{
		reconnFns: []func(context.Context){
			func(_ context.Context) {},
		},
	}

	var calls int

	s.OnReconnect(func(_ context.Context) {
		calls++
	})

	for i := 0; i < 10; i++ {
		for _, fn := range s.reconnFns {
			fn(context.Background())
		}
	}

	assert.Equal(t, 10, calls)
}

func Test_Client_process(t *testing.T) {
	var respErr bool

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if respErr {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		for {
			_, msg, err := conn.Read(context.Background())
			if err != nil {
				return
			}

			conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
				context.Background(),
				websocket.MessageText,
				msg,
			)
		}
	}))
	t.Cleanup(hs.Close)

	// error returned by Open()
	var (
		baseCtxCalled bool
		reconnCalled  bool
	)

	out, b := testutil.NewBuffer()
	respErr = true
	keeper := &SubKeeperMock{}
	s := Client{
		opts: ClientOptions{
			BaseContext: func() context.Context {
				baseCtxCalled = true
				return context.Background()
			},
		},
		log:     slog.New(slog.NewJSONHandler(out, nil)),
		metrics: &ClientMetricsMock{},
		keeper:  keeper,
		ws: wsclient.New(slog.New(slog.DiscardHandler), &ClientMetricsMock{}, wsclient.Options{
			URL: hs.URL,
		}),
		reconnFns: []func(context.Context){
			func(_ context.Context) {
				reconnCalled = true
			},
		},
	}
	s.process(context.Background())

	require.NoError(t, out.Flush())
	assert.Contains(t, b.String(), "cannot reopen the connection")
	assert.NotContains(t, b.String(), "cannot send a payload")
	assert.NotContains(t, b.String(), "cannot write a payload")
	assert.True(t, baseCtxCalled)
	assert.False(t, reconnCalled)
	assert.Len(t, keeper.ResetAllCalls(), 1)
	assert.Empty(t, keeper.PayloadsCalls())

	b.Reset()

	// success when the mutex is locked
	baseCtxCalled = false
	respErr = false
	keeper = &SubKeeperMock{}
	s.keeper = keeper

	s.procMu.Lock()
	s.process(context.Background())
	s.procMu.Unlock()

	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), "cannot reopen the connection")
	assert.NotContains(t, b.String(), "cannot send a payload")
	assert.NotContains(t, b.String(), "cannot write a payload")
	assert.False(t, baseCtxCalled)
	assert.Empty(t, keeper.ResetAllCalls())
	assert.Empty(t, keeper.PayloadsCalls())
	assert.False(t, reconnCalled)

	b.Reset()

	// success when the cooldown is zero
	var confirmCalled bool

	baseCtxCalled = false
	payloadInvalidSend := &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return func() {}, func(_ json.RawMessage) {}
		},
	}
	payloadInvalidWrite := &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return func() {}, nil
		},
	}
	payloadConfirmedSend := &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return map[string]string{
					"hello": "test",
				}, func(d json.RawMessage) {
					confirmCalled = true
					assert.JSONEq(t,
						`{"hello":"test","id":1}`,
						string(d),
					)
				}
		},
	}
	keeper = &SubKeeperMock{
		PayloadsFunc: func() ([]SubPayloader, time.Duration) {
			return []SubPayloader{
				payloadInvalidSend,
				payloadInvalidWrite,
				payloadConfirmedSend,
			}, 0
		},
	}
	s.keeper = keeper

	s.process(context.Background())

	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), "cannot reopen the connection")
	assert.Contains(t, b.String(), "cannot send a payload")
	assert.Contains(t, b.String(), "cannot write a payload")
	assert.True(t, baseCtxCalled)
	assert.Len(t, keeper.ResetAllCalls(), 1)
	assert.Len(t, keeper.PayloadsCalls(), 1)
	assert.Len(t, payloadInvalidSend.PayloadCalls(), 1)
	assert.Len(t, payloadInvalidWrite.PayloadCalls(), 1)
	assert.Len(t, payloadConfirmedSend.PayloadCalls(), 1)
	assert.True(t, confirmCalled)

	b.Reset()

	// success when the cooldown is non zero, ctx is cancelled
	// (conn is already open), and there is a nil payload
	ctx, cancel := context.WithCancel(context.Background())

	confirmCalled = false
	baseCtxCalled = false
	payloadInvalidSend = &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return func() {}, func(_ json.RawMessage) {}
		},
	}
	payloadNil := &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return nil, func(_ json.RawMessage) {}
		},
	}
	payloadConfirmedSend = &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return map[string]string{
					"hello": "test",
				}, func(d json.RawMessage) {
					confirmCalled = true

					cancel()
					assert.JSONEq(t,
						`{"hello":"test","id":2}`,
						string(d),
					)
				}
		},
	}
	keeper = &SubKeeperMock{
		PayloadsFunc: func() ([]SubPayloader, time.Duration) {
			return []SubPayloader{
				payloadNil,
				payloadConfirmedSend,
				payloadInvalidSend,
			}, time.Hour
		},
	}
	s.keeper = keeper

	s.process(ctx)

	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), "cannot reopen the connection")
	assert.NotContains(t, b.String(), "cannot send a payload")
	assert.NotContains(t, b.String(), "cannot write a payload")
	assert.True(t, baseCtxCalled)
	assert.Empty(t, keeper.ResetAllCalls())
	assert.Len(t, keeper.PayloadsCalls(), 1)
	assert.Len(t, payloadConfirmedSend.PayloadCalls(), 1)
	assert.Empty(t, payloadInvalidSend.PayloadCalls())
	assert.True(t, confirmCalled)

	b.Reset()

	// success when the cooldown is non zero (conn is already open)
	confirmCalled = false
	baseCtxCalled = false
	payloadInvalidSend = &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return func() {}, func(_ json.RawMessage) {}
		},
	}
	payloadConfirmedSend = &SubPayloaderMock{
		PayloadFunc: func() (any, func(json.RawMessage)) {
			return map[string]string{
					"hello": "test",
				}, func(d json.RawMessage) {
					confirmCalled = true
					assert.JSONEq(t,
						`{"hello":"test","id":3}`,
						string(d),
					)
				}
		},
	}
	keeper = &SubKeeperMock{
		PayloadsFunc: func() ([]SubPayloader, time.Duration) {
			return []SubPayloader{
				payloadConfirmedSend,
				payloadInvalidSend,
			}, time.Millisecond
		},
	}
	s.keeper = keeper

	s.process(context.Background())

	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), "cannot reopen the connection")
	assert.Contains(t, b.String(), "cannot send a payload")
	assert.NotContains(t, b.String(), "cannot write a payload")
	assert.True(t, baseCtxCalled)
	assert.Empty(t, keeper.ResetAllCalls())
	assert.Len(t, keeper.PayloadsCalls(), 1)
	assert.Len(t, payloadConfirmedSend.PayloadCalls(), 1)
	assert.Len(t, payloadInvalidSend.PayloadCalls(), 1)
	assert.True(t, confirmCalled)

	s.ws.Close()

	assert.True(t, reconnCalled)
}
