package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/coder/websocket"
	"github.com/jellydator/xync"
	"github.com/oxynote/wetsocks/internal/httpclient"
	"github.com/oxynote/wetsocks/internal/testutil"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("github.com/coder/websocket.(*Conn).timeoutLoop"),
	)
}

func Test_New(t *testing.T) {
	// default options
	c := New(slog.New(slog.DiscardHandler), &MetricsMock{}, Options{})
	require.NotNil(t, c)
	assert.Equal(t, int64(_defaultReadLimit), c.opts.ReadLimit)
	assert.NotZero(t, c.log)
	assert.NotNil(t, c.supv)
	assert.NotNil(t, c.writeCh)
	assert.Equal(t, _defaultReqID, c.req.nextID)
	assert.NotNil(t, c.req.respFns)

	assert.Nil(t, c.readCh)

	// non-default options
	opts := Options{
		ReadLimit:    1,
		RecoveryFunc: func(_ any) {},
		SerialReads:  true,
	}
	c = New(slog.New(slog.DiscardHandler), &MetricsMock{}, opts)
	require.NotNil(t, c)
	assert.Equal(t, opts.ReadLimit, c.opts.ReadLimit)
	assert.NotZero(t, c.log)
	assert.NotNil(t, c.supv)
	assert.NotNil(t, c.writeCh)
	assert.Equal(t, _defaultReqID, c.req.nextID)
	assert.NotNil(t, c.req.respFns)
	assert.NotNil(t, c.readCh)
	assert.Equal(t, _serialReadsBuffer, cap(c.readCh))
}

func Test_Client_IsOpen(t *testing.T) {
	var c Client

	assert.False(t, c.IsOpen())

	c.conn = &websocket.Conn{}
	assert.True(t, c.IsOpen())
}

func Test_Client_isOpen(t *testing.T) {
	var c Client

	assert.False(t, c.isOpen())

	c.conn = &websocket.Conn{}
	assert.True(t, c.isOpen())
}

func Test_Client_Open(t *testing.T) {
	var (
		retryMu    sync.Mutex
		retryIndex int

		invalidMu sync.Mutex
		invalid   bool
	)

	resCh := make(chan []byte)

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retryMu.Lock()
		if retryIndex < 5 {
			w.WriteHeader(http.StatusInternalServerError)

			if retryIndex >= 0 {
				retryIndex++
			}

			retryMu.Unlock()

			return
		}
		retryMu.Unlock()

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		_, msg, err := conn.Read(context.Background())
		if err != nil {
			return
		}

		invalidMu.Lock()
		if invalid {
			conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
				context.Background(),
				websocket.MessageText,
				[]byte(`{"hello":`),
			)
		}
		invalidMu.Unlock()

		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			context.Background(),
			websocket.MessageText,
			[]byte(`{"hello":"123"}`),
		)
		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			context.Background(),
			websocket.MessageText,
			[]byte(`{"hi":"123","id":2}`),
		)
		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			context.Background(),
			websocket.MessageText,
			[]byte(`{"hi":"123","id":300}`),
		)
		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			context.Background(),
			websocket.MessageText,
			[]byte(`{"hi":"123","id":{"sub":123}}`),
		)
		conn.Write( //nolint:gosec,errcheck // error provides no meaningful info
			context.Background(),
			websocket.MessageText,
			[]byte(`[{"hi":"abc"}]`),
		)

		// we need to make sure that all events were read before
		// allowing tests to continue
		time.Sleep(time.Millisecond * 100)
		resCh <- msg

		// read is needed for the closing frame
		conn.Read(context.Background()) //nolint:gosec,errcheck // error provides no meaningful info
	}))
	t.Cleanup(hs.Close)

	var (
		wg sync.WaitGroup

		callsMu sync.RWMutex
		calls   int
	)

	respFns := map[uint64]func(json.RawMessage){
		1: func(_ json.RawMessage) { // unregistered handler
			callsMu.Lock()
			calls++
			callsMu.Unlock()
			wg.Done()
		},
		2: func(data json.RawMessage) {
			assert.Equal(t, `{"hi":"123","id":2}`, string(data))
			callsMu.Lock()
			calls++
			callsMu.Unlock()
			wg.Done()
		},
	}
	out, b := testutil.NewBuffer()
	c := Client{
		log: slog.New(slog.NewJSONHandler(out, nil)),
		opts: Options{
			DialOptions: websocket.DialOptions{
				HTTPClient: httpclient.New(0),
			},
			URL:          hs.URL,
			ReadLimit:    _defaultReadLimit,
			Cron:         cron.New(),
			PingInterval: time.Millisecond,
		},
		supv:    xync.NewSupervisor(),
		metrics: &MetricsMock{},
		conn:    &websocket.Conn{},
		readFns: []func(context.Context, json.RawMessage){
			func(_ context.Context, data json.RawMessage) {
				assert.Contains(t, []string{
					`{"hello":"123"}`,
					`{"hi":"123","id":300}`,
					`{"hi":"123","id":{"sub":123}}`,
					`[{"hi":"abc"}]`,
				}, string(data))
				callsMu.Lock()
				calls++
				callsMu.Unlock()
				wg.Done()
			},
			func(_ context.Context, data json.RawMessage) {
				assert.Contains(t, []string{
					`{"hello":"123"}`,
					`{"hi":"123","id":300}`,
					`{"hi":"123","id":{"sub":123}}`,
					`[{"hi":"abc"}]`,
				}, string(data))
				callsMu.Lock()
				calls++
				callsMu.Unlock()
				wg.Done()
			},
		},
		writeCh: make(chan json.RawMessage),
	}
	c.req.respFns = respFns
	ctx := httpclient.WithRetryContext(
		context.Background(),
		backoff.NewConstantBackOff(time.Millisecond*10),
	)

	// error - connection already open
	assert.Equal(t, ErrConnOpen, c.Open(ctx))
	require.NoError(t, out.Flush())
	assert.Empty(t, b.String())

	c.supv.Wait()
	callsMu.RLock()
	assert.Zero(t, calls)
	callsMu.RUnlock()

	b.Reset()

	// error - url function returns an error
	retryMu.Lock()
	retryIndex = 0
	retryMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	c.opts.URL = ""
	c.opts.URLFunc = func() (string, error) {
		return "", assert.AnError
	}

	assert.Error(t, c.Open(context.Background()))
	require.NoError(t, out.Flush())
	assert.Empty(t, b.String())

	c.supv.Wait()
	callsMu.RLock()
	assert.Zero(t, calls)
	callsMu.RUnlock()

	b.Reset()

	// error - context cancelled
	c.opts.URL = hs.URL

	retryMu.Lock()
	retryIndex = 0
	retryMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	assert.Error(t, c.Open(cancelledCtx))
	require.NoError(t, out.Flush())
	assert.Empty(t, b.String())

	c.supv.Wait()
	callsMu.RLock()
	assert.Zero(t, calls)
	callsMu.RUnlock()

	b.Reset()

	// success with conn.Write ctx cancellation
	wg.Add(9)
	retryMu.Lock()
	retryIndex = 0
	retryMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	c.opts.URL = ""
	c.opts.URLFunc = func() (string, error) {
		return hs.URL, nil
	}

	require.NoError(t, c.Open(ctx))
	c.connMu.RLock()
	assert.NotNil(t, c.conn)
	assert.NotNil(t, c.stop)
	c.connMu.RUnlock()

	c.writeCh <- json.RawMessage(`{"test":123}`)

	assert.Eventually(t, func() bool {
		select {
		case data := <-resCh:
			assert.Equal(t, `{"test":123}`, string(data))
			return true
		default:
			return false
		}
	}, time.Millisecond*500, time.Millisecond*250)

	wg.Wait()

	stopCh := make(chan struct{})

	c.connMu.Lock() // we need time to cancel ctx in the middle of startWriter()
	c.writeCh <- json.RawMessage("{}")
	c.stop()
	c.stop = func() {
		stopCh <- struct{}{}
	}
	c.connMu.Unlock()

	<-stopCh

	c.supv.Wait()
	callsMu.RLock()
	assert.Equal(t, 9, calls)
	callsMu.RUnlock()

	require.NoError(t, out.Flush())
	assert.Empty(t, b.String())

	b.Reset()

	// success with write loop ctx cancellation
	wg.Add(9)
	retryMu.Lock()
	retryIndex = 0
	retryMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.req.mu.Lock()
	c.req.respFns = respFns
	c.req.mu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	require.NoError(t, c.Open(ctx))
	c.connMu.RLock()
	assert.NotNil(t, c.conn)
	assert.NotNil(t, c.stop)
	c.connMu.RUnlock()

	c.writeCh <- json.RawMessage(`{"hello":"hi"}`)

	assert.Eventually(t, func() bool {
		select {
		case data := <-resCh:
			assert.Equal(t, `{"hello":"hi"}`, string(data))
			return true
		default:
			return false
		}
	}, time.Millisecond*500, time.Millisecond*250)

	wg.Wait()

	stopCh = make(chan struct{})

	c.connMu.Lock()
	c.stop()
	c.stop = func() {
		stopCh <- struct{}{}
	}
	c.connMu.Unlock()

	<-stopCh

	c.supv.Wait()
	callsMu.RLock()
	assert.Equal(t, 9, calls)
	callsMu.RUnlock()

	require.NoError(t, out.Flush())
	assert.Empty(t, b.String())

	b.Reset()

	// success with invalid json
	invalidMu.Lock()
	invalid = true
	invalidMu.Unlock()

	retryMu.Lock()
	retryIndex = 0
	retryMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.req.mu.Lock()
	c.req.respFns = nil
	c.req.mu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()

	require.NoError(t, c.Open(ctx))
	c.connMu.RLock()
	assert.NotNil(t, c.conn)
	assert.NotNil(t, c.stop)
	c.connMu.RUnlock()

	c.writeCh <- json.RawMessage(`{"hello":"hi"}`)

	assert.Eventually(t, func() bool {
		select {
		case data := <-resCh:
			assert.Equal(t, `{"hello":"hi"}`, string(data))
			return true
		default:
			return false
		}
	}, time.Millisecond*500, time.Millisecond*250)

	c.stop()
	c.supv.Wait()
	callsMu.RLock()
	assert.Zero(t, calls)
	callsMu.RUnlock()

	require.NoError(t, out.Flush())
	assert.Contains(t, b.String(), "failed to read JSON")

	b.Reset()

	// successful startReader/startWriter execution without conn
	invalidMu.Lock()
	invalid = false
	invalidMu.Unlock()

	c.connMu.Lock()
	c.conn = nil
	c.writeCh = make(chan json.RawMessage, 1) // to prevent blocking
	c.connMu.Unlock()

	callsMu.Lock()
	calls = 0
	callsMu.Unlock()

	c.startReader(context.Background())
	c.writeCh <- json.RawMessage("{}")
	c.startWriter(context.Background())

	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), "cannot continue reading from the active connection")
	assert.NotContains(t, b.String(), "cannot continue writing to the active connection")

	c.supv.Wait()
	callsMu.RLock()
	assert.Zero(t, calls)
	callsMu.RUnlock()
}

func Test_Client_Close(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Read(context.Background()) //nolint:gosec,errcheck // error provides no meaningful info
	}))
	t.Cleanup(hs.Close)

	var called bool

	c := Client{
		opts: Options{
			Cron: cron.New(),
		},
		supv:    xync.NewSupervisor(),
		metrics: &MetricsMock{},
		stop:    func() { called = true },
	}
	c.req.nextID = 300
	c.req.respFns = map[uint64]func(json.RawMessage){
		1: func(json.RawMessage) {},
	}

	// conn not open
	c.Close()
	assert.Nil(t, c.conn)
	assert.NotNil(t, c.stop)
	assert.False(t, called)
	assert.Equal(t, uint64(300), c.req.nextID)
	assert.Len(t, c.req.respFns, 1)

	// success with context cancellation
	called = false
	c.req.nextID = 300
	c.req.respFns = map[uint64]func(json.RawMessage){
		1: func(json.RawMessage) {},
	}
	c.stop = func() { called = true }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.conn = dial(t, hs.URL)
	err := c.conn.Write(ctx, websocket.MessageText, []byte("{}"))
	assert.ErrorIs(t, err, context.Canceled)

	c.Close()
	assert.Nil(t, c.conn)
	assert.Nil(t, c.stop)
	assert.True(t, called)
	assert.Equal(t, _defaultReqID, c.req.nextID)
	assert.Empty(t, c.req.respFns)

	// success with conn already closed
	called = false
	c.req.nextID = 300
	c.req.respFns = map[uint64]func(json.RawMessage){
		1: func(json.RawMessage) {},
	}
	c.stop = func() { called = true }

	c.conn = dial(t, hs.URL)
	require.NoError(t, c.conn.Close(websocket.StatusNormalClosure, ""))

	c.Close()
	assert.Nil(t, c.conn)
	assert.Nil(t, c.stop)
	assert.True(t, called)
	assert.Equal(t, _defaultReqID, c.req.nextID)
	assert.Empty(t, c.req.respFns)
}

func Test_Client_OnRead(t *testing.T) {
	var c Client

	c.OnRead(func(context.Context, json.RawMessage) {})
	c.OnRead(func(context.Context, json.RawMessage) {})
	assert.Len(t, c.readFns, 2)
}

func Test_Client_startSerialProcessor(t *testing.T) {
	var (
		mu        sync.Mutex
		received  []string
		recovered int
	)

	c := New(slog.New(slog.DiscardHandler), &MetricsMock{}, Options{
		SerialReads: true,
		RecoveryFunc: func(_ any) {
			mu.Lock()
			recovered++
			mu.Unlock()
		},
	})
	c.OnRead(func(_ context.Context, d json.RawMessage) {
		if string(d) == `"boom"` {
			panic("boom")
		}

		mu.Lock()
		received = append(received, string(d))
		mu.Unlock()
	})

	for i := range 100 {
		c.readCh <- json.RawMessage(fmt.Sprintf("%d", i))
	}

	// a panicking event must not kill the processing of
	// subsequent events
	c.readCh <- json.RawMessage(`"boom"`)
	c.readCh <- json.RawMessage(`"last"`)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.startSerialProcessor(ctx)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(received) == 101
	}, time.Second*5, time.Millisecond*10)

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()

	for i := range 100 {
		assert.Equal(t, fmt.Sprintf("%d", i), received[i])
	}

	assert.Equal(t, `"last"`, received[100])
	assert.Equal(t, 1, recovered)
}

func Test_Client_SerialReads(t *testing.T) {
	const total = 50

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		for i := range total {
			err := conn.Write(
				r.Context(),
				websocket.MessageText,
				[]byte(fmt.Sprintf(`{"seq":%d}`, i)),
			)
			if err != nil {
				return
			}
		}

		conn.Read(context.Background()) //nolint:gosec,errcheck // error provides no meaningful info
	}))
	t.Cleanup(hs.Close)

	var (
		mu   sync.Mutex
		seqs []int64
	)

	c := New(slog.New(slog.DiscardHandler), &MetricsMock{}, Options{
		URL:         hs.URL,
		SerialReads: true,
	})
	c.OnRead(func(_ context.Context, d json.RawMessage) {
		// the sleep amplifies reordering that would occur if the
		// events were processed concurrently
		time.Sleep(time.Millisecond)

		mu.Lock()
		seqs = append(seqs, gjson.GetBytes(d, "seq").Int())
		mu.Unlock()
	})

	require.NoError(t, c.Open(context.Background()))
	t.Cleanup(c.Close)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(seqs) == total
	}, time.Second*5, time.Millisecond*10)

	mu.Lock()
	defer mu.Unlock()

	for i := range seqs {
		assert.Equal(t, int64(i), seqs[i])
	}
}

func Test_Client_Write(t *testing.T) {
	ch := make(chan json.RawMessage)
	c := Client{
		writeCh: make(chan json.RawMessage),
	}
	msg := map[string]string{"hello": "hi"}

	go func() {
		for data := range c.writeCh {
			ch <- data
		}
	}()

	// error - conn closed
	assert.Equal(t, ErrConnClosed, c.Write(context.Background(), msg))
	assert.Empty(t, ch)

	// error - invalid json
	c.conn = &websocket.Conn{}
	assert.Error(t, c.Write(context.Background(), func() {}))
	assert.Empty(t, ch)

	// error - cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.writeCh <- json.RawMessage("-1")

	assert.Equal(t, context.Canceled, c.Write(ctx, msg))
	assert.Equal(t, "-1", string(<-ch))

	// success
	assert.NoError(t, c.Write(context.Background(), msg))
	assert.JSONEq(t, `{"hello":"hi"}`, string(<-ch))
	close(c.writeCh)
}

func Test_Client_Send(t *testing.T) {
	const reqID uint64 = 300

	c := Client{
		writeCh: make(chan json.RawMessage),
	}
	c.req.nextID = reqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))

	msg := map[string]string{"hello": "hi"}
	midCh := make(chan json.RawMessage)

	t.Cleanup(func() {
		close(c.writeCh)
	})

	go func() {
		for data := range c.writeCh {
			midCh <- data
		}

		close(midCh)
	}()

	// error - conn closed
	res, err := c.Send(context.Background(), msg)
	assert.Equal(t, ErrConnClosed, err)
	assert.Nil(t, res)
	assert.Empty(t, midCh)
	assert.NotContains(t, c.req.respFns, reqID)
	assert.Equal(t, reqID, c.req.nextID)

	// error - invalid payload
	c.conn = &websocket.Conn{}
	c.req.nextID = reqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))

	res, err = c.Send(context.Background(), func() {})
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Empty(t, midCh)
	assert.NotContains(t, c.req.respFns, reqID)
	assert.Equal(t, reqID, c.req.nextID)

	// error - context cancellation in Write()
	c.req.nextID = reqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))
	c.writeCh <- json.RawMessage("-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err = c.Send(ctx, msg)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
	assert.Equal(t, "-1", string(<-midCh))
	assert.NotContains(t, c.req.respFns, reqID)
	assert.Equal(t, reqID+1, c.req.nextID)

	// error - context cancellation after Write()
	c.req.nextID = reqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))
	ctx, cancel = context.WithCancel(context.Background())

	go func() {
		req := <-midCh
		assert.JSONEq(t,
			fmt.Sprintf(`{"hello":"hi","id":%d}`, reqID),
			string(req),
		)
		cancel()

		// test respFn's context cancellation
		c.req.mu.RLock()
		c.req.respFns[reqID](json.RawMessage(fmt.Sprintf(`
			{
				"id":%d,
				"hello":"test"
			}`, reqID)),
		)
		c.req.mu.RUnlock()
	}()

	res, err = c.Send(ctx, msg)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
	assert.NotContains(t, c.req.respFns, reqID)
	assert.Equal(t, reqID+1, c.req.nextID)

	// success
	c.req.nextID = reqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))

	go func() {
		data := <-midCh
		assert.JSONEq(t,
			fmt.Sprintf(`{"hello":"hi","id":%d}`, reqID),
			string(data),
		)
		c.req.mu.RLock()
		c.req.respFns[reqID](json.RawMessage(fmt.Sprintf(`
			{
				"id":%d,
				"hello":"test"
			}`, reqID)),
		)
		c.req.mu.RUnlock()
	}()

	res, err = c.Send(context.Background(), msg)
	assert.NoError(t, err)
	assert.JSONEq(t, fmt.Sprintf(`
			{
				"id":%d,
				"hello":"test"
			}`, reqID,
	), string(res))
	assert.NotContains(t, c.req.respFns, reqID)
	assert.Equal(t, reqID+1, c.req.nextID)
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	conn, _, err := websocket.Dial(ctx, url, nil) //nolint:bodyclose // body closing is handled by the lib
	require.NoError(t, err)

	return conn
}
