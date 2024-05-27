package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jellydator/purse/util/testutil"
	"github.com/jellydator/wetsocks/wsutil"
	"github.com/jellydator/xync"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"nhooyr.io/websocket"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_New(t *testing.T) {
	s := New(
		zerolog.Nop(),
		&Router{},
		Options{},
	)

	require.NotNil(t, s)
	assert.NotZero(t, s.log)
	assert.NotNil(t, s.router)
	assert.Equal(t, int64(_defaultReadLimit), s.opts.ReadLimit)
	assert.Equal(t, time.Second*5, s.opts.ResponseWriteTimeout)
	require.NotNil(t, s.opts.BaseContext)

	ctx, err := s.opts.BaseContext(nil)
	assert.NoError(t, err)
	assert.Equal(t, context.Background(), ctx)
	assert.NotNil(t, s.conns)
	assert.NotNil(t, s.supv)

	bctx := context.WithValue(context.Background(), contextKey(3), "test")
	acceptOpts := websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}
	s = New(
		zerolog.Nop(),
		&Router{},
		Options{
			AcceptOptions:        acceptOpts,
			ReadLimit:            300,
			ResponseWriteTimeout: time.Microsecond,
			BaseContext: func(_ *http.Request) (context.Context, error) {
				return bctx, nil
			},
		},
	)

	require.NotNil(t, s)
	assert.NotZero(t, s.log)
	assert.NotNil(t, s.router)
	assert.Equal(t, acceptOpts, s.opts.AcceptOptions)
	assert.Equal(t, int64(300), s.opts.ReadLimit)
	assert.Equal(t, time.Microsecond, s.opts.ResponseWriteTimeout)
	require.NotNil(t, s.opts.BaseContext)

	ctx, err = s.opts.BaseContext(nil)
	assert.NoError(t, err)
	assert.Same(t, bctx, ctx)
	assert.NotNil(t, s.conns)
	assert.NotNil(t, s.supv)
}

func Test_Server_Close(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Read(context.Background()) //nolint:errcheck,gosec // error provides no meaningful info
	}))
	defer hs.Close()

	wsConn := dial(t, hs.URL)

	var (
		callsMu sync.Mutex
		calls   int
	)

	c1 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() {
			callsMu.Lock()
			defer callsMu.Unlock()

			calls++
		},
	}
	c2 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() {
			callsMu.Lock()
			defer callsMu.Unlock()

			calls++
		},
	}
	c3 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() {
			callsMu.Lock()
			defer callsMu.Unlock()

			calls++
		},
	}
	s := Server{
		conns: map[*conn]struct{}{
			c1: {}, c2: {}, c3: {},
		},
		router: &Router{
			topics: map[string]*topic{
				"1": {
					subs: map[*conn][]map[string]string{
						c2: {{}, {}},
						c3: {{}},
					},
					supv: xync.NewSupervisor(),
				},
				"2": {
					subs: map[*conn][]map[string]string{
						c2: {{}, {}},
					},
					supv: xync.NewSupervisor(),
				},
				"3": {
					subs: map[*conn][]map[string]string{
						c1: {{}, {}},
						c3: {{}, {}},
					},
					supv: xync.NewSupervisor(),
				},
			},
		},
		supv: xync.NewSupervisor(),
	}

	s.Close()
	assert.Equal(t, 3, calls)
	assert.Empty(t, s.router.topics["1"].subs)
	assert.Empty(t, s.router.topics["2"].subs)
	assert.Empty(t, s.router.topics["3"].subs)
	assert.Empty(t, s.conns)
}

func Test_Server_CloseConn(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Read(context.Background()) //nolint:errcheck,gosec // error provides no meaningful info
	}))
	defer hs.Close()

	wsConn1 := dial(t, hs.URL)
	wsConn2 := dial(t, hs.URL)

	var (
		callsMu sync.Mutex
		calls   int
	)

	c1 := &conn{
		conn: wsConn1,
		ctx:  _connCtx,
		stop: func() {
			callsMu.Lock()
			defer callsMu.Unlock()

			calls++
		},
	}
	c2 := &conn{
		conn: wsConn2,
		ctx:  context.Background(),
		stop: func() {
			callsMu.Lock()
			defer callsMu.Unlock()

			calls++
		},
	}
	s := Server{
		conns: map[*conn]struct{}{
			c1: {}, c2: {},
		},
		router: &Router{
			topics: map[string]*topic{
				"1": {
					subs: map[*conn][]map[string]string{
						c1: {{}, {}},
						c2: {{}},
					},
				},
				"2": {
					subs: map[*conn][]map[string]string{
						c2: {{}, {}},
					},
				},
			},
		},
	}

	s.CloseConn(func(ctx context.Context) bool {
		v, ok := ctx.Value(_connCtxKey).(string)
		return ok && v == _connCtxValue
	})
	assert.Equal(t, 1, calls)
	assert.Len(t, s.router.topics["1"].subs, 1)
	assert.Len(t, s.router.topics["2"].subs, 1)
	assert.Len(t, s.conns, 1)
	require.NoError(t, c2.conn.Close(websocket.StatusNormalClosure, ""))
}

func Test_Server_closeConn(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		conn.Read(context.Background()) //nolint:errcheck,gosec // error provides no meaningful info
	}))
	defer hs.Close()

	wsConn := dial(t, hs.URL)

	var calls int

	c1 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() { calls++ },
	}
	c2 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() { calls++ },
	}
	c3 := &conn{
		conn: wsConn,
		ctx:  _connCtx,
		stop: func() { calls++ },
	}
	s := Server{
		conns: map[*conn]struct{}{
			c1: {}, c2: {}, c3: {},
		},
		router: &Router{
			topics: map[string]*topic{
				"1": {
					subs: map[*conn][]map[string]string{
						c2: {{}, {}},
						c3: {{}},
					},
				},
				"2": {
					subs: map[*conn][]map[string]string{
						c2: {{}, {}},
					},
				},
			},
		},
	}

	s.closeConn(c2)
	assert.Equal(t, 1, calls)
	require.Len(t, s.router.topics["1"].subs, 1)
	assert.Len(t, s.router.topics["1"].subs[c3], 1)
	assert.Empty(t, s.router.topics["2"].subs)
	assert.Len(t, s.conns, 2)
}

func Test_Server_ServeHTTP(t *testing.T) {
	s := &Server{
		conns:  make(map[*conn]struct{}),
		router: &Router{},
		opts: Options{
			BaseContext: func(_ *http.Request) (context.Context, error) {
				return nil, assert.AnError
			},
		},
		supv: xync.NewSupervisor(),
	}
	req := httptest.NewRequest("GET", "http://test.com/", http.NoBody)
	rec := httptest.NewRecorder()

	// context error
	s.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	// acception error
	s.opts.BaseContext = func(_ *http.Request) (context.Context, error) {
		return context.Background(), nil
	}
	req = httptest.NewRequest("GET", "http://test.com/", http.NoBody)
	rec = httptest.NewRecorder()

	s.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUpgradeRequired, rec.Code)

	// success
	hs := httptest.NewServer(s)

	t.Cleanup(func() {
		hs.Close()
	})

	wsConn := dial(t, hs.URL)

	// ServeHTTP accepts connections asynchronously, so we need to wait
	// for it to be accepted.
	assert.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) == 1
	}, time.Second, time.Millisecond*250)

	s.mu.RLock()
	for conn := range s.conns {
		assert.NotZero(t, conn.ctx.Value(_wsCtxID))
	}
	s.mu.RUnlock()

	require.NoError(t, wsConn.Close(websocket.StatusNormalClosure, ""))
	s.supv.Wait()
	assert.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) == 0
	}, time.Second, time.Millisecond*250)
}

//nolint:gocognit // there are a lot of cases that slightly differ from one another.
func Test_Server_startReader(t *testing.T) {
	inpCh := make(chan string)

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		ch := make(chan struct{})

		go func() {
			for {
				_, _, err := c.Read(context.Background())
				if err != nil {
					close(ch)
					return
				}
			}
		}()

		for {
			select {
			case <-ch:
				return
			case data := <-inpCh:
				err := c.Write(context.Background(), websocket.MessageText, []byte(data))
				if err != nil {
					return
				}
			}
		}
	}))

	c := &conn{
		conn:    dial(t, hs.URL),
		ctx:     context.Background(),
		stop:    func() {},
		writeCh: make(chan []byte),
	}

	t.Cleanup(func() {
		hs.Close()
		close(inpCh)

		if c.conn != nil {
			c.conn.Close(websocket.StatusNormalClosure, "") //nolint:gosec,errcheck // error provides no meaningful info
		}
	})

	s := Server{
		conns: map[*conn]struct{}{
			c:  {},
			{}: {},
		},
		router: &Router{
			topics: map[string]*topic{
				"update@configs": {
					pattern: pattern{
						topic: wsutil.Topic{
							Operation: "update",
							Path:      []string{"configs"},
						},
						path: []pathElem{
							{
								value: "configs",
							},
						},
					},
					subs: map[*conn][]map[string]string{
						{}: {},
					},
				},
			},
		},
		opts: Options{
			ResponseWriteTimeout: time.Second * 5,
		},
	}

	// invalid json
	go s.startReader(c)

	msg, err := json.Marshal(struct {
		ID    string `json:"id"`
		Topic string `json:"topic"`
	}{ID: "hello", Topic: "configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	require.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) == 1
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)

	// context timeout
	c = &conn{
		conn:    dial(t, hs.URL),
		ctx:     context.Background(),
		stop:    func() {},
		writeCh: make(chan []byte),
	}
	s.conns[c] = struct{}{}

	go s.startReader(c)

	s.opts.ResponseWriteTimeout = time.Nanosecond
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 300, Topic: "sub~activation@configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Never(t, func() bool {
		select {
		case <-c.writeCh:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*500)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)
	require.NoError(t, c.conn.Close(websocket.StatusNormalClosure, ""))
	require.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) == 1
	}, time.Second, time.Millisecond*250)

	s.opts.ResponseWriteTimeout = time.Second * 5

	// invalid topic
	c = &conn{
		conn:    dial(t, hs.URL),
		ctx:     context.Background(),
		stop:    func() {},
		writeCh: make(chan []byte),
	}
	s.conns[c] = struct{}{}

	go s.startReader(c)

	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 300, Topic: "configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":300, "success":false}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)

	// invalid descriptor
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 301, Topic: "hello~update@configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":301, "success":false}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)

	// sub - topic not found
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 302, Topic: "sub~activation@exchange"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":302, "success":false}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)

	// sub - success
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 303, Topic: "sub~update@configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":303, "success":true}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	require.Len(t, s.router.topics["update@configs"].subs, 2)
	assert.Len(t, s.router.topics["update@configs"].subs[c], 1)

	// unsub - topic not found
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 304, Topic: "unsub~activation@exchange"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":304, "success":true}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	require.Len(t, s.router.topics["update@configs"].subs, 2)
	assert.Len(t, s.router.topics["update@configs"].subs[c], 1)

	// unsub - success
	msg, err = json.Marshal(struct {
		ID    uint64 `json:"id"`
		Topic string `json:"topic"`
	}{ID: 305, Topic: "unsub~update@configs"})
	require.NoError(t, err)

	inpCh <- string(msg)

	assert.Eventually(t, func() bool {
		select {
		case data := <-c.writeCh:
			assert.JSONEq(t, `{"id":305, "success":true}`, string(data))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
	require.Never(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) < 2
	}, time.Second, time.Millisecond*250)
	assert.Len(t, s.router.topics["update@configs"].subs, 1)
}

func Test_Server_startWriter(t *testing.T) {
	resCh := make(chan []byte)

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		for {
			_, msg, err := c.Read(context.Background())
			if err != nil {
				return
			}

			resCh <- msg
		}
	}))
	defer hs.Close()

	wsConn := dial(t, hs.URL)
	connCtx, connCancel := context.WithCancel(context.Background())
	c := &conn{
		conn:    wsConn,
		ctx:     connCtx,
		stop:    func() {},
		writeCh: make(chan []byte),
	}
	s := Server{
		conns: map[*conn]struct{}{
			c:  {},
			{}: {},
		},
		router: &Router{
			topics: make(map[string]*topic),
		},
	}

	// success
	go s.startWriter(c)
	c.writeCh <- []byte(`{"msg":"hello"}`)

	assert.Eventually(t, func() bool {
		select {
		case msg := <-resCh:
			assert.Equal(t, `{"msg":"hello"}`, string(msg))
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)

	// conn cancelled
	connCancel()
	assert.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return len(s.conns) == 1
	}, time.Second, time.Millisecond*250)

	c.ctx = context.Background()

	// conn closed
	go s.startWriter(c)
	c.writeCh <- []byte(`{"msg":"hello"}`)

	assert.Never(t, func() bool {
		select {
		case <-resCh:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*250)
}

func Test_newConn(t *testing.T) {
	c := newConn(context.Background(), zerolog.Nop(), &websocket.Conn{})
	require.NotNil(t, c)
	assert.NotZero(t, c.log)
	assert.NotNil(t, c.conn)
	assert.NotNil(t, c.ctx)
	assert.NotNil(t, c.stop)
	assert.NotNil(t, c.writeCh)
}

func Test_conn_safeContext(t *testing.T) {
	c := conn{ctx: context.WithValue(context.Background(), contextKey(3), "test")}
	assert.Equal(t, c.ctx, c.safeContext())
}

func Test_conn_publish(t *testing.T) {
	out, b := testutil.NewBuffer()
	connCtx, connCancel := context.WithCancel(context.Background())
	c := conn{
		log:     zerolog.New(out),
		ctx:     connCtx,
		writeCh: make(chan []byte, 1),
	}

	// error
	c.publish(context.Background(), func() ([]byte, error) {
		return nil, errors.New("test")
	})
	require.NoError(t, out.Flush())
	assert.Contains(t, b.String(), `"message":"cannot encode websocket payload"`)
	assert.Contains(t, b.String(), `"error":"test"`)
	b.Reset()

	// cann ctx cancellation
	c.writeCh <- nil

	connCancel()
	c.publish(context.Background(), func() ([]byte, error) {
		return []byte(`{"msg":"hello"}`), nil
	})
	require.Len(t, c.writeCh, 1)
	<-c.writeCh
	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), `"message":"cannot encode websocket payload"`)
	assert.NotContains(t, b.String(), `"error":"test"`)
	b.Reset()

	// pub ctx cancellation
	c.writeCh <- nil
	c.ctx = context.Background()

	pubCtx, pubCancel := context.WithCancel(context.Background())
	pubCancel()

	c.publish(pubCtx, func() ([]byte, error) {
		return []byte(`{"msg":"hello"}`), nil
	})
	require.Len(t, c.writeCh, 1)
	<-c.writeCh
	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), `"message":"cannot encode websocket payload"`)
	assert.NotContains(t, b.String(), `"error":"test"`)
	b.Reset()

	// success
	c.publish(context.Background(), func() ([]byte, error) {
		return []byte(`{"msg":"hello"}`), nil
	})
	require.Len(t, c.writeCh, 1)
	assert.Equal(t, `{"msg":"hello"}`, string(<-c.writeCh))
	require.NoError(t, out.Flush())
	assert.NotContains(t, b.String(), `"message":"cannot encode websocket payload"`)
	assert.NotContains(t, b.String(), `"error":"test"`)
	b.Reset()
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil) //nolint:bodyclose // body closing is handled by the lib
	require.NoError(t, err)

	return conn
}
