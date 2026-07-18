// Package wsclient provides client-side WebSocket handling functionality.
package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/davseby/wetsocks/wsutil"
	"github.com/jellydator/purse/util/logutil"
	"github.com/jellydator/xync"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	_defaultReadLimit           = 1 << 15 // approx 32kb
	_defaultReqID        uint64 = 1
	_defaultPingInterval        = 30 * time.Second
	_serialReadsBuffer          = 512
)

var (
	// ErrConnOpen is returned when a client attempts to open a new
	// websocket connection while another connection is still open.
	ErrConnOpen = errors.New("connection is already open")

	// ErrConnClosed is returned when there is no active websocket
	// connection.
	ErrConnClosed = errors.New("connection is closed")
)

// Client handles an active client-side WebSocket connection.
type Client struct {
	opts    Options
	log     zerolog.Logger
	supv    *xync.Supervisor
	metrics Metrics

	stop   context.CancelFunc
	connMu sync.RWMutex
	conn   *websocket.Conn

	readMu  sync.RWMutex
	readFns []func(context.Context, json.RawMessage)
	writeCh chan json.RawMessage

	// readCh buffers incoming events for serial processing.
	// It is nil unless the SerialReads option is used.
	readCh chan json.RawMessage

	// serialOnce guards the serial processor start-up.
	serialOnce sync.Once

	req struct {
		mu      sync.RWMutex
		nextID  uint64
		respFns map[uint64]func(json.RawMessage)
	}
}

// Options contains client-side WebSocket connection's options.
type Options struct {
	websocket.DialOptions

	// URL specifies the target URL of the dial request.
	URL string

	// URLFunc is a function that returns the target URL of the dial request.
	// It is used when the URL is not known in advance.
	URLFunc func() (string, error)

	// ReadLimit specifies the max number of bytes to read for
	// a single message.
	// The default is 32kb.
	ReadLimit int64

	// PingInterval specifies the interval between ping requests.
	PingInterval time.Duration

	// Cron specifies repeated tasks that should be executed alongside
	// the WebSocket connection (e.g., manual ping/keep alive requests).
	// The cron instance is started and stopped each time a
	// new WebSocket connection is opened (via the Open method) and
	// closed (via the Close method).
	Cron *cron.Cron

	// RecoveryFunc is used to handle panics.
	// An empty func is used if it's set to nil.
	RecoveryFunc func(any)

	// SerialReads specifies whether the incoming events should be
	// processed one at a time in their arrival order. When disabled,
	// each incoming event is processed on a separate goroutine and
	// no ordering guarantees are provided.
	// Note that when the internal event buffer fills up (e.g., when
	// the read functions cannot keep up with the event flow), reading
	// from the connection is paused until there is space in the
	// buffer again.
	// Request responses (see the Send method) are always processed
	// on separate goroutines.
	SerialReads bool
}

// New creates a fresh instance of the WebSocket client.
func New(logger zerolog.Logger, metrics Metrics, opts Options) *Client {
	if opts.ReadLimit == 0 {
		opts.ReadLimit = _defaultReadLimit
	}

	if opts.RecoveryFunc == nil {
		opts.RecoveryFunc = func(_ any) {}
	}

	c := &Client{
		log:  logger,
		opts: opts,
		supv: xync.NewSupervisor(
			xync.WithSupervisorRecovery(opts.RecoveryFunc),
		),
		metrics: metrics,
		writeCh: make(chan json.RawMessage),
	}
	c.req.nextID = _defaultReqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))

	if opts.SerialReads {
		c.readCh = make(chan json.RawMessage, _serialReadsBuffer)
	}

	return c
}

// IsOpen determines whether the WebSocket connection is open or not.
func (c *Client) IsOpen() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	return c.isOpen()
}

// isOpen determines whether the websocket connection is open or not.
// Not concurrently safe.
func (c *Client) isOpen() bool {
	return c.conn != nil
}

// Open opens a new WebSocket connection. If there is a connection already
// open, an error is returned.
func (c *Client) Open(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.isOpen() {
		return ErrConnOpen
	}

	url := c.opts.URL

	if c.opts.URLFunc != nil {
		var err error

		url, err = c.opts.URLFunc()
		if err != nil {
			return err
		}
	}

	conn, _, err := websocket.Dial(ctx, url, &c.opts.DialOptions) //nolint:bodyclose // the body is closed automatically (docs)
	if err != nil {
		return err
	}

	conn.SetReadLimit(c.opts.ReadLimit)
	c.conn = conn

	// we are creating a global/lifetime context, so we don't need
	// to wrap the Open-dedicated ctx
	var clientCtx context.Context

	clientCtx, c.stop = context.WithCancel(context.Background())

	if c.readCh != nil {
		c.serialOnce.Do(func() {
			// the serial processor is started only once and uses
			// the supervisor's context because its lifetime must
			// match the client's and not the connection's: events
			// that are still buffered when the connection drops
			// have to be processed before the events of any
			// subsequent connection.
			c.supv.Go(c.startSerialProcessor)
		})
	}

	c.supv.Go(func(_ context.Context) {
		// we use the context that was created specifically
		// for the connection because startReader() handles its
		// closing. Also, the lifetime of supv may be longer than the
		// connection's (supv is closed only in Close(), whereas
		// the connection is also closed in reset()).
		c.startReader(clientCtx)
	})
	c.supv.Go(func(_ context.Context) {
		// we use the context that was created specifically
		// for the connection because startWriter() handles its
		// closing. Also, the lifetime of supv may be longer than the
		// connection's (supv is closed only in Close(), whereas
		// the connection is also closed in reset()).
		c.startWriter(clientCtx)
	})
	c.supv.Go(func(_ context.Context) {
		// we use the context that was created specifically
		// for the connection because startPinger() handles its
		// closing. Also, the lifetime of supv may be longer than the
		// connection's (supv is closed only in Close(), whereas
		// the connection is also closed in reset()).
		c.startPinger(clientCtx)
	})

	if c.opts.Cron != nil {
		c.opts.Cron.Start()
	}

	c.metrics.IncWsOpenConnections()

	return nil
}

// startReader handles reading from the active WebSocket connection.
// Reading is handled centrally and (usually) on a separate goroutine,
// because the context's cancellation could shut down the whole websocket
// connection: https://github.com/coder/websocket/issues/242#issuecomment-633182220
func (c *Client) startReader(ctx context.Context) {
	defer logutil.Recover(
		c.log,
		logutil.NewRecoveryPlan("cannot continue reading from the active connection"),
	)
	defer c.reset()

	for {
		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()

		if conn == nil {
			return
		}

		var data json.RawMessage

		// read validates json structure as well
		if err := wsjson.Read(ctx, conn, &data); err != nil {
			if wsutil.IsClosure(err) {
				return
			}

			wsutil.LogError(c.log, err, true)

			continue
		}

		id := gjson.GetBytes(data, "id").Uint()
		if id >= _defaultReqID {
			c.req.mu.RLock()
			respFn := c.req.respFns[id]
			c.req.mu.RUnlock()

			if respFn != nil {
				c.supv.Go(func(_ context.Context) {
					// respFns don't depend on the
					// supervisor's context because their
					// logic is static and visible only
					// in this package (see the Send
					// method). Also, it cannot 'hang'
					// because the response channel is
					// being constantly read in the Send
					// method.
					respFn(data)
				})

				continue
			}
		}

		if c.readCh != nil {
			select {
			case <-ctx.Done():
				return
			case c.readCh <- data:
			}

			continue
		}

		c.readMu.RLock()
		for _, fn := range c.readFns {
			fn := fn

			c.supv.Go(func(gctx context.Context) {
				fn(gctx, data)
			})
		}
		c.readMu.RUnlock()
	}
}

// startSerialProcessor executes read functions on buffered incoming
// events one event at a time, preserving the arrival order.
func (c *Client) startSerialProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-c.readCh:
			func() {
				// panics are recovered per event so that a
				// single faulty event wouldn't kill the
				// processing of all subsequent ones.
				defer func() {
					if v := recover(); v != nil {
						c.opts.RecoveryFunc(v)
					}
				}()

				c.readMu.RLock()
				defer c.readMu.RUnlock()

				for _, fn := range c.readFns {
					fn(ctx, data)
				}
			}()
		}
	}
}

// startWriter handles writing to the active WebSocket connection.
// Writing is handled centrally and (usually) on a separate goroutine,
// because the context's cancellation could shut down the whole websocket
// connection: https://github.com/coder/websocket/issues/242#issuecomment-633182220
func (c *Client) startWriter(ctx context.Context) {
	defer logutil.Recover(
		c.log,
		logutil.NewRecoveryPlan("cannot continue writing to the active connection"),
	)
	defer c.reset()

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-c.writeCh:
			c.connMu.RLock()
			conn := c.conn
			c.connMu.RUnlock()

			if conn == nil {
				return
			}

			// although we should be using wsjon.Write here, it
			// seems that not all servers work with it (namely,
			// binance), therefore, we are marshaling
			// it by ourselves and sending the data via the
			// connection.
			err := conn.Write(ctx, websocket.MessageText, data)
			if err != nil {
				if wsutil.IsClosure(err) {
					return
				}

				// NOCOV: errors unrelated to closure
				// are hard to simulate here
				wsutil.LogError(c.log, err, false)
			}
		}
	}
}

// startPinger handles pinging to the active WebSocket connection.
func (c *Client) startPinger(ctx context.Context) {
	defer logutil.Recover(
		c.log,
		logutil.NewRecoveryPlan("cannot continue pinging to the active connection"),
	)
	defer c.reset()

	pingInterval := c.opts.PingInterval
	if pingInterval == 0 {
		pingInterval = _defaultPingInterval
	}

	tc := time.NewTicker(pingInterval)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tc.C:
			c.connMu.RLock()
			conn := c.conn
			c.connMu.RUnlock()

			if conn == nil {
				return
			}

			err := conn.Ping(ctx)
			if err != nil {
				if wsutil.IsClosure(err) {
					return
				}

				// NOCOV: errors unrelated to closure
				// are hard to simulate here
				wsutil.LogError(c.log, err, false)
			}
		}
	}
}

// Close closes the active WebSocket connection.
// If the connection is already closed, this method is no-op.
func (c *Client) Close() {
	c.reset()
	c.supv.CloseAndWait()
}

// reset closes the active WebSocket connection and resets other
// relevant data fields to their defaults.
func (c *Client) reset() {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if !c.isOpen() {
		return
	}

	c.conn.Close(websocket.StatusNormalClosure, "") //nolint:errcheck,gosec // most errors are usually false positives here
	c.stop()

	c.conn = nil
	c.stop = nil

	c.req.mu.Lock()
	c.req.nextID = _defaultReqID
	c.req.respFns = make(map[uint64]func(json.RawMessage))
	c.req.mu.Unlock()

	if c.opts.Cron != nil {
		c.opts.Cron.Stop()
	}

	c.metrics.DecWsOpenConnections()
}

// OnRead sets the provided function to be executed on a new incoming
// JSON event. JSON data is not to be modified.
func (c *Client) OnRead(fn func(context.Context, json.RawMessage)) {
	c.readMu.Lock()
	c.readFns = append(c.readFns, fn)
	c.readMu.Unlock()
}

// Write writes the provided data to the active WebSocket connection.
func (c *Client) Write(ctx context.Context, payload any) error {
	if !c.IsOpen() {
		return ErrConnClosed
	}

	d, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return c.write(ctx, json.RawMessage(d))
}

// write writes the provided data to the active WebSocket connection.
func (c *Client) write(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.writeCh <- json.RawMessage(payload):
		return nil
	}
}

// Send sends a request to the active WebSocket connection and waits for a
// response.
// Each request has a unique uint64 as an ID, which means that the response
// must have the same ID as well.
func (c *Client) Send(ctx context.Context, payload any) (json.RawMessage, error) {
	if !c.IsOpen() {
		return nil, ErrConnClosed
	}

	rawReq, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	respCh := make(chan json.RawMessage)

	c.req.mu.Lock()
	id := c.req.nextID
	c.req.respFns[id] = func(resp json.RawMessage) {
		select {
		case <-ctx.Done():
		case respCh <- resp:
		}
	}
	c.req.nextID++
	c.req.mu.Unlock()

	rawReq, err = sjson.SetBytes(rawReq, "id", id)
	if err != nil {
		// NOCOV: an error is never returned, because the path
		// is static and always valid
		return nil, err
	}

	defer func() {
		c.req.mu.Lock()
		delete(c.req.respFns, id)
		c.req.mu.Unlock()
	}()

	if err = c.write(ctx, rawReq); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp, nil
	}
}

// Metrics is an interface that should be used to track the status metrics
// of a connection.
//
//go:generate ../scripts/codegen/mock -t internal Metrics
type Metrics interface {
	// IncWsOpenConnections should increase the number of open
	// connections.
	IncWsOpenConnections()

	// DecWsOpenConnections should increase the number of closed
	// connections.
	DecWsOpenConnections()
}
