## Wetsocks - a WebSocket toolkit with topic-based pub/sub, reconnection and sharding

[![Go Reference](https://pkg.go.dev/badge/github.com/oxynote/wetsocks.svg)](https://pkg.go.dev/github.com/oxynote/wetsocks)
[![Build Status](https://github.com/oxynote/wetsocks/actions/workflows/qa.yml/badge.svg)](https://github.com/oxynote/wetsocks/actions/workflows/qa.yml)

Wetsocks is a set of packages built on top of
[coder/websocket](https://github.com/coder/websocket) that covers both ends of
a JSON WebSocket API: a server that routes subscriptions to topics, and a
client that keeps a connection (or a fleet of them) open and subscribed.

## Features
- Topic router with path placeholders (`update@{exchange}.tickers.{symbol}`)
- Publishing and dropping of many or individual subscribers
- Subscription middlewares and `OnSub`/`OnUnsub`/`OnFirstSub`/`OnLastUnsub` event handlers
- Centralized connection reading and writing, with optional serial (ordered) event processing
- Request-response correlation by message ID
- Automatic pinging and connection recovery
- Automatic reconnection and resubscription
- Subscription sharding across multiple connections
- Solo and grouped subscription keepers with automatic or manual confirmations
- Metrics interfaces
- Panic recovery
- Thread safety

## Installation
```
go get github.com/oxynote/wetsocks
```

## Packages
- `wsserver` — server-side connection pool, topic router and event publishing.
- `wsclient` — a single client-side connection: reads, writes, requests, pings.
- `wsclient/wsreconn` — wraps `wsclient` with reconnection and resubscription.
- `wsclient/wsshards` — wraps `wsreconn` and spreads subscriptions across
  multiple connections.
- `wsutil` — topic parsing and WebSocket error helpers.

## Topics
Both ends of the library speak the same topic format:
```
[descriptor~]operation@path.to.resource
```
The operation and the path describe the resource, while the descriptor
describes what is being done with it. Four descriptors are used: `sub` and
`unsub` are sent by the client, `pub` and `drop` are sent by the server.

A path element wrapped in curly braces (`{symbol}`) is a placeholder: the
server binds a topic pattern once and matches every concrete topic against it,
exposing the captured values as topic parameters.

The wire protocol follows from that. A client subscribes and receives a
response carrying the same ID:
```json
{ "id": 1, "topic": "sub~update@binance.tickers.btc-usdt" }
{ "id": 1, "success": true }
```
The server then publishes events into the topic and may drop subscribers
from it:
```json
{ "topic": "pub~update@binance.tickers.btc-usdt", "payload": { "price": "42000" } }
{ "topic": "drop~update@binance.tickers.btc-usdt", "reason": "unknown symbol" }
```

Topics can be parsed and assembled with `wsutil`, which is handy on the
client side where topic strings are built from values rather than written
out by hand:
```go
tpc := wsutil.Topic{
	Operation: "update",
	Path:      []string{"binance", "tickers", "btc-usdt"},
}

tpc.String() // update@binance.tickers.btc-usdt
```

## Usage

### Server
The main types of `wsserver` are `Server` and `Router`. The router registers
topic patterns and hands each of them to a binder, the server accepts
connections and routes their subscription requests:
```go
func (s *Server) wsRouter() *wsserver.Router {
	r := wsserver.NewRouter()

	r.BindFunc("ping@version", s.bindVersionPing)
	r.BindFunc("update@{exchange}.symbols", s.bindSymbols)
	r.BindFunc("update@{exchange}.tickers.{symbol}", s.bindTickers)

	return r
}

func (s *Server) init() {
	s.ws = wsserver.New(s.log, s.wsRouter(), wsserver.Options{
		AcceptOptions: websocket.AcceptOptions{
			OriginPatterns: s.originPatterns,
		},
		ReadLimit: 1 << 16,
	})
}
```

`Server` implements `http.Handler`, so it can be mounted on any route of an
existing mux, and satisfies `wsserver.Pool`, which is the interface to keep in
struct fields. Note that `Bind`/`BindFunc` panic on an invalid or an already
registered topic: they are meant to be called during start-up, the same way
HTTP routes are registered.

A binder is called once per topic pattern, not once per subscriber, and it is
the only place where the topic's control instance is handed out. Everything
else — publishing, dropping, subscription events — goes through it:
```go
// bindTickers publishes ticker updates to the subscribers of a single
// exchange and symbol.
func (s *Server) bindTickers(tpc wsserver.Topic) {
	// the pattern is bound once, so a filter is needed to select the
	// subscribers that asked for this exact exchange and symbol
	filter := func(exch, sym string) func(context.Context, string) bool {
		return func(ctx context.Context, _ string) bool {
			return wsserver.TopicParamFromContext(ctx, "exchange") == exch &&
				wsserver.TopicParamFromContext(ctx, "symbol") == sym
		}
	}

	// OnFirstSub cannot be used here, because its context doesn't
	// carry any topic parameters
	tpc.OnSub(func(ctx context.Context) {
		var (
			exch = wsserver.TopicParamFromContext(ctx, "exchange")
			sym  = wsserver.TopicParamFromContext(ctx, "symbol")
		)

		ticker, err := s.tickers.Fetch(ctx, exch, sym)
		if err != nil {
			// the subscription is rejected with a reason instead of
			// closing the whole connection
			tpc.DropOne(ctx, err.Error())

			return
		}

		// the new subscriber receives the current state right away
		tpc.PublishOne(ctx, ticker)

		s.tickers.Watch(exch, sym, func(ctx context.Context, t Ticker) {
			tpc.PublishMany(ctx, t, filter(exch, sym))
		})
	})

	tpc.OnUnsub(func(ctx context.Context) {
		s.tickers.Unwatch(
			wsserver.TopicParamFromContext(ctx, "exchange"),
			wsserver.TopicParamFromContext(ctx, "symbol"),
		)
	})
}
```

`OnSub` and `OnUnsub` fire for every subscriber, so a topic that is backed by
an upstream resource usually needs a reference counter per parameter
combination: the upstream subscription is created on the first subscriber and
released on the last one. For topics without placeholders that bookkeeping is
already done by `OnFirstSub` and `OnLastUnsub`:
```go
func (s *Server) bindVersionPing(tpc wsserver.Topic) {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	cr := cron.New(cron.WithSeconds())

	cr.AddFunc("*/5 * * * * *", func() { //nolint:errcheck
		tpc.PublishMany(ctx, version{Version: buildinfo.Version}, nil)
	})

	tpc.OnFirstSub(func(_ context.Context) {
		// this context has to outlive the subscriber that
		// triggered the event
		ctx, cancel = context.WithCancel(context.Background())

		cr.Start()
	})

	tpc.OnLastUnsub(func(_ context.Context) {
		cancel()
		cr.Stop()
	})
}
```

Middlewares are called before every publish with the connection's context and
the formatted topic. They are a per-topic filter, but since they see every
outgoing event, they also work as a place for instrumentation. Wrapping
`BindFunc` keeps that out of the binders:
```go
// bind registers a topic and counts every event published into it.
func bind(r *wsserver.Router, count func(pattern, topic string)) func(
	string,
	wsserver.BinderFunc,
	...func(context.Context, string) bool,
) {
	return func(
		pattern string,
		binderFn wsserver.BinderFunc,
		middlewares ...func(context.Context, string) bool,
	) {
		r.BindFunc(pattern, binderFn, append(middlewares,
			func(_ context.Context, rawTopic string) bool {
				count(pattern, rawTopic)

				return true
			},
		)...)
	}
}
```

Each connection gets a base context, which is a good place for authentication
data. The context outlives the HTTP handler, so it should be built from
`context.Background()` and not from the request's context. Returning an error
rejects the connection before the upgrade:
```go
srv := wsserver.New(log, router, wsserver.Options{
	ResponseWriteTimeout: 10 * time.Second,
	BaseContext: func(r *http.Request) (context.Context, error) {
		user, err := authenticate(r)
		if err != nil {
			return nil, err
		}

		return context.WithValue(context.Background(), _userKey, user), nil
	},
})
```

That context is then available in middlewares, subscription events and
connection filters, which is what makes revoking access possible without
restarting anything:
```go
router.BindFunc("update@{exchange}.tickers.{symbol}", s.bindTickers,
	func(ctx context.Context, _ string) bool {
		return userFromContext(ctx).HasMarketData
	},
)

srv.CloseConn(func(ctx context.Context) bool {
	return userFromContext(ctx).ID == revokedID
})
```

### Client
`wsclient.Client` handles a single connection. Reading, writing and pinging
happen on dedicated goroutines, so the connection is not tied to the context
of the caller. Every constructor in this library expects a metrics
implementation; the interfaces are small and a no-op struct is a valid choice:
```go
type metrics struct{}

func (metrics) IncWsOpenConnections()    {}
func (metrics) DecWsOpenConnections()    {}
func (metrics) IncWsConnectionAttempts() {}
func (metrics) IncWsWrites()             {}
func (metrics) IncWsSubs()               {}
func (metrics) DecWsSubs()               {}
func (metrics) IncWsSubConfirmations()   {}
func (metrics) IncWsUnsubConfirmations() {}
```

Once the connection is open, `OnRead` receives every incoming event, `Write`
sends a fire-and-forget payload and `Send` sends a request and waits for the
response with a matching ID:
```go
client := wsclient.New(log, metrics{}, wsclient.Options{
	URL:          "wss://example.com/ws",
	PingInterval: 15 * time.Second,
	ReadLimit:    1 << 24, // approx 16.8mb
})
defer client.Close()

client.OnRead(func(ctx context.Context, data json.RawMessage) {
	log.Info("event", "data", string(data))
})

if err := client.Open(ctx); err != nil {
	return err
}

// the ID is assigned by the client and matched against the response
resp, err := client.Send(ctx, map[string]any{
	"topic": "sub~update@binance.tickers.btc-usdt",
})
```

By default every incoming event is handed to the read functions on a separate
goroutine, and no ordering is guaranteed. When the order matters, the
`SerialReads` option buffers events and processes them one at a time in their
arrival order.

Most real connections need a little more than a URL. `DialOptions` is embedded
directly, so the HTTP client, headers and compression settings are set the
same way as in `coder/websocket`; `URLFunc` covers endpoints that hand out a
single-use URL or token; `Cron` registers repeated tasks that are started and
stopped together with the connection:
```go
cr := cron.New()

// some servers expect the listen key to be refreshed periodically
cr.AddFunc("@every 30m", func() { //nolint:errcheck
	c.refreshWsKey(context.Background(), key)
})

opts := wsclient.Options{
	DialOptions: websocket.DialOptions{
		HTTPClient:      c.http,
		CompressionMode: websocket.CompressionDisabled,
	},
	ReadLimit: 1 << 24,
	URLFunc: func() (string, error) {
		return c.createWsURL(ctx)
	},
	Cron:         cr,
	RecoveryFunc: c.processRecoveryValue,
}
```

### Reconnection and resubscription
`wsreconn.Client` wraps `wsclient` and periodically checks whether the
connection is still open and whether all subscriptions have been delivered.
Reopening the connection resets every subscription to an unconfirmed state, so
they are sent again automatically, and the checks are triggered immediately
whenever subscription data changes.

The client knows nothing about the format of the subscription messages: that
is the job of a formatter, which produces the payloads and recognizes the
server's confirmations. A client of a `wsserver` API looks like this:
```go
type wsSubFormatter struct {
	log *slog.Logger
}

func (w *wsSubFormatter) SubMessage(topic string) any {
	return struct {
		Topic string `json:"topic"`
	}{
		Topic: fmt.Sprintf("sub~%s", topic),
	}
}

func (w *wsSubFormatter) UnsubMessage(topic string) any {
	return struct {
		Topic string `json:"topic"`
	}{
		Topic: fmt.Sprintf("unsub~%s", topic),
	}
}

func (w *wsSubFormatter) ConfirmSub(topic string, data json.RawMessage) bool {
	var resp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(data, &resp); err != nil || !resp.Success {
		w.log.Warn("cannot confirm a subscription event", "topic", topic)

		return false
	}

	return true
}

func (w *wsSubFormatter) ConfirmUnsub(topic string, data json.RawMessage) bool {
	return w.ConfirmSub(topic, data)
}
```

A `SubKeeper` holds the subscriptions and their states, and `SoloSubKeeper`
sends one payload per topic. The cooldown keeps bursts of subscriptions within
the server's rate limits, while `CheckAfter` sets how often the connection and
the subscriptions are re-checked:
```go
const (
	_wsReadLimitBytes = 1 << 24 // approx 16.8mb

	_wsCooldownBetweenStatusChecks = time.Second * 15
	_wsCooldownBetweenWrites       = time.Millisecond * 100
)

func (c *Client) initWsClient(ctx context.Context) (err error) {
	c.ws.subs = wsreconn.NewSoloSubKeeper(
		c.metrics,
		&wsSubFormatter{log: c.log},
		_wsCooldownBetweenWrites,
		false, // confirmations are handled by the formatter
	)

	c.ws.client, err = wsreconn.NewClient(ctx,
		c.log,
		c.metrics,
		wsreconn.ClientOptions{
			Options: wsclient.Options{
				DialOptions: websocket.DialOptions{
					HTTPClient: c.http,
				},
				ReadLimit:    _wsReadLimitBytes,
				URL:          c.ws.url,
				RecoveryFunc: c.processRecoveryValue,
			},
			// a fresh context for every (re)connection and
			// (re)subscription attempt
			BaseContext: func() context.Context {
				return withRetry(context.Background())
			},
			CheckAfter: _wsCooldownBetweenStatusChecks,
		},
		c.ws.subs,
	)
	if err != nil {
		c.ws.subs = nil
		c.ws.client = nil

		return err
	}

	c.setupWs()

	return nil
}
```

Subscriptions are managed through the keeper and can be added or removed at
any time, including while the connection is down — they are sent as soon as it
is back up:
```go
func (c *Client) setupWs() {
	c.ws.client.OnRead(c.onRead)

	// global streams are opened once and kept for the client's lifetime
	c.ws.subs.Subscribe("ping@version")
	c.ws.subs.Subscribe(newWsTopic(c.exchange, "symbols").String())

	// per-symbol streams follow the local subscribers
	c.events.tickers.OnFirstSub(func(_ context.Context, sym Symbol) {
		c.ws.subs.Subscribe(newWsTopic(c.exchange, "tickers", sym.String()).String())
	})
	c.events.tickers.OnLastUnsub(func(_ context.Context, sym Symbol) {
		c.ws.subs.Unsubscribe(newWsTopic(c.exchange, "tickers", sym.String()).String())
	})
}
```

Incoming events are dispatched by their topic. Drops are worth handling
explicitly: the server has already removed the subscription, so
`UnsubscribeLocal` forgets it without sending an unsubscription message back:
```go
func (c *Client) onRead(_ context.Context, raw json.RawMessage) {
	var event struct {
		Topic   string          `json:"topic"`
		Payload json.RawMessage `json:"payload"`
		Reason  string          `json:"reason"`
	}

	if err := json.Unmarshal(raw, &event); err != nil {
		c.log.Warn("invalid WebSocket payload", "error", err)

		return
	}

	tpc, err := wsutil.ParseTopic(event.Topic)
	if err != nil {
		c.log.Warn("received invalid WebSocket topic", "error", err)

		return
	}

	if tpc.Descriptor == "drop" {
		c.log.Info("the subscription has been dropped", "reason", event.Reason)

		tpc.Descriptor = ""
		c.ws.subs.UnsubscribeLocal(tpc.String())

		return
	}

	// the first element of the path is the exchange name
	switch {
	case tpc.Operation == "ping":
		return // heartbeat
	case len(tpc.Path) == 3 && tpc.Path[1] == "tickers":
		c.handleWsTicker(tpc.Path[2], event.Payload)
	default:
		c.log.Warn("received unknown WebSocket topic", "topic", tpc.String())
	}
}
```

### Grouped subscriptions and manual confirmations
Third-party APIs rarely accept one message per topic. `GroupedSubKeeper`
subscribes many keys under a single topic and lets the formatter pack them
into one payload. It also counts subscriptions per key, so a key stays
subscribed until it is unsubscribed as many times as it was subscribed:
```go
keeper := wsreconn.NewGroupedSubKeeper[Symbol](
	metrics,
	&wsSubFormatter{client: c},
	_wsCooldownBetweenWrites,
	true, // confirmations are performed manually
)
defer keeper.Close()

keeper.Subscribe("ticker", btcUSDT, ethUSDT)
keeper.Unsubscribe("ticker", ethUSDT)

syms := keeper.TopicKeys("ticker") // confirmed keys only

// the formatter packs the whole group into a single message
func (w *wsSubFormatter) SubMessage(topic string, syms []Symbol) any {
	return w.makeMsg("subscribe", topic, syms)
}
```

The last constructor parameter switches confirmations to manual mode. It is
meant for servers that acknowledge subscriptions outside of the
request-response flow — usually as a snapshot of everything that is currently
subscribed. In that mode the formatter's `ConfirmSub`/`ConfirmUnsub` are never
called, and the keeper is updated from the event handler instead:
```go
func (c *Client) handleWsSubscriptions(subs wsSubscriptions) {
	syms := c.symbolsFromRaw(subs.Tickers)

	// everything the server lists is subscribed
	c.ws.subs.ConfirmSub(_wsTickerChannel, syms...)

	// and, inverted, everything it does not list is not
	c.ws.subs.ConfirmUnsub(_wsTickerChannel, true, syms...)
}
```

### Sharding
When a single connection cannot hold all the subscriptions, `wsshards` spreads
them across several. The number of shards is determined by the store, and each
shard is a full `wsreconn` client with its own logger and metrics — hence the
`connectionNumber` passed into the metrics constructors:
```go
const _wsShards = 3

func (c *Client) initWsClient(ctx context.Context) (err error) {
	c.ws.subs = wsshards.NewSoloSubKeeperStore(
		func(connectionNumber int) wsreconn.SubKeeperMetrics {
			return newWsMetrics(c.exchange, connectionNumber)
		},
		&wsSubFormatter{log: c.log},
		_wsCooldownBetweenWrites,
		true,
		_wsShards,
	)

	c.ws.client, err = wsshards.NewClient(ctx,
		c.log,
		func(connectionNumber int) wsreconn.ClientMetrics {
			return newWsMetrics(c.exchange, connectionNumber)
		},
		wsreconn.ClientOptions{
			Options: wsclient.Options{
				ReadLimit: _wsReadLimitBytes,
				URL:       c.ws.url,
			},
			CheckAfter: _wsCooldownBetweenStatusChecks,
		},
		c.ws.subs,
	)
	if err != nil {
		return err
	}

	// read functions are registered on every shard
	c.ws.client.OnRead(c.onRead)

	// the topic is hashed to a shard, so the same topic always lands
	// on the same connection
	c.ws.subs.Subscribe(newWsTopic(c.exchange, "tickers", "btc-usdt").String())

	return nil
}
```

The store mirrors the keeper's API, so switching a client from a single
connection to several is mostly a constructor change.
`GroupedSubKeeperStore` shards by key instead of by topic and, for that
reason, expects a formatter with an additional `KeyHash` method:
```go
func (w *wsSubFormatter) KeyHash(sym Symbol) int {
	var hash int

	for _, r := range sym.String() {
		hash += int(r)
	}

	return hash
}
```

## Development
```
make test   # unit tests with a coverage threshold
make lint   # golangci-lint
make qa     # both
```

The minimum coverage can be overridden with the `WETSOCKS_TEST_MIN_COV`
environment variable. Mocks are generated with `go generate ./...`.

## License
[MIT](LICENSE)
