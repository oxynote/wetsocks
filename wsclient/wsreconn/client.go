// Package wsreconn provides wsclient reconnection and resubscription logic.
package wsreconn

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/oxynote/wetsocks/internal/ctxutil"
	"github.com/oxynote/wetsocks/internal/timeutil"
	"github.com/oxynote/wetsocks/wsclient"
)

// Client wraps a WebSocket client and adds automatic
// reconnection as well as resubscription logic.
type Client struct {
	opts    ClientOptions
	log     *slog.Logger
	metrics ClientMetrics
	keeper  SubKeeper
	ws      *wsclient.Client

	procMu sync.Mutex
	proc   *timeutil.PeriodicExec

	// NOTE: Reconnection functions are not using
	// supervisor as we expect that some logic might
	// be critical to re-openning the connection.
	reconnMu  sync.RWMutex
	reconnFns []func(context.Context)
}

// ClientOptions contains a WebSocket client's options.
type ClientOptions struct {
	wsclient.Options

	// BaseContext returns a new context for each (re)connection and
	// (re)subscription attempt.
	// The default is context.Background().
	BaseContext func() context.Context

	// CheckAfter specifies how much time should pass between
	// repeated connection and subscription checks.
	// Note that subscriptions are checked each time data managed
	// by SubKeeper changes.
	// The default is 30 seconds.
	CheckAfter time.Duration
}

// NewClient creates a fresh instance of the Client.
// It initializes the client and opens a WebSocket connection.
// The context parameter is used only when openning the connection for
// the first time. Note that it is also combined with the base context
// produced by the function in the options.
func NewClient(
	ctx context.Context,
	logger *slog.Logger,
	metrics ClientMetrics,
	opts ClientOptions,
	keeper SubKeeper,
) (*Client, error) {
	if opts.RecoveryFunc == nil {
		opts.RecoveryFunc = func(_ any) {}
	}

	if opts.BaseContext == nil {
		opts.BaseContext = context.Background
	}

	if opts.CheckAfter <= 0 {
		opts.CheckAfter = time.Second * 30
	}

	s := &Client{
		opts:    opts,
		log:     logger,
		metrics: metrics,
		keeper:  keeper,
		ws:      wsclient.New(logger, metrics, opts.Options),
	}
	s.proc = timeutil.NewPeriodicExec(
		opts.CheckAfter,
		time.Second,
		s.process,
		opts.RecoveryFunc,
		false,
	)

	multiCtx, multiCancel := ctxutil.MultiContext(ctx, opts.BaseContext())
	defer multiCancel()

	s.metrics.IncWsConnectionAttempts()

	if err := s.ws.Open(multiCtx); err != nil {
		return nil, err
	}

	go s.proc.Start()

	s.keeper.OnChange(func(_ context.Context) {
		s.proc.Trigger()
	})

	return s, nil
}

// Close stops all the client-related processes.
func (c *Client) Close() {
	c.proc.Stop()
	c.ws.Close()
}

// OnRead sets the provided function to be executed on a new incoming
// JSON event. JSON data is not to be modified.
func (c *Client) OnRead(fn func(context.Context, json.RawMessage)) {
	c.ws.OnRead(fn)
}

// OnReconnect sets the provided function to be executed on a
// successful reconnection.
func (c *Client) OnReconnect(fn func(context.Context)) {
	c.reconnMu.Lock()
	c.reconnFns = append(c.reconnFns, fn)
	c.reconnMu.Unlock()
}

// process checks if the client's connection is not closed or any
// of the subscriptions unsent.
func (c *Client) process(ctx context.Context) {
	if !c.procMu.TryLock() {
		// we need to ensure that only one process is running
		// at a time, even if enough time passes for another
		// one to start
		return
	}

	defer c.procMu.Unlock()

	ctx, cancel := ctxutil.MultiContext(ctx, c.opts.BaseContext())
	defer cancel()

	if !c.ws.IsOpen() {
		c.keeper.ResetAll()
		c.metrics.IncWsConnectionAttempts()

		if err := c.ws.Open(ctx); err != nil {
			c.logWarn("cannot reopen the connection", err)

			return
		}

		for _, fn := range c.reconnFns {
			fn(ctx)
		}
	}

	pp, cooldown := c.keeper.Payloads()
	for i := range pp {
		p, confirm := pp[i].Payload()
		if p == nil {
			continue
		}

		c.metrics.IncWsWrites()

		var err error

		if confirm != nil {
			var res json.RawMessage

			res, err = c.ws.Send(ctx, p)
			if err != nil {
				c.logWarn("cannot send a payload", err)
			} else {
				confirm(res)
			}
		} else {
			err = c.ws.Write(ctx, p)
			if err != nil {
				c.logWarn("cannot write a payload", err)
			}
		}

		if errors.Is(err, wsclient.ErrConnClosed) {
			// the connection died mid-loop, so the remaining
			// payloads cannot be delivered until it is reopened;
			// a prompt re-check reopens it instead of waiting
			// for the next periodic one
			c.proc.Trigger()

			return
		}

		if cooldown <= 0 || i == len(pp)-1 {
			continue
		}

		t := time.NewTimer(cooldown)

		select {
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
					// NOCOV: since the timer is created
					// and stopped inside this function,
					// there is no way to stop it from
					// the outside to trigger this case.
				default:
				}
			}

			return
		case <-t.C:
		}
	}
}

// logWarn logs the provided message and error at the warning level.
// Context cancellation errors are skipped.
func (c *Client) logWarn(msg string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	c.log.Warn(msg, "error", err)
}

// SubKeeper is an interface that handles subscriptions and unsubscriptions.
//
//go:generate ../../scripts/codegen/mock -t both SubKeeper
type SubKeeper interface {
	// Payloads should return a slice of payloads that should
	// be sent through a WebSocket connection and a duration
	// value indicating how much time has to pass after each
	// write before a new one is sent.
	Payloads() ([]SubPayloader, time.Duration)

	// ResetAll should reset every subscription to an unconfirmed state.
	// It should not trigger any change functions.
	ResetAll()

	// OnChange should set the provided function to be executed
	// when subscription-related data changes.
	OnChange(fn func(context.Context))
}

// SubPayloader is an interface that handles a single payload's state.
//
//go:generate ../../scripts/codegen/mock -t internal SubPayloader
type SubPayloader interface {
	// Payload should return a payload that should be sent through
	// a WebSocket connection and a function that should be used
	// when confirming a response.
	// If the first return value is nil, the payload is not sent (i.e.,
	// it is skipped).
	// If the function is nil, the response should not expected.
	Payload() (any, func(json.RawMessage))
}

// ClientMetrics is an interface that handles reconnection-related metrics.
//
//go:generate ../../scripts/codegen/mock -t both ClientMetrics
type ClientMetrics interface {
	wsclient.Metrics

	// IncConnectionAttempts should increase the connection attempt
	// number.
	IncWsConnectionAttempts()

	// IncWsWrites should increase the number of writes to a connection.
	IncWsWrites()
}

// SubKeeperMetrics is an interface that handles subscription-related metrics.
//
//go:generate ../../scripts/codegen/mock -t both SubKeeperMetrics
type SubKeeperMetrics interface {
	// IncWsSubs should increase the subscription number.
	IncWsSubs()

	// DecWsSubs should decrease the subscription number.
	DecWsSubs()

	// IncWsSubConfirmations should increase the
	// subscription confirmation number.
	IncWsSubConfirmations()

	// IncWsUnsubConfirmations should decrease the
	// unsubscription confirmation number.
	IncWsUnsubConfirmations()
}
