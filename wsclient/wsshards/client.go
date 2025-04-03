// Package wsshards allows to create multiple
// WebSocket connections.
package wsshards

import (
	"context"
	"encoding/json"

	"github.com/jellydator/wetsocks/wsclient/wsreconn"
	"github.com/rs/zerolog"
)

// Client wraps multiple WebSocket clients and adds automatic
// sharding logic (it spreads topics across multiple connections).
type Client struct {
	ws []client
}

// NewClient creates a fresh instance of the Client.
// It initializes the reconnection client.
// The number of shards is determined by the number of
// sub keepers in the provided store.
func NewClient(
	ctx context.Context,
	logger zerolog.Logger,
	metrics wsreconn.ClientMetrics,
	opts wsreconn.ClientOptions,
	store SubKeeperStore,
) (*Client, error) {
	client := &Client{}

	for i, keeper := range store.SubKeepers() {
		rc, err := wsreconn.NewClient(
			ctx,
			logger.With().Int("id", i+1).Logger(),
			metrics,
			opts,
			keeper,
		)
		if err != nil {
			client.Close()

			return nil, err
		}

		client.ws = append(client.ws, rc)
	}

	return client, nil
}

// Close stops all the client-related processes.
func (c *Client) Close() {
	for _, ws := range c.ws {
		ws.Close()
	}
}

// OnRead sets the provided function to be executed on a new incoming
// JSON event. JSON data is not to be modified.
func (c *Client) OnRead(fn func(context.Context, json.RawMessage)) {
	for _, ws := range c.ws {
		ws.OnRead(fn)
	}
}

// client is an interface that handles message reading and closure.
//
//go:generate ../../scripts/codegen/mock -t internal client
type client interface {
	// OnRead should set the provided function to be executed
	// on a new incoming message.
	OnRead(fn func(context.Context, json.RawMessage))

	// Close should stop all the client-related processes.
	Close()
}

// SubKeeperStore is an interface that handles sub keeper
// storage and retrieval.
//
//go:generate ../../scripts/codegen/mock -t internal SubKeeperStore
type SubKeeperStore interface {
	// SubKeepers should return all available
	// sub keepers.
	SubKeepers() []wsreconn.SubKeeper
}
