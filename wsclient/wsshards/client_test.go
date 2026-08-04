package wsshards

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/oxynote/wetsocks/wsclient"
	"github.com/oxynote/wetsocks/wsclient/wsreconn"
	wsreconnMock "github.com/oxynote/wetsocks/wsclient/wsreconn/_mock"
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
	store := &SubKeeperStoreMock{
		SubKeepersFunc: func() []wsreconn.SubKeeper {
			return []wsreconn.SubKeeper{
				&wsreconnMock.SubKeeper{},
			}
		},
	}

	s, err := NewClient(
		context.Background(),
		slog.New(slog.DiscardHandler),
		func(_ int) wsreconn.ClientMetrics {
			return &wsreconnMock.ClientMetrics{}
		},
		wsreconn.ClientOptions{
			Options: wsclient.Options{
				URL: hs.URL,
			},
			BaseContext: func() context.Context {
				baseCtxCalled = true
				return context.Background()
			},
		},
		store,
	)
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.True(t, baseCtxCalled)

	// success with defaults
	baseCtxCalled = false
	respErr = false

	s, err = NewClient(
		context.Background(),
		slog.New(slog.DiscardHandler),
		func(_ int) wsreconn.ClientMetrics {
			return &wsreconnMock.ClientMetrics{}
		},
		wsreconn.ClientOptions{
			Options: wsclient.Options{
				URL: hs.URL,
			},
		},
		store,
	)
	assert.NoError(t, err)
	require.NotNil(t, s)
	assert.Len(t, s.ws, 1)
	assert.NotPanics(t, func() {
		s.Close()
	})
}

func Test_Client_Close(t *testing.T) {
	c1 := &clientMock{}
	c2 := &clientMock{}

	s := Client{
		ws: []client{
			c1,
			c2,
		},
	}
	s.Close()
	assert.Len(t, c1.CloseCalls(), 1)
	assert.Len(t, c2.CloseCalls(), 1)
}

func Test_Client_OnRead(t *testing.T) {
	c1 := &clientMock{}
	c2 := &clientMock{}

	s := Client{
		ws: []client{
			c1,
			c2,
		},
	}
	s.OnRead(nil)
	assert.Len(t, c1.OnReadCalls(), 1)
	assert.Len(t, c2.OnReadCalls(), 1)
}
