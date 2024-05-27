package wsserver

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jellydator/wetsocks/wsutil"
	"github.com/jellydator/xync"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_connCtxKey   ctxKey = "conn-ctx"
	_connCtxValue        = "123"
	_connCtx             = context.WithValue(context.Background(), _connCtxKey, _connCtxValue)
)

func Test_BinderFunc_Bind(t *testing.T) {
	var called bool

	bf := BinderFunc(func(_ Topic) { called = true })

	bf.Bind(nil)
	assert.True(t, called)
}

func Test_NewRouter(t *testing.T) {
	r := NewRouter()
	require.NotNil(t, r)
	assert.NotNil(t, r.topics)
}

func Test_Router_BindFunc(t *testing.T) {
	r := Router{
		topics: map[string]*topic{
			"test1@engine.configs": {},
			"test2@engine.configs": {},
		},
	}
	middlewares := []func(context.Context, string) bool{
		func(context.Context, string) bool { return true },
		func(context.Context, string) bool { return true },
	}

	// invalid topic format
	assert.PanicsWithValue(t, "wsserver: invalid topic format", func() {
		r.BindFunc("invalidtopic", func(_ Topic) {}, middlewares...)
	})

	// invalid binder
	assert.PanicsWithValue(t, "wsserver: invalid binder", func() {
		r.Bind("update@engine.configs", nil, middlewares...)
	})

	// duplicate topic
	assert.PanicsWithValue(t, "wsserver: multiple registrations for test1@engine.configs", func() {
		r.BindFunc("test1@engine.configs", func(_ Topic) {}, middlewares...)
	})

	// success
	var res Topic

	assert.NotPanics(t, func() {
		r.BindFunc("update@engine.configs", func(tpc Topic) {
			res = tpc
		}, middlewares...)
	})
	require.Contains(t, r.topics, "update@engine.configs")
	assert.Same(t, r.topics["update@engine.configs"], res)
	assert.Equal(t, middlewares, r.topics["update@engine.configs"].middlewares)
}

func Test_Router_Close(_ *testing.T) {
	r := Router{
		topics: map[string]*topic{
			"test1@engine.configs": {
				supv: xync.NewSupervisor(),
			},
			"test2@engine.configs": {
				supv: xync.NewSupervisor(),
			},
		},
	}

	r.Close()
}

func Test_Router_findTopic(t *testing.T) {
	r := Router{
		topics: map[string]*topic{
			"activation@engine.configs.active": {
				pattern: pattern{
					topic: wsutil.Topic{
						Operation: "activation",
						Path: []string{
							"engine",
							"configs",
							"active",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
						{
							value: "active",
						},
					},
				},
			},
			"update@engine.configs": {
				pattern: pattern{
					topic: wsutil.Topic{
						Operation: "update",
						Path: []string{
							"engine",
							"configs",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
					},
				},
			},
			"update@exchange.live.{freq}.tickers.{symbol}": {
				pattern: pattern{
					hasPlaceholders: true,
					topic: wsutil.Topic{
						Operation: "update",
						Path: []string{
							"exchange",
							"live",
							"{freq}",
							"tickers",
							"{symbol}",
						},
					},
					path: []pathElem{
						{
							value: "exchange",
						},
						{
							value: "live",
						},
						{
							value:       "freq",
							placeholder: true,
						},
						{
							value: "tickers",
						},
						{
							value:       "symbol",
							placeholder: true,
						},
					},
				},
			},
		},
	}

	inpTpc := wsutil.Topic{
		Operation: "update",
		Path:      []string{"exchange", "live", "candles", "BTC_USD", "5mins"},
	}

	// not found
	res, params := r.findTopic(inpTpc)
	assert.Nil(t, res)
	assert.Nil(t, params)

	// success
	exp := &topic{
		pattern: pattern{
			hasPlaceholders: true,
			topic: wsutil.Topic{
				Operation: "update",
				Path: []string{
					"exchange",
					"live",
					"candles",
					"{symbol}",
					"{interval}",
				},
			},
			path: []pathElem{
				{
					value: "exchange",
				},
				{
					value: "live",
				},
				{
					value: "candles",
				},
				{
					value:       "symbol",
					placeholder: true,
				},
				{
					value:       "interval",
					placeholder: true,
				},
			},
		},
	}
	r.topics["update@exchange.live.candles.{symbol}.{interval}"] = exp

	res, params = r.findTopic(inpTpc)
	assert.Same(t, exp, res)
	assert.Equal(t, map[string]string{
		"symbol":   "BTC_USD",
		"interval": "5mins",
	}, params)
}

func Test_Router_addTopicSub(t *testing.T) {
	c1 := &conn{ctx: _connCtx}
	c2 := &conn{ctx: _connCtx}
	r := Router{
		topics: map[string]*topic{
			"update@engine.configs.{param1}.{param2}": {
				pattern: pattern{
					hasPlaceholders: true,
					topic: wsutil.Topic{
						Operation: "update",
						Path: []string{
							"engine",
							"configs",
							"{param1}",
							"{param2}",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
						{
							value:       "param1",
							placeholder: true,
						},
						{
							value:       "param2",
							placeholder: true,
						},
					},
				},
				subs: map[*conn][]map[string]string{
					c1: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
			"activation@engine.configs.{param1}.{param2}": {
				pattern: pattern{
					hasPlaceholders: true,
					topic: wsutil.Topic{
						Operation: "activation",
						Path: []string{
							"engine",
							"configs",
							"{param1}",
							"{param2}",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
						{
							value:       "param1",
							placeholder: true,
						},
						{
							value:       "param2",
							placeholder: true,
						},
					},
				},
				subs: map[*conn][]map[string]string{
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
		},
	}

	inpTpc := wsutil.Topic{
		Operation: "write",
		Path: []string{
			"engine",
			"configs",
			"notfound1",
			"notfound2",
		},
	}

	// not found
	err := r.addTopicSub(inpTpc, c1)
	assert.Error(t, err)

	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs, 2)
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1], 2)
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c2], 2)
	require.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs, 1)
	assert.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs[c2], 2)

	// success
	inpTpc.Operation = "update"
	inpTpc.Path = []string{
		"engine",
		"configs",
		"new1",
		"new2",
	}
	err = r.addTopicSub(inpTpc, c1)
	assert.NoError(t, err)

	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs, 2)
	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1], 3)
	assert.Equal(t, map[string]string{
		"param1": "new1",
		"param2": "new2",
	}, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1][2])
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c2], 2)
	require.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs, 1)
	assert.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs[c2], 2)
}

func Test_Router_removeTopicSub(t *testing.T) {
	c1 := &conn{ctx: _connCtx}
	c2 := &conn{ctx: _connCtx}
	r := Router{
		topics: map[string]*topic{
			"update@engine.configs.{param1}.{param2}": {
				pattern: pattern{
					hasPlaceholders: true,
					topic: wsutil.Topic{
						Operation: "update",
						Path: []string{
							"engine",
							"configs",
							"{param1}",
							"{param2}",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
						{
							value:       "param1",
							placeholder: true,
						},
						{
							value:       "param2",
							placeholder: true,
						},
					},
				},
				subs: map[*conn][]map[string]string{
					c1: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
			"activation@engine.configs.{param1}.{param2}": {
				pattern: pattern{
					hasPlaceholders: true,
					topic: wsutil.Topic{
						Operation: "activation",
						Path: []string{
							"engine",
							"configs",
							"{param1}",
							"{param2}",
						},
					},
					path: []pathElem{
						{
							value: "engine",
						},
						{
							value: "configs",
						},
						{
							value:       "param1",
							placeholder: true,
						},
						{
							value:       "param2",
							placeholder: true,
						},
					},
				},
				subs: map[*conn][]map[string]string{
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
		},
	}

	inpTpc := wsutil.Topic{
		Operation: "write",
		Path: []string{
			"engine",
			"configs",
			"notfound1",
			"notfound2",
		},
	}

	// not found
	r.removeTopicSub(inpTpc, c1)

	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs, 2)
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1], 2)
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c2], 2)
	require.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs, 1)
	assert.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs[c2], 2)

	// success
	inpTpc.Operation = "update"
	inpTpc.Path = []string{
		"engine",
		"configs",
		"p1",
		"p2",
	}
	r.removeTopicSub(inpTpc, c1)

	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs, 2)
	require.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1], 1)
	assert.Equal(t, map[string]string{
		"param1": "p11",
		"param2": "p22",
	}, r.topics["update@engine.configs.{param1}.{param2}"].subs[c1][0])
	assert.Len(t, r.topics["update@engine.configs.{param1}.{param2}"].subs[c2], 2)
	require.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs, 1)
	assert.Len(t, r.topics["activation@engine.configs.{param1}.{param2}"].subs[c2], 2)
}

func Test_Router_removeConn(t *testing.T) {
	c1 := &conn{ctx: _connCtx}
	c2 := &conn{ctx: _connCtx}
	r := Router{
		topics: map[string]*topic{
			"update@engine.configs": {
				subs: map[*conn][]map[string]string{
					c1: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
			"activation@engine.configs": {
				subs: map[*conn][]map[string]string{
					c2: {
						{
							"param1": "p1",
							"param2": "p2",
						},
						{
							"param1": "p11",
							"param2": "p22",
						},
					},
				},
			},
		},
	}

	r.removeConn(c2)

	assert.Len(t, r.topics["update@engine.configs"].subs, 1)
	assert.Empty(t, r.topics["activation@engine.configs"].subs)
}

func Test_newTopic(t *testing.T) {
	tpc := wsutil.Topic{
		Operation: "update",
		Path:      []string{"test"},
	}
	v := newTopic(tpc, func(_ context.Context, _ string) bool { return false })
	require.NotNil(t, v)
	assert.Equal(t, tpc, v.pattern.topic)
	assert.NotNil(t, v.supv)
	assert.NotNil(t, v.subs)
}

func Test_topic_addSub(t *testing.T) {
	var (
		subWg, firstSubWg sync.WaitGroup

		subMu    sync.Mutex
		subCalls int

		firstSubMu    sync.Mutex
		firstSubCalls int
	)

	c1 := &conn{ctx: _connCtx}
	tpc := topic{
		supv: xync.NewSupervisor(),
		subs: make(map[*conn][]map[string]string),
	}
	tpc.events.sub.fns = []func(context.Context){
		func(ctx context.Context) {
			subMu.Lock()
			defer subMu.Unlock()

			assert.Equal(t, _connCtxValue, ctx.Value(_connCtxKey))

			p1 := TopicParamFromContext(ctx, "param1")
			p2 := TopicParamFromContext(ctx, "param2")

			assert.True(t, p1 == "p1" || p1 == "p111")
			assert.True(t, p2 == "p2" || p2 == "p222")

			subCalls++
			subWg.Done()
		},
	}
	tpc.events.firstSub.fns = []func(context.Context){
		func(_ context.Context) {
			firstSubMu.Lock()
			defer firstSubMu.Unlock()

			firstSubCalls++
			firstSubWg.Done()
		},
	}

	firstSubWg.Add(1)
	subWg.Add(1)
	tpc.addSub(c1, map[string]string{
		"param1": "p111",
		"param2": "p222",
	})
	subWg.Wait()
	firstSubWg.Wait()
	require.Len(t, tpc.subs, 1)
	require.Len(t, tpc.subs[c1], 1)
	assert.Equal(t, map[string]string{
		"param1": "p111",
		"param2": "p222",
	}, tpc.subs[c1][0])
	assert.Equal(t, 1, subCalls)
	assert.Equal(t, 1, firstSubCalls)

	subCalls = 0
	firstSubCalls = 0

	tpc.addSub(c1, map[string]string{
		"param1": "p111",
		"param2": "p222",
	})
	require.Len(t, tpc.subs, 1)
	require.Len(t, tpc.subs[c1], 1)
	assert.Zero(t, subCalls)
	assert.Zero(t, firstSubCalls)

	subCalls = 0
	firstSubCalls = 0
	tpc.pattern.hasPlaceholders = true

	tpc.addSub(c1, map[string]string{
		"param1": "p111",
		"param2": "p222",
	})
	require.Len(t, tpc.subs, 1)
	require.Len(t, tpc.subs[c1], 1)
	assert.Zero(t, subCalls)
	assert.Zero(t, firstSubCalls)

	subCalls = 0
	firstSubCalls = 0

	subWg.Add(1)
	tpc.addSub(c1, map[string]string{
		"param1": "p1",
		"param2": "p2",
	})
	subWg.Wait()
	require.Len(t, tpc.subs, 1)
	require.Len(t, tpc.subs[c1], 2)
	assert.Equal(t, map[string]string{
		"param1": "p1",
		"param2": "p2",
	}, tpc.subs[c1][1])
	assert.Equal(t, 1, subCalls)
	assert.Zero(t, firstSubCalls)
}

func Test_topic_removeSub(t *testing.T) {
	var (
		unsubWg, lastUnsubWg sync.WaitGroup

		unsubMu    sync.Mutex
		unsubCalls int

		lastUnsubMu    sync.Mutex
		lastUnsubCalls int
	)

	c1 := &conn{ctx: _connCtx}
	c2 := &conn{ctx: _connCtx}
	tpc := topic{
		pattern: pattern{
			hasPlaceholders: true,
		},
		supv: xync.NewSupervisor(),
		subs: map[*conn][]map[string]string{
			c1: {
				{
					"param1": "p1",
					"param2": "p2",
				},
				{
					"param1": "p11",
					"param2": "p22",
				},
			},
			c2: {
				{
					"param1": "p1",
					"param2": "p2",
				},
				{
					"param1": "p11",
					"param2": "p22",
				},
			},
		},
	}
	tpc.events.unsub.fns = []func(context.Context){
		func(ctx context.Context) {
			unsubMu.Lock()
			defer unsubMu.Unlock()

			assert.Equal(t, _connCtxValue, ctx.Value(_connCtxKey))

			p1 := TopicParamFromContext(ctx, "param1")
			p2 := TopicParamFromContext(ctx, "param2")

			assert.True(t, p1 == "p1" || p1 == "p11")
			assert.True(t, p2 == "p2" || p2 == "p22")

			unsubCalls++
			unsubWg.Done()
		},
	}
	tpc.events.lastUnsub.fns = []func(context.Context){
		func(_ context.Context) {
			lastUnsubMu.Lock()
			defer lastUnsubMu.Unlock()

			lastUnsubCalls++
			lastUnsubWg.Done()
		},
	}

	tpc.removeSub(&conn{}, map[string]string{})
	require.Len(t, tpc.subs, 2)
	assert.Len(t, tpc.subs[c1], 2)
	assert.Len(t, tpc.subs[c2], 2)

	unsubWg.Add(1)
	tpc.removeSub(c1, map[string]string{
		"param1": "p11",
		"param2": "p22",
	})
	unsubWg.Wait()
	require.Len(t, tpc.subs, 2)
	assert.Len(t, tpc.subs[c1], 1)
	assert.Len(t, tpc.subs[c2], 2)
	assert.Equal(t, 1, unsubCalls)
	assert.Zero(t, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0

	unsubWg.Add(1)
	tpc.removeSub(c1, map[string]string{
		"param1": "p1",
		"param2": "p2",
	})
	unsubWg.Wait()
	require.Len(t, tpc.subs, 1)
	assert.Len(t, tpc.subs[c2], 2)
	assert.Equal(t, 1, unsubCalls)
	assert.Zero(t, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0

	unsubWg.Add(1)
	tpc.removeSub(c2, map[string]string{
		"param1": "p11",
		"param2": "p22",
	})
	unsubWg.Wait()
	require.Len(t, tpc.subs, 1)
	assert.Len(t, tpc.subs[c2], 1)
	assert.Equal(t, 1, unsubCalls)
	assert.Zero(t, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0

	lastUnsubWg.Add(1)
	unsubWg.Add(1)
	tpc.removeSub(c2, map[string]string{
		"param1": "p1",
		"param2": "p2",
	})
	unsubWg.Wait()
	lastUnsubWg.Wait()
	require.Empty(t, tpc.subs)
	assert.Equal(t, 1, unsubCalls)
	assert.Equal(t, 1, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0
	tpc.pattern.hasPlaceholders = false
	tpc.subs[c1] = []map[string]string{{}}
	tpc.events.unsub.fns[0] = func(ctx context.Context) {
		unsubMu.Lock()
		defer unsubMu.Unlock()

		assert.Equal(t, _connCtxValue, ctx.Value(_connCtxKey))

		unsubCalls++

		unsubWg.Done()
	}

	lastUnsubWg.Add(1)
	unsubWg.Add(1)
	tpc.removeSub(c1, map[string]string{})
	unsubWg.Wait()
	lastUnsubWg.Wait()
	require.Empty(t, tpc.subs)
	assert.Equal(t, 1, unsubCalls)
	assert.Equal(t, 1, lastUnsubCalls)
}

func Test_topic_removeConn(t *testing.T) {
	var (
		unsubWg, lastUnsubWg sync.WaitGroup

		unsubMu    sync.Mutex
		unsubCalls int

		lastUnsubMu    sync.Mutex
		lastUnsubCalls int
	)

	c1 := &conn{ctx: _connCtx}
	c2 := &conn{ctx: _connCtx}
	tpc := topic{
		supv: xync.NewSupervisor(),
		subs: map[*conn][]map[string]string{
			c1: {
				{
					"param1": "p1",
					"param2": "p2",
				},
				{
					"param1": "p11",
					"param2": "p22",
				},
			},
			c2: {
				{
					"param1": "p1",
					"param2": "p2",
				},
				{
					"param1": "p11",
					"param2": "p22",
				},
			},
		},
	}
	tpc.events.unsub.fns = []func(context.Context){
		func(ctx context.Context) {
			unsubMu.Lock()
			defer unsubMu.Unlock()

			assert.Equal(t, _connCtxValue, ctx.Value(_connCtxKey))

			p1 := TopicParamFromContext(ctx, "param1")
			p2 := TopicParamFromContext(ctx, "param2")

			assert.True(t, p1 == "p1" || p1 == "p11")
			assert.True(t, p2 == "p2" || p2 == "p22")

			unsubCalls++
			unsubWg.Done()
		},
	}
	tpc.events.lastUnsub.fns = []func(context.Context){
		func(_ context.Context) {
			lastUnsubMu.Lock()
			defer lastUnsubMu.Unlock()

			lastUnsubCalls++
			lastUnsubWg.Done()
		},
	}

	tpc.removeConn(&conn{})
	assert.Len(t, tpc.subs, 2)

	unsubWg.Add(2)
	tpc.removeConn(c1)
	unsubWg.Wait()

	assert.Len(t, tpc.subs, 1)
	assert.Equal(t, 2, unsubCalls)
	assert.Zero(t, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0

	unsubWg.Add(2)
	lastUnsubWg.Add(1)
	tpc.removeConn(c2)
	unsubWg.Wait()
	lastUnsubWg.Wait()

	assert.Empty(t, tpc.subs)
	assert.Equal(t, 2, unsubCalls)
	assert.Equal(t, 1, lastUnsubCalls)
}

func Test_topic_publish(t *testing.T) {
	var tpc topic

	pubCtx := context.WithValue(context.Background(), contextKey(-100), "-100")
	writeCh := make(chan []byte)
	subs := map[*conn][]map[string]string{
		prepConn(writeCh): {
			{
				"symbol":   "BTC_USD",
				"interval": "3mins",
			},
			{
				"symbol":   "ETH_BTC",
				"interval": "10mins",
			},
		},
		prepConn(writeCh): {
			{
				"symbol":   "BTC_USD",
				"interval": "3mins",
			},
			{
				"symbol":   "LTC_BTC",
				"interval": "15mins",
			},
		},
		prepConn(writeCh): {
			{
				"symbol":   "LTC_USD",
				"interval": "30mins",
			},
		},
	}
	ptrn := pattern{
		topic: wsutil.Topic{
			Descriptor: "sub",
			Operation:  "update",
			Path: []string{
				"exchange",
				"live",
				"candles",
				"{symbol}",
				"{interval}",
			},
		},
		path: []pathElem{
			{
				value: "exchange",
			},
			{
				value: "live",
			},
			{
				value: "candles",
			},
			{
				value:       "symbol",
				placeholder: true,
			},
			{
				value:       "interval",
				placeholder: true,
			},
		},
		hasPlaceholders: true,
	}

	var (
		middlewareMu    sync.Mutex
		middlewareCalls int
	)

	middlewares := []func(context.Context, string) bool{
		func(mctx context.Context, _ string) bool {
			assert.Nil(t, mctx.Value(contextKey(-100)))

			middlewareMu.Lock()
			middlewareCalls++
			middlewareMu.Unlock()

			return true
		},
		func(mctx context.Context, _ string) bool {
			assert.Equal(t, _connCtxValue, mctx.Value(_connCtxKey))

			middlewareMu.Lock()
			middlewareCalls++
			middlewareMu.Unlock()

			return true
		},
		func(mctx context.Context, _ string) bool {
			params := mctx.Value(_topicParamsKey)
			assert.Contains(t, params, "symbol")
			assert.Contains(t, params, "interval")

			middlewareMu.Lock()
			middlewareCalls++
			middlewareMu.Unlock()

			return true
		},
		func(mctx context.Context, _ string) bool {
			sym := TopicParamFromContext(mctx, "symbol")

			middlewareMu.Lock()
			middlewareCalls++
			middlewareMu.Unlock()

			return sym == "BTC_USD"
		},
	}
	closeCh := make(chan struct{})

	// invalid payload
	go func() {
		assert.Never(t, func() bool {
			select {
			case <-writeCh:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	tpc.publish(pubCtx, ptrn, subs, map[string]any{
		"invalid": func() {},
	}, middlewares...)
	<-closeCh

	assert.Equal(t, 20, middlewareCalls)

	middlewareCalls = 0

	// success
	go func() {
		var count int

		assert.Eventually(t, func() bool {
			select {
			case data := <-writeCh:
				count++

				assert.JSONEq(t, `{
					"topic":"sub~update@exchange.live.candles.BTC_USD.3mins",
					"msg":"hello"
				}`, string(data))

				return count == 2
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	data := map[string]any{
		"msg": "hello",
	}

	tpc.publish(pubCtx, ptrn, subs, data, middlewares...)
	<-closeCh

	assert.Equal(t, 20, middlewareCalls)
}

func Test_topic_Publish(t *testing.T) {
	var (
		middlewareMu    sync.Mutex
		middlewareCalls int
	)

	pubCtx := context.WithValue(context.Background(), contextKey(-100), "-100")
	writeCh := make(chan []byte)
	tpc := topic{
		pattern: pattern{
			topic: wsutil.Topic{
				Operation: "update",
				Path: []string{
					"exchange",
					"live",
					"candles",
					"{symbol}",
					"{interval}",
				},
			},
			path: []pathElem{
				{
					value: "exchange",
				},
				{
					value: "live",
				},
				{
					value: "candles",
				},
				{
					value:       "symbol",
					placeholder: true,
				},
				{
					value:       "interval",
					placeholder: true,
				},
			},
			hasPlaceholders: true,
		},
		middlewares: []func(context.Context, string) bool{
			func(mctx context.Context, _ string) bool {
				assert.Nil(t, mctx.Value(contextKey(-100)))

				middlewareMu.Lock()
				middlewareCalls++
				middlewareMu.Unlock()

				return true
			},
			func(mctx context.Context, _ string) bool {
				assert.Equal(t, _connCtxValue, mctx.Value(_connCtxKey))

				middlewareMu.Lock()
				middlewareCalls++
				middlewareMu.Unlock()

				return true
			},
			func(mctx context.Context, _ string) bool {
				params := mctx.Value(_topicParamsKey)
				assert.Contains(t, params, "symbol")
				assert.Contains(t, params, "interval")

				middlewareMu.Lock()
				middlewareCalls++
				middlewareMu.Unlock()

				return true
			},
			func(mctx context.Context, _ string) bool {
				sym := TopicParamFromContext(mctx, "symbol")

				middlewareMu.Lock()
				middlewareCalls++
				middlewareMu.Unlock()

				return sym == "BTC_USD"
			},
		},
		subs: map[*conn][]map[string]string{
			prepConn(writeCh): {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "ETH_BTC",
					"interval": "10mins",
				},
			},
			prepConn(writeCh): {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "LTC_BTC",
					"interval": "15mins",
				},
			},
			prepConn(writeCh): {
				{
					"symbol":   "LTC_USD",
					"interval": "30mins",
				},
			},
		},
	}
	closeCh := make(chan struct{})

	go func() {
		var count int

		assert.Eventually(t, func() bool {
			select {
			case data := <-writeCh:
				count++

				assert.JSONEq(t, `{
					"topic":"pub~update@exchange.live.candles.BTC_USD.3mins",
					"payload":{"msg":"hello"}
				}`, string(data))

				return count == 2
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	payload := struct {
		Msg string `json:"msg"`
	}{Msg: "hello"}

	tpc.Publish(pubCtx, payload, nil)
	<-closeCh

	assert.Equal(t, 20, middlewareCalls)

	middlewareCalls = 0

	go func() {
		assert.Never(t, func() bool {
			select {
			case <-writeCh:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	payload = struct {
		Msg string `json:"msg"`
	}{Msg: "hello"}

	tpc.Publish(pubCtx, payload, func(mctx context.Context, _ string) bool {
		intv := TopicParamFromContext(mctx, "interval")

		middlewareMu.Lock()
		middlewareCalls++
		middlewareMu.Unlock()

		return intv != "3mins"
	})
	<-closeCh

	assert.Equal(t, 22, middlewareCalls)
}

func Test_topic_DropMany(t *testing.T) {
	dropCtx := context.WithValue(context.Background(), contextKey(-100), "-100")
	writeCh := make(chan []byte)
	c1 := prepConn(writeCh)
	c2 := prepConn(writeCh)
	c3 := prepConn(writeCh)
	tpc := topic{
		pattern: pattern{
			topic: wsutil.Topic{
				Operation: "update",
				Path: []string{
					"exchange",
					"live",
					"candles",
					"{symbol}",
					"{interval}",
				},
			},
			path: []pathElem{
				{
					value: "exchange",
				},
				{
					value: "live",
				},
				{
					value: "candles",
				},
				{
					value:       "symbol",
					placeholder: true,
				},
				{
					value:       "interval",
					placeholder: true,
				},
			},
			hasPlaceholders: true,
		},
		supv: xync.NewSupervisor(),
		subs: map[*conn][]map[string]string{
			c1: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "LTC_USD",
					"interval": "10mins",
				},
			},
			c2: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "LTC_USD",
					"interval": "15mins",
				},
			},
			c3: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
			},
		},
	}
	closeCh := make(chan struct{})

	var (
		filterMu    sync.Mutex
		filterCalls int

		unsubWg, lastUnsubWg sync.WaitGroup

		unsubMu    sync.Mutex
		unsubCalls int

		lastUnsubMu    sync.Mutex
		lastUnsubCalls int
	)

	tpc.events.unsub.fns = []func(context.Context){
		func(_ context.Context) {
			unsubMu.Lock()
			defer unsubMu.Unlock()

			unsubCalls++
			unsubWg.Done()
		},
		func(_ context.Context) {
			unsubMu.Lock()
			defer unsubMu.Unlock()

			unsubCalls++
			unsubWg.Done()
		},
	}
	tpc.events.lastUnsub.fns = []func(context.Context){
		func(_ context.Context) {
			lastUnsubMu.Lock()
			defer lastUnsubMu.Unlock()

			lastUnsubCalls++
			lastUnsubWg.Done()
		},
		func(_ context.Context) {
			lastUnsubMu.Lock()
			defer lastUnsubMu.Unlock()

			lastUnsubCalls++
			lastUnsubWg.Done()
		},
	}

	unsubWg.Add(6)

	go func() {
		var count int

		assert.Eventually(t, func() bool {
			select {
			case data := <-writeCh:
				count++

				assert.JSONEq(t, `{
					"topic":"drop~update@exchange.live.candles.BTC_USD.3mins",
					"reason":"error"
				}`, string(data))

				return count == 3
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	tpc.DropMany(dropCtx, "error", func(mctx context.Context, _ string) bool {
		assert.Nil(t, mctx.Value(contextKey(-100)))
		assert.Equal(t, _connCtxValue, mctx.Value(_connCtxKey))

		params := mctx.Value(_topicParamsKey)
		assert.Contains(t, params, "symbol")
		assert.Contains(t, params, "interval")

		intv := TopicParamFromContext(mctx, "interval")

		filterMu.Lock()
		filterCalls++
		filterMu.Unlock()

		return intv == "3mins"
	})
	<-closeCh

	unsubWg.Wait()

	assert.Equal(t, 5, filterCalls)
	assert.Equal(t, 6, unsubCalls)
	assert.Zero(t, lastUnsubCalls)
	require.NotNil(t, tpc.subs[c1])
	assert.Len(t, tpc.subs[c1], 1)
	require.NotNil(t, tpc.subs[c2])
	assert.Len(t, tpc.subs[c2], 1)
	assert.Nil(t, tpc.subs[c3])

	unsubCalls = 0
	lastUnsubCalls = 0

	unsubWg.Add(4)
	lastUnsubWg.Add(2)

	go func() {
		var count int

		assert.Eventually(t, func() bool {
			select {
			case data := <-writeCh:
				count++

				assert.NotContains(t, string(data), `"reason"`)

				ok1 := strings.Contains(string(data), `"topic":"drop~update@exchange.live.candles.LTC_USD.10mins"`)
				ok2 := strings.Contains(string(data), `"topic":"drop~update@exchange.live.candles.LTC_USD.15mins"`)

				assert.True(t, ok1 || ok2)

				return count == 2
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	tpc.DropMany(dropCtx, "", nil)
	<-closeCh

	unsubWg.Wait()
	lastUnsubWg.Wait()

	require.Empty(t, tpc.subs)
	assert.Equal(t, 4, unsubCalls)
	assert.Equal(t, 2, lastUnsubCalls)

	unsubCalls = 0
	lastUnsubCalls = 0

	go func() {
		assert.Never(t, func() bool {
			select {
			case <-writeCh:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	tpc.DropMany(dropCtx, "", nil)
	<-closeCh
	require.Empty(t, tpc.subs)
	assert.Zero(t, unsubCalls)
	assert.Zero(t, lastUnsubCalls)
}

func Test_topic_DropOne(t *testing.T) {
	writeCh := make(chan []byte, 3)
	c1 := prepConn(writeCh)
	c2 := prepConn(writeCh)
	c3 := prepConn(writeCh)
	tpc := topic{
		pattern: pattern{
			topic: wsutil.Topic{
				Operation: "update",
				Path: []string{
					"exchange",
					"live",
					"candles",
					"{symbol}",
					"{interval}",
				},
			},
			path: []pathElem{
				{
					value: "exchange",
				},
				{
					value: "live",
				},
				{
					value: "candles",
				},
				{
					value:       "symbol",
					placeholder: true,
				},
				{
					value:       "interval",
					placeholder: true,
				},
			},
			hasPlaceholders: true,
		},
		supv: xync.NewSupervisor(),
		subs: map[*conn][]map[string]string{
			c1: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "LTC_USD",
					"interval": "10mins",
				},
			},
			c2: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
				{
					"symbol":   "LTC_USD",
					"interval": "15mins",
				},
			},
			c3: {
				{
					"symbol":   "BTC_USD",
					"interval": "3mins",
				},
			},
		},
	}
	closeCh := make(chan struct{})

	go func() {
		assert.Eventually(t, func() bool {
			select {
			case data := <-writeCh:
				assert.JSONEq(t, `{
					"topic":"drop~update@exchange.live.candles.BTC_USD.3mins",
					"reason":"error"
				}`, string(data))

				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond*250)

		closeCh <- struct{}{}
	}()

	tpc.DropOne(c3.ctx, "error")
	<-closeCh

	require.NotNil(t, tpc.subs[c1])
	assert.Len(t, tpc.subs[c1], 2)
	require.NotNil(t, tpc.subs[c2])
	assert.Len(t, tpc.subs[c2], 2)
	require.Nil(t, tpc.subs[c3])

	assert.NotPanics(t, func() {
		tpc.DropOne(context.Background(), "123")
	})
}

func Test_topic_OnSub(t *testing.T) {
	var tpc topic

	tpc.OnSub(func(_ context.Context) {})
	assert.Len(t, tpc.events.sub.fns, 1)
}

func Test_topic_execSubFns(t *testing.T) {
	var called1, called2 bool

	tpc := topic{
		supv: xync.NewSupervisor(),
	}

	tpc.events.sub.fns = []func(context.Context){
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called1 = true
		},
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called2 = true
		},
	}

	tpc.execSubFns(context.Background())
	tpc.supv.Wait()
	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_topic_OnUnsub(t *testing.T) {
	var tpc topic

	tpc.OnUnsub(func(_ context.Context) {})
	assert.Len(t, tpc.events.unsub.fns, 1)
}

func Test_topic_execUnsubFns(t *testing.T) {
	var called1, called2 bool

	tpc := topic{
		supv: xync.NewSupervisor(),
	}

	tpc.events.unsub.fns = []func(context.Context){
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called1 = true
		},
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called2 = true
		},
	}

	tpc.execUnsubFns(context.Background())
	tpc.supv.Wait()
	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_topic_OnFirstSub(t *testing.T) {
	var tpc topic

	tpc.OnFirstSub(func(_ context.Context) {})
	assert.Len(t, tpc.events.firstSub.fns, 1)
}

func Test_topic_execFirstSubFns(t *testing.T) {
	var called1, called2 bool

	tpc := topic{
		supv: xync.NewSupervisor(),
	}

	tpc.events.firstSub.fns = []func(context.Context){
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called1 = true
		},
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called2 = true
		},
	}

	tpc.execFirstSubFns()
	tpc.supv.Wait()
	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_topic_OnLastUnsub(t *testing.T) {
	var tpc topic

	tpc.OnLastUnsub(func(_ context.Context) {})
	assert.Len(t, tpc.events.lastUnsub.fns, 1)
}

func Test_topic_execLastUnsubFns(t *testing.T) {
	var called1, called2 bool

	tpc := topic{
		supv: xync.NewSupervisor(),
	}

	tpc.events.lastUnsub.fns = []func(context.Context){
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called1 = true
		},
		func(ctx context.Context) {
			assert.NotNil(t, ctx)
			called2 = true
		},
	}

	tpc.execLastUnsubFns()
	tpc.supv.Wait()
	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_newPattern(t *testing.T) {
	tpc := wsutil.Topic{
		Descriptor: "sub",
		Operation:  "update",
		Path: []string{
			"exchange",
			"live",
			"candles",
			"{symbol}",
			"{interval}",
		},
	}
	p := newPattern(tpc)
	assert.Equal(t, tpc, p.topic)
	assert.Equal(t, []pathElem{
		{
			value: "exchange",
		},
		{
			value: "live",
		},
		{
			value: "candles",
		},
		{
			value:       "symbol",
			placeholder: true,
		},
		{
			value:       "interval",
			placeholder: true,
		},
	}, p.path)
	assert.True(t, p.hasPlaceholders)
}

func Test_pattern_format(t *testing.T) {
	p := pattern{
		topic: wsutil.Topic{
			Descriptor: "sub",
			Operation:  "update",
			Path: []string{
				"exchange",
				"live",
				"candles",
				"{symbol}",
				"{interval}",
			},
		},
		path: []pathElem{
			{
				value: "exchange",
			},
			{
				value: "live",
			},
			{
				value: "candles",
			},
			{
				value:       "symbol",
				placeholder: true,
			},
			{
				value:       "interval",
				placeholder: true,
			},
		},
		hasPlaceholders: true,
	}
	params := map[string]string{
		"symbol":   "BTC_USD",
		"interval": "5mins",
	}

	assert.Equal(t, "sub~update@exchange.live.candles.BTC_USD.5mins", p.format(params))
}

func Test_NewTopicParamsContext(t *testing.T) {
	bgCtx := context.WithValue(context.Background(), contextKey(2), "test")
	ctx := NewTopicParamsContext(bgCtx, map[string]string{})
	require.NotNil(t, ctx)
	assert.Same(t, bgCtx, ctx)

	params := map[string]string{"test1": "t1"}
	ctx = NewTopicParamsContext(bgCtx, params)
	require.NotNil(t, ctx)
	assert.NotSame(t, bgCtx, ctx)

	params1, ok := ctx.Value(_topicParamsKey).(map[string]string)
	assert.True(t, ok)
	assert.Equal(t, params, params1)
}

func Test_TopicParamFromContext(t *testing.T) {
	assert.Zero(t, TopicParamFromContext(context.Background(), "test"))

	ctx := context.WithValue(context.Background(), _topicParamsKey, true)
	assert.Zero(t, TopicParamFromContext(ctx, "test"))

	ctx = context.WithValue(context.Background(), _topicParamsKey, map[string]string{
		"test1": "123",
	})
	assert.Zero(t, TopicParamFromContext(ctx, "test"))

	ctx = context.WithValue(context.Background(), _topicParamsKey, map[string]string{
		"test": "123",
	})
	assert.Equal(t, "123", TopicParamFromContext(ctx, "test"))
}

func Test_areTopicParamsEqual(t *testing.T) {
	assert.False(t, areTopicParamsEqual(
		map[string]string{"test1": "t1", "test2": "t2"},
		map[string]string{"test1": "t1", "test3": "t3"},
	))
	assert.False(t, areTopicParamsEqual(
		map[string]string{"test1": "t1", "test2": "t2"},
		map[string]string{"test1": "t1", "test2": "t3"},
	))
	assert.True(t, areTopicParamsEqual(
		map[string]string{"test1": "t1", "test2": "t2"},
		map[string]string{"test1": "t1", "test2": "t2"},
	))
}

func prepConn(ch chan []byte) *conn {
	return &conn{
		log:     zerolog.Nop(),
		ctx:     context.WithValue(_connCtx, _wsCtxID, xid.New().String()),
		writeCh: ch,
	}
}
