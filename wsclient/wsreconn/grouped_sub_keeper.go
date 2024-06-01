package wsreconn

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jellydator/xync"
	"golang.org/x/exp/maps"
)

// GroupedSubKeeper holds subscriptions whose payloads are sent in groups.
// It also tracks how many subscriptions are made with a specific
// key and requires that the same number of unsubscriptions should
// be made as well.
type GroupedSubKeeper[K comparable] struct {
	metrics       SubKeeperMetrics
	supv          *xync.Supervisor
	fmt           GroupedSubFormatter[K]
	cooldown      time.Duration
	manualConfirm bool

	subsMu sync.RWMutex
	subs   map[string]map[K]groupedSub

	fnsMu sync.RWMutex
	fns   []func(context.Context)
}

// NewGroupedSubKeeper creates a fresh instance of GroupedSubKeeper.
// The duration parameter determines how long to wait between payload events.
// The bool parameter determines if a manual confirmation should be
// expected or not.
func NewGroupedSubKeeper[K comparable](metrics SubKeeperMetrics, fmt GroupedSubFormatter[K], cooldown time.Duration, manualConfirm bool) *GroupedSubKeeper[K] {
	return &GroupedSubKeeper[K]{
		metrics:       metrics,
		supv:          xync.NewSupervisor(),
		fmt:           fmt,
		cooldown:      cooldown,
		manualConfirm: manualConfirm,
		subs:          make(map[string]map[K]groupedSub),
	}
}

// Close stops all background processes.
func (g *GroupedSubKeeper[K]) Close() {
	g.supv.CloseAndWait()
}

// Payloads returns a slice of payloads that should be sent through a
// WebSocket connection and a duration value indicating how much time has to
// pass after each write before a new one is sent.
func (g *GroupedSubKeeper[K]) Payloads() (res []SubPayloader, d time.Duration) {
	g.subsMu.RLock()
	defer g.subsMu.RUnlock()

	for topic, vv := range g.subs {
		// we need to gather all the keys that need to
		// be subscribed to/unsubscribed from and sent together
		var subKeys, unsubKeys []K

		for key, v := range vv {
			if v.confirmed {
				continue
			}

			if v.count == 0 {
				unsubKeys = append(unsubKeys, key)
				continue
			}

			subKeys = append(subKeys, key)
		}

		if len(subKeys) != 0 {
			res = append(res, groupedPayload[K]{
				keeper:    g,
				topic:     topic,
				subbed:    true,
				keys:      subKeys,
				topicData: vv,
			})
		}

		if len(unsubKeys) != 0 {
			res = append(res, groupedPayload[K]{
				keeper:    g,
				topic:     topic,
				subbed:    false,
				keys:      unsubKeys,
				topicData: vv,
			})
		}
	}

	return res, g.cooldown
}

// ResetAll resets every subscription to an unconfirmed 'subscribed' state.
// It does not trigger any change functions.
func (g *GroupedSubKeeper[K]) ResetAll() {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	for topic, vv := range g.subs {
		for key, v := range vv {
			v.confirmed = false

			if v.count == 0 {
				delete(vv, key)
				g.metrics.DecWsSubs()

				continue
			}

			vv[key] = v
		}

		if len(vv) == 0 {
			delete(g.subs, topic)
		}
	}
}

// OnChange sets the provided function to be executed
// when subscription-related data changes.
func (g *GroupedSubKeeper[K]) OnChange(fn func(context.Context)) {
	g.fnsMu.Lock()
	g.fns = append(g.fns, fn)
	g.fnsMu.Unlock()
}

// execFns executes all change-subscribed functions.
func (g *GroupedSubKeeper[K]) execFns() {
	g.fnsMu.RLock()
	defer g.fnsMu.RUnlock()

	for _, fn := range g.fns {
		fn := fn

		g.supv.Go(func(gctx context.Context) {
			fn(gctx)
		})
	}
}

// TopicKeys returns all the confirmed keys of the provided topic.
// A nil value is returned if the topic does not exist or if it does not
// have any confirmed keys.
func (g *GroupedSubKeeper[K]) TopicKeys(topic string) []K {
	g.subsMu.RLock()
	defer g.subsMu.RUnlock()

	vv := g.subs[topic]
	if vv == nil {
		return nil
	}

	var res []K

	for key, v := range vv {
		if v.count > 0 && v.confirmed {
			res = append(res, key)
		}
	}

	return res
}

// Subscribe subscribes to the provided topic and key combinations.
// Each topic and key combination can be subscribed to more than once,
// but to fully unsubscribe from it, the same number of Unsubscribe calls
// is needed.
func (g *GroupedSubKeeper[K]) Subscribe(topic string, keys ...K) {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	vv := g.subs[topic]
	if vv == nil {
		vv = make(map[K]groupedSub, len(keys))
		g.subs[topic] = vv
	}

	var ok bool

	for i := range keys {
		v, ok1 := vv[keys[i]]
		if !ok1 || v.count == 0 && v.confirmed {
			if !ok {
				ok = true
			}

			v.confirmed = false
		}

		v.count++
		vv[keys[i]] = v

		g.metrics.IncWsSubs()
	}

	if ok {
		g.execFns()
	}
}

// Unsubscribe unsubscribes from the provided topic and key combinations.
func (g *GroupedSubKeeper[K]) Unsubscribe(topic string, keys ...K) {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	vv, ok := g.subs[topic]
	if !ok {
		return
	}

	if g.unsubscribe(topic, keys, vv, false) {
		g.execFns()
	}
}

// unsubscribe deletes/marks the provided subscriber as unsubscribed.
// The bool return value indicates if the subscriber has been updated.
// Should not be used in a concurrent environment.
func (g *GroupedSubKeeper[K]) unsubscribe(topic string, keys []K, vv map[K]groupedSub, decrMax bool) bool {
	var updated bool

	for i := range keys {
		v, ok := vv[keys[i]]
		if !ok {
			continue
		}

		if v.count == 1 && !v.confirmed || v.count == 0 && v.confirmed {
			// delete if no additional payloads need to be sent
			delete(vv, keys[i])
			g.metrics.DecWsSubs()

			continue
		} else if v.count == 0 {
			continue
		}

		if decrMax {
			v.count = 0
		} else {
			v.count--
		}

		if v.count == 0 {
			v.confirmed = false
			updated = true
		}

		vv[keys[i]] = v
	}

	if len(vv) == 0 {
		delete(g.subs, topic)
		return false
	}

	return updated
}

// UnsubscribeAll unsubscribes from all topic and key combinations.
func (g *GroupedSubKeeper[K]) UnsubscribeAll() {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	var ok bool

	for topic, vv := range g.subs {
		ok1 := g.unsubscribe(topic, maps.Keys(vv), vv, true)
		if !ok {
			ok = ok1
		}
	}

	if ok {
		g.execFns()
	}
}

// ConfirmSub manually confirms a subcription.
func (g *GroupedSubKeeper[K]) ConfirmSub(topic string, keys ...K) {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	vv := g.subs[topic]
	if vv == nil {
		return
	}

	g.confirm(true, topic, vv, keys...)
}

// ConfirmUnsub manually confirms an unsubcription.
// If the invert parameter is set to true, all the other keys except
// those that are provided are confirmed.
func (g *GroupedSubKeeper[K]) ConfirmUnsub(topic string, invert bool, keys ...K) {
	g.subsMu.Lock()
	defer g.subsMu.Unlock()

	vv := g.subs[topic]
	if vv == nil {
		return
	}

	if invert {
		vv1 := maps.Clone(vv)
		for i := range keys {
			delete(vv1, keys[i])
		}

		keys = maps.Keys(vv1)
	}

	g.confirm(false, topic, vv, keys...)
}

// confirm manually confirms topic and key subscriptions.
// Not concurrently safe.
func (g *GroupedSubKeeper[K]) confirm(subbed bool, topic string, vv map[K]groupedSub, keys ...K) {
	for i := range keys {
		v, ok := vv[keys[i]]
		if !ok {
			continue
		}

		if v.confirmed || v.count > 0 && !subbed || v.count == 0 && subbed {
			continue
		}

		v.confirmed = true

		if subbed {
			g.metrics.IncWsSubConfirmations()
		} else {
			g.metrics.IncWsUnsubConfirmations()
		}

		if v.count == 0 {
			delete(vv, keys[i])
			g.metrics.DecWsSubs()

			continue
		}

		vv[keys[i]] = v
	}

	if len(vv) == 0 {
		delete(g.subs, topic)
	}
}

// groupedSub contains a single key's subscription data.
type groupedSub struct {
	confirmed bool

	// count indicates how many times a single key was subscribed
	// to.
	// It should be checked if confirmed is false.
	// If count is 0, unsubcription should be performed.
	// If count is >0, subscription should be performed.
	count uint
}

// groupedPayload contains a single subscription payload.
type groupedPayload[K comparable] struct {
	keeper    *GroupedSubKeeper[K]
	topic     string
	subbed    bool
	keys      []K
	topicData map[K]groupedSub
}

// Payload returns a payload that should be sent through
// a WebSocket connection and a function that should be used
// when confirming a response.
// The function return value might be nil if no confirmation is needed.
func (g groupedPayload[K]) Payload() (any, func(json.RawMessage)) {
	var fn func(d json.RawMessage)

	if !g.keeper.manualConfirm {
		fn = func(d json.RawMessage) {
			g.keeper.subsMu.Lock()
			defer g.keeper.subsMu.Unlock()

			if g.subbed {
				if g.keeper.fmt.ConfirmSub(g.topic, g.keys, d) {
					g.keeper.confirm(true, g.topic, g.topicData, g.keys...)
				}

				return
			}

			if g.keeper.fmt.ConfirmUnsub(g.topic, g.keys, d) {
				g.keeper.confirm(false, g.topic, g.topicData, g.keys...)
			}
		}
	}

	if g.subbed {
		return g.keeper.fmt.SubMessage(g.topic, g.keys), fn
	}

	return g.keeper.fmt.UnsubMessage(g.topic, g.keys), fn
}

// GroupedSubFormatter is an interface that handles grouped subscribers'
// message formatting.
//
//go:generate ../../scripts/codegen/mock -t internal GroupedSubFormatter
type GroupedSubFormatter[K comparable] interface {
	// SubMessage should return data that should be sent as a
	// subscription message.
	// If the message cannot be produced (e.g., on error), it
	// should return nil, in which case the subscription will be
	// skipped and retried later.
	SubMessage(topic string, keys []K) any

	// UnsubMessage should return data that should be sent as an
	// unsubscription message.
	// If the message cannot be produced (e.g., on error), it
	// should return nil, in which case the unsubscription will be
	// skipped and retried later.
	UnsubMessage(topic string, keys []K) any

	// ConfirmSub should confirm the subscription if the provided data
	// contains the confirmation information.
	// 'true' should be returned if the subscription is confirmed.
	ConfirmSub(topic string, keys []K, data json.RawMessage) bool

	// ConfirmUnsub should confirm the unsubscription if the provided
	// data contains the confirmation information.
	// 'true' should be returned if the unsubscription is confirmed.
	ConfirmUnsub(topic string, keys []K, data json.RawMessage) bool
}
