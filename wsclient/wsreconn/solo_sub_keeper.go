package wsreconn

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jellydator/xync"
)

// SoloSubKeeper holds subscriptions whose payloads are sent one by one.
type SoloSubKeeper struct {
	supv          *xync.Supervisor
	fmt           SoloSubFormatter
	cooldown      time.Duration
	manualConfirm bool

	subsMu sync.RWMutex
	subs   map[string]*soloSub

	fnsMu sync.RWMutex
	fns   []func(context.Context)
}

// NewSoloSubKeeper creates a fresh instance of SoloSubKeeper.
// The duration parameter determines how long to wait between payload events.
// The bool parameter determines if a manual confirmation should be
// expected or not.
func NewSoloSubKeeper(fmt SoloSubFormatter, cooldown time.Duration, manualConfirm bool) *SoloSubKeeper {
	return &SoloSubKeeper{
		supv:          xync.NewSupervisor(),
		fmt:           fmt,
		cooldown:      cooldown,
		manualConfirm: manualConfirm,
		subs:          make(map[string]*soloSub),
	}
}

// Close stops all background processes.
func (s *SoloSubKeeper) Close() {
	s.supv.CloseAndWait()
}

// Payloads returns a slice of payloads that should be sent through a
// WebSocket connection and a duration value indicating how much time has to
// pass after each write before a new one is sent.
func (s *SoloSubKeeper) Payloads() (res []SubPayloader, d time.Duration) {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()

	for _, sub := range s.subs {
		if sub.confirmed {
			continue
		}

		res = append(res, sub)
	}

	return res, s.cooldown
}

// ResetAll resets every subscription to an unconfirmed 'subscribed' state.
// It does not trigger any change functions.
func (s *SoloSubKeeper) ResetAll() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	for topic, sub := range s.subs {
		sub.confirmed = false
		if !sub.subbed {
			delete(s.subs, topic)
		}
	}
}

// OnChange sets the provided function to be executed
// when subscription-related data changes.
func (s *SoloSubKeeper) OnChange(fn func(context.Context)) {
	s.fnsMu.Lock()
	s.fns = append(s.fns, fn)
	s.fnsMu.Unlock()
}

// execFns executes all change-subscribed functions.
func (s *SoloSubKeeper) execFns() {
	s.fnsMu.RLock()
	defer s.fnsMu.RUnlock()

	for _, fn := range s.fns {
		fn := fn

		s.supv.Go(func(gctx context.Context) {
			fn(gctx)
		})
	}
}

// Subscribe subscribes to the provided topic.
func (s *SoloSubKeeper) Subscribe(topic string) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := s.subs[topic]

	switch {
	case sub == nil || (!sub.subbed && sub.confirmed):
	default:
		return
	}

	s.subs[topic] = &soloSub{
		keeper: s,
		topic:  topic,
		subbed: true,
	}

	s.execFns()
}

// Unsubscribe unsubscribes from the provided topic.
func (s *SoloSubKeeper) Unsubscribe(topic string) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := s.subs[topic]
	if sub == nil {
		return
	}

	if s.unsubscribe(topic, sub) {
		s.execFns()
	}
}

// unsubscribe deletes/marks the provided subscriber as unsubscribed.
// The bool return value indicates if the subscriber has been updated.
// Not concurrently safe.
func (s *SoloSubKeeper) unsubscribe(topic string, sub *soloSub) bool {
	if sub.subbed && !sub.confirmed || !sub.subbed && sub.confirmed {
		// delete if no additional payloads need to be sent
		delete(s.subs, topic)
		return false
	} else if !sub.subbed && !sub.confirmed {
		return false
	}

	sub.confirmed = false
	sub.subbed = false

	return true
}

// UnsubscribeAll unsubscribes from all topics.
func (s *SoloSubKeeper) UnsubscribeAll() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	var ok bool

	for topic, sub := range s.subs {
		ok1 := s.unsubscribe(topic, sub)
		if !ok {
			ok = ok1
		}
	}

	if ok {
		s.execFns()
	}
}

// ConfirmSub manually confirms a subcription.
func (s *SoloSubKeeper) ConfirmSub(topic string) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := s.subs[topic]
	if sub == nil {
		return
	}

	s.confirm(true, topic, sub)
}

// ConfirmUnsub manually confirms an unsubcription.
func (s *SoloSubKeeper) ConfirmUnsub(topic string) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := s.subs[topic]
	if sub == nil {
		return
	}

	s.confirm(false, topic, sub)
}

// confirm manually confirms a topic subscription.
// Not concurrently safe.
func (s *SoloSubKeeper) confirm(subbed bool, topic string, sub *soloSub) {
	if sub.confirmed || sub.subbed != subbed {
		return
	}

	sub.confirmed = true

	if !subbed {
		delete(s.subs, topic)
	}
}

// soloSub represents a single subscriber.
type soloSub struct {
	keeper    *SoloSubKeeper
	topic     string
	confirmed bool
	subbed    bool // subbed should be checked if confirmed is false
}

// Payload returns a payload that should be sent through
// a WebSocket connection and a function that should be used
// when confirming a response.
// The function return value might be nil if no confirmation is needed.
func (s *soloSub) Payload() (any, func(json.RawMessage)) {
	var fn func(d json.RawMessage)

	if !s.keeper.manualConfirm {
		fn = func(d json.RawMessage) {
			s.keeper.subsMu.Lock()
			defer s.keeper.subsMu.Unlock()

			if s.subbed {
				if s.keeper.fmt.ConfirmSub(s.topic, d) {
					s.keeper.confirm(true, s.topic, s)
				}

				return
			}

			if s.keeper.fmt.ConfirmUnsub(s.topic, d) {
				s.keeper.confirm(false, s.topic, s)
			}
		}
	}

	s.keeper.subsMu.RLock()
	defer s.keeper.subsMu.RUnlock()

	if s.subbed {
		return s.keeper.fmt.SubMessage(s.topic), fn
	}

	return s.keeper.fmt.UnsubMessage(s.topic), fn
}

// SoloSubFormatter is an interface that handles solo subscribers' message
// formatting.
//
//go:generate ../../scripts/codegen/mock -t internal SoloSubFormatter
type SoloSubFormatter interface {
	// SubMessage should return data that should be sent as a
	// subscription message.
	// If the message cannot be produced (e.g., on error), it
	// should return nil, in which case the subscription will be
	// skipped and retried later.
	SubMessage(topic string) any

	// UnsubMessage should return data that should be sent as an
	// unsubscription message.
	// If the message cannot be produced (e.g., on error), it
	// should return nil, in which case the unsubscription will be
	// skipped and retried later.
	UnsubMessage(topic string) any

	// ConfirmSub should confirm the subscription if the provided data
	// contains the confirmation information.
	// 'true' should be returned if the subscription is confirmed.
	ConfirmSub(topic string, data json.RawMessage) bool

	// ConfirmUnsub should confirm the unsubscription if the provided
	// data contains the confirmation information.
	// 'true' should be returned if the unsubscription is confirmed.
	ConfirmUnsub(topic string, data json.RawMessage) bool
}
