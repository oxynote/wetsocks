package wsreconn

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/jellydator/xync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewSoloSubKeeper(t *testing.T) {
	fmt := &SoloSubFormatterMock{}
	s := NewSoloSubKeeper(fmt, 1, true)
	require.NotNil(t, s)
	assert.NotNil(t, s.supv)
	assert.Same(t, fmt, s.fmt)
	assert.Equal(t, time.Duration(1), s.cooldown)
	assert.True(t, s.manualConfirm)
	assert.NotNil(t, s.subs)
}

func Test_SoloSubKeeper_Close(t *testing.T) {
	s := SoloSubKeeper{
		supv: xync.NewSupervisor(),
	}

	assert.NotPanics(t, func() {
		s.Close()
	})
}

func Test_SoloSubKeeper_Payloads(t *testing.T) {
	s := SoloSubKeeper{
		cooldown: time.Hour,
		subs: map[string]*soloSub{
			"1": {
				confirmed: true,
				subbed:    true,
			},
			"2": {
				confirmed: true,
				subbed:    false,
			},
			"3": {
				confirmed: false,
				subbed:    true,
			},
			"4": {
				confirmed: false,
				subbed:    false,
			},
		},
	}

	res, d := s.Payloads()
	assert.ElementsMatch(t, []SubPayloader{
		&soloSub{
			confirmed: false,
			subbed:    true,
		},
		&soloSub{
			confirmed: false,
			subbed:    false,
		},
	}, res)
	assert.Equal(t, time.Hour, d)
}

func Test_SoloSubKeeper_ResetAll(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"1": {
				confirmed: true,
				subbed:    true,
			},
			"2": {
				confirmed: true,
				subbed:    false,
			},
			"3": {
				confirmed: false,
				subbed:    true,
			},
			"4": {
				confirmed: false,
				subbed:    false,
			},
		},
	}

	s.ResetAll()
	assert.Equal(t, map[string]*soloSub{
		"1": {
			confirmed: false,
			subbed:    true,
		},
		"3": {
			confirmed: false,
			subbed:    true,
		},
	}, s.subs)
}

func Test_SoloSubKeeper_OnChange(t *testing.T) {
	var s SoloSubKeeper

	s.OnChange(func(_ context.Context) {})
	s.OnChange(func(_ context.Context) {})

	assert.Len(t, s.fns, 2)
}

func Test_SoloSubKeeper_execFns(t *testing.T) {
	var called1, called2 bool

	s := SoloSubKeeper{
		supv: xync.NewSupervisor(),
		fns: []func(context.Context){
			func(_ context.Context) {
				called1 = true
			},
			func(_ context.Context) {
				called2 = true
			},
		},
	}

	s.execFns()
	s.supv.Wait()

	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_SoloSubKeeper_Subscribe(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	s := &SoloSubKeeper{
		supv: xync.NewSupervisor(),
		subs: map[string]*soloSub{
			"hey": {
				subbed:    false,
				confirmed: true,
			},
			"abc": {
				subbed:    false,
				confirmed: false,
			},
		},
		fns: []func(context.Context){
			func(_ context.Context) {
				mu.Lock()
				defer mu.Unlock()

				calls++
			},
		},
	}

	s.Subscribe("hello1")
	s.Subscribe("hello1")
	s.Subscribe("hey")
	s.Subscribe("abc")
	s.Subscribe("test1")

	s.supv.Wait()

	sub := s.subs["hello1"]
	require.NotNil(t, sub)
	assert.Equal(t, "hello1", sub.topic)
	assert.Same(t, s, sub.keeper)
	assert.True(t, sub.subbed)
	assert.False(t, sub.confirmed)

	sub = s.subs["hey"]
	require.NotNil(t, sub)
	assert.Equal(t, "hey", sub.topic)
	assert.Same(t, s, sub.keeper)
	assert.True(t, sub.subbed)
	assert.False(t, sub.confirmed)

	sub = s.subs["abc"]
	require.NotNil(t, sub)
	assert.Zero(t, sub.topic)
	assert.Nil(t, sub.keeper)
	assert.False(t, sub.subbed)
	assert.False(t, sub.confirmed)

	sub = s.subs["test1"]
	require.NotNil(t, sub)
	assert.Equal(t, "test1", sub.topic)
	assert.Same(t, s, sub.keeper)
	assert.True(t, sub.subbed)
	assert.False(t, sub.confirmed)

	assert.Equal(t, 3, calls)
}

func Test_SoloSubKeeper_UnsubscribeLocal(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"test1": {
				subbed:    true,
				confirmed: true,
			},
		},
	}

	s.UnsubscribeLocal("test1")
	assert.NotContains(t, s.subs, "test1")
}

func Test_SoloSubKeeper_Unsubscribe(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	s := SoloSubKeeper{
		supv: xync.NewSupervisor(),
		subs: map[string]*soloSub{
			"test1": {
				subbed: true,
			},
			"test2": {
				subbed:    true,
				confirmed: true,
			},
			"test3": {
				subbed:    true,
				confirmed: true,
			},
			"test4": {
				subbed:    false,
				confirmed: true,
			},
			"test5": {
				subbed: false,
			},
		},
		fns: []func(context.Context){
			func(_ context.Context) {
				mu.Lock()
				defer mu.Unlock()

				calls++
			},
		},
	}

	s.Unsubscribe("123")
	s.Unsubscribe("test1")
	s.Unsubscribe("test2")
	s.Unsubscribe("test3")
	s.Unsubscribe("test4")
	s.Unsubscribe("test5")

	s.supv.Wait()

	assert.NotContains(t, s.subs, "test1")
	assert.NotContains(t, s.subs, "test4")

	sub := s.subs["test2"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test3"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test5"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	assert.Equal(t, 2, calls)
}

func Test_SoloSubKeeper_unsubscribe(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"test11": {
				subbed: true,
			},
			"test22": {
				subbed:    true,
				confirmed: true,
			},
			"test33": {
				subbed:    true,
				confirmed: true,
			},
			"test44": {
				subbed:    false,
				confirmed: true,
			},
			"test55": {
				subbed: false,
			},
		},
	}

	assert.False(t, s.unsubscribe("test11", s.subs["test11"]))
	assert.True(t, s.unsubscribe("test22", s.subs["test22"]))
	assert.True(t, s.unsubscribe("test33", s.subs["test33"]))
	assert.False(t, s.unsubscribe("test44", s.subs["test44"]))
	assert.False(t, s.unsubscribe("test55", s.subs["test55"]))

	assert.NotContains(t, s.subs, "test11")
	assert.NotContains(t, s.subs, "test44")

	sub := s.subs["test22"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test33"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test55"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)
}

func Test_SoloSubKeeper_UnsubscribeAll(t *testing.T) {
	var calls int

	s := SoloSubKeeper{
		supv: xync.NewSupervisor(),
		subs: map[string]*soloSub{
			"test1": {
				subbed: true,
			},
			"test2": {
				subbed:    true,
				confirmed: true,
			},
			"test3": {
				subbed:    true,
				confirmed: true,
			},
			"test4": {
				subbed:    false,
				confirmed: true,
			},
			"test5": {
				subbed: false,
			},
		},
		fns: []func(context.Context){
			func(_ context.Context) {
				calls++
			},
		},
	}

	s.UnsubscribeAll()
	s.supv.Wait()

	assert.NotContains(t, s.subs, "test1")
	assert.NotContains(t, s.subs, "test4")

	sub := s.subs["test2"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test3"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	sub = s.subs["test5"]
	require.NotNil(t, sub)
	assert.False(t, sub.confirmed)
	assert.False(t, sub.subbed)

	assert.Equal(t, 1, calls)

	// without fns
	calls = 0

	s.UnsubscribeAll()
	s.supv.Wait()

	assert.Contains(t, s.subs, "test2")
	assert.Contains(t, s.subs, "test3")
	assert.Contains(t, s.subs, "test5")

	assert.Zero(t, calls)
}

func Test_SoloSubKeeper_ConfirmSub(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"a": {
				subbed: true,
			},
		},
	}

	s.ConfirmSub("1")
	s.ConfirmSub("a")

	require.Len(t, s.subs, 1)
	assert.True(t, s.subs["a"].confirmed)
}

func Test_SoloSubKeeper_ConfirmUnsub(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"a": {
				subbed: false,
			},
		},
	}

	s.ConfirmUnsub("1")
	s.ConfirmUnsub("a")

	require.Empty(t, s.subs)
}

func Test_SoloSubKeeper_confirm(t *testing.T) {
	s := SoloSubKeeper{
		subs: map[string]*soloSub{
			"1": {
				subbed: false,
			},
			"2": {
				subbed:    false,
				confirmed: true,
			},
			"3": {
				subbed: true,
			},
		},
	}

	s.confirm(false, "2", s.subs["2"])
	s.confirm(true, "1", s.subs["1"])
	s.confirm(false, "1", s.subs["1"])
	s.confirm(true, "3", s.subs["3"])

	assert.NotContains(t, s.subs, "1")

	sub := s.subs["2"]
	assert.True(t, sub.confirmed)

	sub = s.subs["3"]
	assert.True(t, sub.confirmed)
}

func Test_soloSub_Payload(t *testing.T) {
	// sub
	fmt := &SoloSubFormatterMock{
		SubMessageFunc: func(_ string) any {
			return 1
		},
		ConfirmSubFunc: func(_ string, _ json.RawMessage) bool {
			return true
		},
	}
	s := &SoloSubKeeper{
		fmt: fmt,
	}
	sub := soloSub{
		keeper: s,
		topic:  "hey",
		subbed: true,
	}

	v, fn := sub.Payload() // success
	require.NotNil(t, fn)
	assert.Equal(t, 1, v)

	fn(json.RawMessage(`{"hello":"123"}`))
	assert.True(t, sub.confirmed)

	ffSub := fmt.SubMessageCalls()
	require.Len(t, ffSub, 1)
	assert.Equal(t, "hey", ffSub[0].Topic)

	ffConfSub := fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 1)
	assert.Equal(t, "hey", ffConfSub[0].Topic)
	assert.JSONEq(t, `{"hello":"123"}`, string(ffConfSub[0].Data))

	sub.confirmed = false
	fmt.ConfirmSubFunc = func(_ string, _ json.RawMessage) bool {
		return false
	}

	v, fn = sub.Payload() // confirm returns false
	require.NotNil(t, fn)
	assert.Equal(t, 1, v)

	fn(json.RawMessage(`{"hello":"22"}`))
	assert.False(t, sub.confirmed)

	ffSub = fmt.SubMessageCalls()
	require.Len(t, ffSub, 2)
	assert.Equal(t, "hey", ffSub[1].Topic)

	ffConfSub = fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 2)
	assert.Equal(t, "hey", ffConfSub[1].Topic)
	assert.JSONEq(t, `{"hello":"22"}`, string(ffConfSub[1].Data))

	sub.keeper.manualConfirm = true

	v, fn = sub.Payload() // without confirm fn
	require.Nil(t, fn)
	assert.Equal(t, 1, v)

	ffSub = fmt.SubMessageCalls()
	require.Len(t, ffSub, 3)
	assert.Equal(t, "hey", ffSub[2].Topic)

	ffConfSub = fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 2)

	// unsub
	fmt = &SoloSubFormatterMock{
		UnsubMessageFunc: func(_ string) any {
			return 2
		},
		ConfirmUnsubFunc: func(_ string, _ json.RawMessage) bool {
			return true
		},
	}
	sub.keeper.manualConfirm = false
	sub.keeper.fmt = fmt
	sub.subbed = false

	v, fn = sub.Payload() // success
	require.NotNil(t, fn)
	assert.Equal(t, 2, v)

	fn(json.RawMessage(`{"hello":"45"}`))
	assert.True(t, sub.confirmed)

	ffUnsub := fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 1)
	assert.Equal(t, "hey", ffUnsub[0].Topic)

	ffConfUnsub := fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 1)
	assert.Equal(t, "hey", ffConfUnsub[0].Topic)
	assert.JSONEq(t, `{"hello":"45"}`, string(ffConfUnsub[0].Data))

	sub.confirmed = false
	fmt.ConfirmUnsubFunc = func(_ string, _ json.RawMessage) bool {
		return false
	}

	v, fn = sub.Payload() // confirm returns false
	require.NotNil(t, fn)
	assert.Equal(t, 2, v)

	fn(json.RawMessage(`{"hello":"22"}`))
	assert.False(t, sub.confirmed)

	ffUnsub = fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 2)
	assert.Equal(t, "hey", ffUnsub[1].Topic)

	ffConfUnsub = fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 2)
	assert.Equal(t, "hey", ffConfUnsub[1].Topic)
	assert.JSONEq(t, `{"hello":"22"}`, string(ffConfUnsub[1].Data))

	sub.keeper.manualConfirm = true

	v, fn = sub.Payload() // without confirm fn
	require.Nil(t, fn)
	assert.Equal(t, 2, v)

	ffUnsub = fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 3)
	assert.Equal(t, "hey", ffUnsub[2].Topic)

	ffConfUnsub = fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 2)
}
