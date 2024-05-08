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

func Test_NewGroupedSubKeeper(t *testing.T) {
	fmt := &GroupedSubFormatterMock[string]{}
	g := NewGroupedSubKeeper(fmt, 1, true)
	require.NotNil(t, g)
	assert.NotNil(t, g.supv)
	assert.Same(t, fmt, g.fmt)
	assert.Equal(t, time.Duration(1), g.cooldown)
	assert.True(t, g.manualConfirm)
	assert.NotNil(t, g.subs)
}

func Test_GroupedSubKeeper_Close(t *testing.T) {
	g := GroupedSubKeeper[string]{
		supv: xync.NewSupervisor(),
	}

	assert.NotPanics(t, func() {
		g.Close()
	})
}

func Test_GrouptedSubKeeper_Payloads(t *testing.T) {
	g := &GroupedSubKeeper[string]{
		cooldown: time.Hour,
		subs: map[string]map[string]groupedSub{
			"1": {
				"1_1": {
					confirmed: true,
					count:     20,
				},
				"1_2": {
					confirmed: false,
					count:     20,
				},
				"1_3": {
					confirmed: false,
					count:     0,
				},
			},
			"2": {
				"2_1": {
					confirmed: true,
					count:     10,
				},
				"2_2": {
					confirmed: false,
					count:     10,
				},
				"2_3": {
					confirmed: false,
					count:     0,
				},
			},
			"3": {
				"3_1": {
					confirmed: true,
					count:     30,
				},
				"3_2": {
					confirmed: true,
					count:     0,
				},
			},
		},
	}

	res, d := g.Payloads()
	assert.ElementsMatch(t, []SubPayloader{
		groupedPayload[string]{
			keeper: g,
			topic:  "1",
			subbed: true,
			keys: []string{
				"1_2",
			},
			topicData: g.subs["1"],
		},
		groupedPayload[string]{
			keeper: g,
			topic:  "1",
			subbed: false,
			keys: []string{
				"1_3",
			},
			topicData: g.subs["1"],
		},
		groupedPayload[string]{
			keeper: g,
			topic:  "2",
			subbed: true,
			keys: []string{
				"2_2",
			},
			topicData: g.subs["2"],
		},
		groupedPayload[string]{
			keeper: g,
			topic:  "2",
			subbed: false,
			keys: []string{
				"2_3",
			},
			topicData: g.subs["2"],
		},
	}, res)
	assert.Equal(t, time.Hour, d)
}

func Test_GroupedSubKeeper_ResetAll(t *testing.T) {
	g := GroupedSubKeeper[string]{
		cooldown: time.Hour,
		subs: map[string]map[string]groupedSub{
			"A": {
				"A_1": {
					confirmed: true,
					count:     20,
				},
				"A_2": {
					confirmed: false,
					count:     20,
				},
				"A_3": {
					confirmed: false,
					count:     0,
				},
			},
			"B": {
				"B_1": {
					confirmed: true,
					count:     10,
				},
				"B_2": {
					confirmed: false,
					count:     10,
				},
				"B_3": {
					confirmed: false,
					count:     0,
				},
			},
			"C": {
				"C_2": {
					confirmed: true,
					count:     0,
				},
			},
		},
	}

	g.ResetAll()
	assert.Equal(t, map[string]map[string]groupedSub{
		"A": {
			"A_1": {
				confirmed: false,
				count:     20,
			},
			"A_2": {
				confirmed: false,
				count:     20,
			},
		},
		"B": {
			"B_1": {
				confirmed: false,
				count:     10,
			},
			"B_2": {
				confirmed: false,
				count:     10,
			},
		},
	}, g.subs)
}

func Test_GroupedSubKeeper_OnChange(t *testing.T) {
	var g GroupedSubKeeper[string]

	g.OnChange(func(_ context.Context) {})
	g.OnChange(func(_ context.Context) {})

	assert.Len(t, g.fns, 2)
}

func Test_GroupedSubKeeper_execFns(t *testing.T) {
	var called1, called2 bool

	g := GroupedSubKeeper[string]{
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

	g.execFns()
	g.supv.Wait()

	assert.True(t, called1)
	assert.True(t, called2)
}

func Test_GroupedSubKeeper_TopicKeys(t *testing.T) {
	k1 := "Q_1"
	k2 := "Q_2"
	k3 := "Q_3"
	k4 := "Q_4"
	g := &GroupedSubKeeper[string]{
		supv: xync.NewSupervisor(),
		subs: make(map[string]map[string]groupedSub),
	}

	// nil
	assert.Nil(t, g.TopicKeys("Q"))

	// non nil
	g.subs = map[string]map[string]groupedSub{
		"Q": {
			k1: {
				confirmed: true,
				count:     20,
			},
			k2: {
				confirmed: false,
				count:     20,
			},
			k3: {
				confirmed: true,
				count:     30,
			},
			k4: {
				confirmed: true,
				count:     0,
			},
		},
		"B": {},
	}
	assert.ElementsMatch(t, []string{
		k1,
		k3,
	}, g.TopicKeys("Q"))
	assert.Nil(t, g.TopicKeys("B"))
}

func Test_GroupedSubKeeper_Subscribe(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	g := &GroupedSubKeeper[string]{
		supv: xync.NewSupervisor(),
		subs: map[string]map[string]groupedSub{
			"Q": {
				"Q_1": {
					confirmed: true,
					count:     20,
				},
				"Q_2": {
					confirmed: false,
					count:     20,
				},
				"Q_3": {
					confirmed: true,
					count:     0,
				},
				"Q_4": {
					confirmed: false,
					count:     0,
				},
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

	g.Subscribe("Q",
		"Q_1",
		"Q_2",
		"Q_3",
		"Q_4",
		"Q_5",
	)
	g.Subscribe("W",
		"W_1",
		"W_2",
	)
	g.Subscribe("W",
		"W_1",
		"W_2",
	)

	g.supv.Wait()

	assert.Equal(t, map[string]map[string]groupedSub{
		"Q": {
			"Q_1": {
				confirmed: true,
				count:     21,
			},
			"Q_2": {
				confirmed: false,
				count:     21,
			},
			"Q_3": {
				confirmed: false,
				count:     1,
			},
			"Q_4": {
				confirmed: false,
				count:     1,
			},
			"Q_5": {
				confirmed: false,
				count:     1,
			},
		},
		"W": {
			"W_1": {
				confirmed: false,
				count:     2,
			},
			"W_2": {
				confirmed: false,
				count:     2,
			},
		},
	}, g.subs)

	assert.Equal(t, 2, calls)
}

func Test_GroupedSubKeeper_Unsubscribe(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	g := &GroupedSubKeeper[string]{
		supv: xync.NewSupervisor(),
		subs: map[string]map[string]groupedSub{
			"V": {
				"V_1": {
					confirmed: false,
					count:     20,
				},
				"V_2": {
					confirmed: true,
					count:     1,
				},
				"V_3": {
					confirmed: false,
					count:     0,
				},
				"V_4": {
					confirmed: false,
					count:     1,
				},
				"V_5": {
					confirmed: true,
					count:     0,
				},
			},
			"B": {},
		},
		fns: []func(context.Context){
			func(_ context.Context) {
				mu.Lock()
				defer mu.Unlock()

				calls++
			},
		},
	}

	g.Unsubscribe("V",
		"V_0",
		"V_1",
		"V_2",
		"V_3",
		"V_4",
		"V_5",
	)
	g.supv.Wait()

	assert.Equal(t, map[string]map[string]groupedSub{
		"V": {
			"V_1": {
				confirmed: false,
				count:     19,
			},
			"V_2": {
				confirmed: false,
				count:     0,
			},
			"V_3": {
				confirmed: false,
				count:     0,
			},
		},
		"B": {},
	}, g.subs)
	assert.Equal(t, 1, calls)

	// empty subs list
	calls = 0

	g.Unsubscribe("B", "B_1")
	g.supv.Wait()
	assert.NotContains(t, g.subs, "B")
	assert.Zero(t, 0, calls)

	// not found
	calls = 0

	g.Unsubscribe("B", "B_1")
	g.supv.Wait()
	assert.NotContains(t, g.subs, "B")
	assert.Zero(t, 0, calls)
}

func Test_GroupedSubKeeper_unsubscribe(t *testing.T) {
	g := &GroupedSubKeeper[string]{
		subs: map[string]map[string]groupedSub{
			"V": {
				"V_1": {
					confirmed: false,
					count:     20,
				},
				"V_2": {
					confirmed: true,
					count:     1,
				},
				"V_3": {
					confirmed: false,
					count:     0,
				},
				"V_4": {
					confirmed: false,
					count:     1,
				},
				"V_5": {
					confirmed: true,
					count:     0,
				},
			},
		},
	}

	res := g.unsubscribe("V",
		[]string{
			"V_0",
			"V_1",
			"V_2",
			"V_3",
			"V_4",
			"V_5",
		},
		g.subs["V"],
		false,
	)
	assert.True(t, res)

	assert.Equal(t, map[string]map[string]groupedSub{
		"V": {
			"V_1": {
				confirmed: false,
				count:     19,
			},
			"V_2": {
				confirmed: false,
				count:     0,
			},
			"V_3": {
				confirmed: false,
				count:     0,
			},
		},
	}, g.subs)

	// decr max
	res = g.unsubscribe("V",
		[]string{
			"V_1",
			"V_2",
			"V_3",
		},
		g.subs["V"],
		true,
	)
	assert.True(t, res)

	assert.Equal(t, map[string]map[string]groupedSub{
		"V": {
			"V_1": {
				confirmed: false,
				count:     0,
			},
			"V_2": {
				confirmed: false,
				count:     0,
			},
			"V_3": {
				confirmed: false,
				count:     0,
			},
		},
	}, g.subs)

	// empty subs list
	res = g.unsubscribe("V",
		[]string{
			"V_1",
			"V_2",
			"V_3",
		},
		nil,
		true,
	)
	assert.False(t, res)
	assert.NotContains(t, g.subs, "V")
}

func Test_GroupedSubKeeper_UnsubscribeAll(t *testing.T) {
	var calls int

	g := &GroupedSubKeeper[string]{
		supv: xync.NewSupervisor(),
		subs: map[string]map[string]groupedSub{
			"V": {
				"V_1": {
					confirmed: false,
					count:     20,
				},
				"V_2": {
					confirmed: true,
					count:     1,
				},
				"V_3": {
					confirmed: false,
					count:     0,
				},
				"V_4": {
					confirmed: false,
					count:     1,
				},
				"V_5": {
					confirmed: true,
					count:     0,
				},
			},
			"B": {},
		},
		fns: []func(context.Context){
			func(_ context.Context) {
				calls++
			},
		},
	}

	g.UnsubscribeAll()
	g.supv.Wait()

	assert.Equal(t, map[string]map[string]groupedSub{
		"V": {
			"V_1": {
				confirmed: false,
				count:     0,
			},
			"V_2": {
				confirmed: false,
				count:     0,
			},
			"V_3": {
				confirmed: false,
				count:     0,
			},
		},
	}, g.subs)
	assert.Equal(t, 1, calls)

	// not found
	calls = 0

	g.UnsubscribeAll()
	g.supv.Wait()
	assert.Zero(t, 0, calls)
}

func Test_GroupedSubKeeper_ConfirmSub(t *testing.T) {
	g := &GroupedSubKeeper[string]{
		subs: map[string]map[string]groupedSub{
			"T": {
				"T_1": {
					confirmed: true,
					count:     20,
				},
				"T_2": {
					confirmed: false,
					count:     0,
				},
				"T_3": {
					confirmed: false,
					count:     10,
				},
			},
		},
	}

	g.ConfirmSub(
		"T",
		"T_0",
		"T_1",
		"T_2",
		"T_3",
	)
	g.ConfirmSub(
		"U",
		"U_0",
	)

	assert.Equal(t, map[string]map[string]groupedSub{
		"T": {
			"T_1": {
				confirmed: true,
				count:     20,
			},
			"T_2": {
				confirmed: false,
				count:     0,
			},
			"T_3": {
				confirmed: true,
				count:     10,
			},
		},
	}, g.subs)
}

func Test_GroupedSubKeeper_ConfirmUnsub(t *testing.T) {
	g := &GroupedSubKeeper[string]{
		subs: map[string]map[string]groupedSub{
			"G": {
				"G_1": {
					confirmed: true,
					count:     20,
				},
				"G_2": {
					confirmed: false,
					count:     10,
				},
				"G_3": {
					confirmed: false,
					count:     0,
				},
			},
			"O": {
				"O_1": {
					confirmed: true,
					count:     20,
				},
				"O_2": {
					confirmed: false,
					count:     10,
				},
				"O_3": {
					confirmed: false,
					count:     0,
				},
				"O_4": {
					confirmed: false,
					count:     0,
				},
			},
		},
	}

	g.ConfirmUnsub(
		"G",
		false,
		"G_0",
		"G_1",
		"G_2",
		"G_3",
	)
	g.ConfirmUnsub(
		"Y",
		false,
		"Y_1",
	)
	g.ConfirmUnsub(
		"O",
		true,
		"O_1",
	)

	assert.Equal(t, map[string]map[string]groupedSub{
		"G": {
			"G_1": {
				confirmed: true,
				count:     20,
			},
			"G_2": {
				confirmed: false,
				count:     10,
			},
		},
		"O": {
			"O_1": {
				confirmed: true,
				count:     20,
			},
			"O_2": {
				confirmed: false,
				count:     10,
			},
		},
	}, g.subs)
}

func Test_GroupedSubKeeper_confirm(t *testing.T) {
	g := &GroupedSubKeeper[string]{
		subs: map[string]map[string]groupedSub{
			"T": {
				"T_1": {
					confirmed: true,
					count:     20,
				},
				"T_2": {
					confirmed: false,
					count:     0,
				},
				"T_3": {
					confirmed: false,
					count:     10,
				},
			},
			"G": {
				"G_1": {
					confirmed: true,
					count:     20,
				},
				"G_2": {
					confirmed: false,
					count:     10,
				},
				"G_3": {
					confirmed: false,
					count:     0,
				},
			},
			"Y": {
				"Y_1": {
					confirmed: false,
					count:     0,
				},
			},
		},
	}

	g.confirm(
		true,
		"T",
		g.subs["T"],
		"T_0",
		"T_1",
		"T_2",
		"T_3",
	)
	g.confirm(
		false,
		"G",
		g.subs["G"],
		"G_0",
		"G_1",
		"G_2",
		"G_3",
	)
	g.confirm(
		false,
		"Y",
		g.subs["Y"],
		"Y_1",
	)

	assert.Equal(t, map[string]map[string]groupedSub{
		"T": {
			"T_1": {
				confirmed: true,
				count:     20,
			},
			"T_2": {
				confirmed: false,
				count:     0,
			},
			"T_3": {
				confirmed: true,
				count:     10,
			},
		},
		"G": {
			"G_1": {
				confirmed: true,
				count:     20,
			},
			"G_2": {
				confirmed: false,
				count:     10,
			},
		},
	}, g.subs)
}

func Test_groupedPayload_Payload(t *testing.T) {
	key := "G_1"

	// sub
	fmt := &GroupedSubFormatterMock[string]{
		SubMessageFunc: func(_ string, _ []string) any {
			return 1
		},
		ConfirmSubFunc: func(_ string, _ []string, _ json.RawMessage) bool {
			return true
		},
	}
	g := &GroupedSubKeeper[string]{
		fmt: fmt,
	}
	pay := groupedPayload[string]{
		keeper: g,
		topic:  "hey",
		subbed: true,
		keys: []string{
			key,
		},
		topicData: map[string]groupedSub{
			key: {
				count: 10,
			},
		},
	}

	v, fn := pay.Payload() // success
	require.NotNil(t, fn)
	assert.Equal(t, 1, v)

	fn(json.RawMessage(`{"hello":"123"}`))
	assert.True(t, pay.topicData[key].confirmed)

	ffSub := fmt.SubMessageCalls()
	require.Len(t, ffSub, 1)
	assert.Equal(t, "hey", ffSub[0].Topic)
	assert.Equal(t, []string{key}, ffSub[0].Keys)

	ffConfSub := fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 1)
	assert.Equal(t, "hey", ffConfSub[0].Topic)
	assert.Equal(t, []string{key}, ffConfSub[0].Keys)
	assert.JSONEq(t, `{"hello":"123"}`, string(ffConfSub[0].Data))

	sub := pay.topicData[key]
	sub.confirmed = false
	pay.topicData[key] = sub
	fmt.ConfirmSubFunc = func(_ string, _ []string, _ json.RawMessage) bool {
		return false
	}

	v, fn = pay.Payload() // confirm returns false
	require.NotNil(t, fn)
	assert.Equal(t, 1, v)

	fn(json.RawMessage(`{"hello":"22"}`))
	assert.False(t, pay.topicData[key].confirmed)

	ffSub = fmt.SubMessageCalls()
	require.Len(t, ffSub, 2)
	assert.Equal(t, "hey", ffSub[1].Topic)
	assert.Equal(t, []string{key}, ffSub[1].Keys)

	ffConfSub = fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 2)
	assert.Equal(t, "hey", ffConfSub[1].Topic)
	assert.Equal(t, []string{key}, ffConfSub[1].Keys)
	assert.JSONEq(t, `{"hello":"22"}`, string(ffConfSub[1].Data))

	pay.keeper.manualConfirm = true

	v, fn = pay.Payload() // without confirm fn
	require.Nil(t, fn)
	assert.Equal(t, 1, v)

	ffSub = fmt.SubMessageCalls()
	require.Len(t, ffSub, 3)
	assert.Equal(t, "hey", ffSub[2].Topic)
	assert.Equal(t, []string{key}, ffSub[2].Keys)

	ffConfSub = fmt.ConfirmSubCalls()
	require.Len(t, ffConfSub, 2)

	// unsub
	fmt = &GroupedSubFormatterMock[string]{
		UnsubMessageFunc: func(_ string, _ []string) any {
			return 2
		},
		ConfirmUnsubFunc: func(_ string, _ []string, _ json.RawMessage) bool {
			return true
		},
	}
	pay.keeper.manualConfirm = false
	pay.keeper.fmt = fmt
	pay.subbed = false
	pay.topicData[key] = groupedSub{
		count: 0,
	}

	v, fn = pay.Payload() // success
	require.NotNil(t, fn)
	assert.Equal(t, 2, v)

	fn(json.RawMessage(`{"hello":"45"}`))
	assert.NotContains(t, pay.topicData, key)

	ffUnsub := fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 1)
	assert.Equal(t, "hey", ffUnsub[0].Topic)
	assert.Equal(t, []string{key}, ffUnsub[0].Keys)

	ffConfUnsub := fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 1)
	assert.Equal(t, "hey", ffConfUnsub[0].Topic)
	assert.Equal(t, []string{key}, ffConfUnsub[0].Keys)
	assert.JSONEq(t, `{"hello":"45"}`, string(ffConfUnsub[0].Data))

	pay.topicData[key] = groupedSub{
		count: 0,
	}
	fmt.ConfirmUnsubFunc = func(_ string, _ []string, _ json.RawMessage) bool {
		return false
	}

	v, fn = pay.Payload() // confirm returns false
	require.NotNil(t, fn)
	assert.Equal(t, 2, v)

	fn(json.RawMessage(`{"hello":"22"}`))
	assert.Contains(t, pay.topicData, key)

	ffUnsub = fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 2)
	assert.Equal(t, "hey", ffUnsub[1].Topic)
	assert.Equal(t, []string{key}, ffUnsub[1].Keys)

	ffConfUnsub = fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 2)
	assert.Equal(t, "hey", ffConfUnsub[1].Topic)
	assert.Equal(t, []string{key}, ffConfUnsub[1].Keys)
	assert.JSONEq(t, `{"hello":"22"}`, string(ffConfUnsub[1].Data))

	pay.keeper.manualConfirm = true

	v, fn = pay.Payload() // without confirm fn
	require.Nil(t, fn)
	assert.Equal(t, 2, v)

	ffUnsub = fmt.UnsubMessageCalls()
	require.Len(t, ffUnsub, 3)
	assert.Equal(t, "hey", ffUnsub[2].Topic)
	assert.Equal(t, []string{key}, ffUnsub[2].Keys)

	ffConfUnsub = fmt.ConfirmUnsubCalls()
	require.Len(t, ffConfUnsub, 2)
}
