package timeutil

import (
	"context"
	"testing"
	"time"

	"github.com/jellydator/xync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_NewPeriodicExec(t *testing.T) {
	p := NewPeriodicExec(time.Hour, time.Minute, func(_ context.Context) {}, func(_ any) {}, false)
	require.NotNil(t, p)
	assert.Equal(t, time.Hour, p.repeatAfter)
	assert.Equal(t, time.Minute, p.cooldown)
	assert.NotNil(t, p.fn)
	assert.NotNil(t, p.stopCh)
	assert.NotNil(t, p.triggerCh)
	assert.NotNil(t, p.supv)

	p = NewPeriodicExec(time.Minute, time.Hour, func(_ context.Context) {}, func(_ any) {}, false)
	require.NotNil(t, p)
	assert.Equal(t, time.Minute, p.repeatAfter)
	assert.Equal(t, time.Minute, p.cooldown)
	assert.NotNil(t, p.fn)
	assert.NotNil(t, p.stopCh)
	assert.NotNil(t, p.triggerCh)
	assert.NotNil(t, p.supv)
}

func Test_PeriodicExec_Start(t *testing.T) {
	resCh := make(chan struct{})

	drain := func() {
		for {
			select {
			case <-resCh:
			default:
				return
			}
		}
	}

	p := PeriodicExec{
		repeatAfter: time.Millisecond,
		cooldown:    time.Millisecond,
		fn: func(_ context.Context) {
			resCh <- struct{}{}
		},
		stopCh:    make(chan struct{}),
		triggerCh: make(chan struct{}, 1),
		supv:      xync.NewSupervisor(),
		fastStart: true,
	}
	finishCh := make(chan struct{})

	// normal exit
	go func() {
		p.Start()
		finishCh <- struct{}{}
	}()

	<-resCh
	p.stopCh <- struct{}{}

	assert.Eventually(t, func() bool {
		select {
		case <-finishCh:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*500)

	drain()

	// exit after trigger while completing cooldown
	p.repeatAfter = time.Hour
	p.cooldown = time.Hour

	go func() {
		p.Start()
		finishCh <- struct{}{}
	}()

	p.triggerCh <- struct{}{}

	<-resCh
	p.stopCh <- struct{}{}

	assert.Eventually(t, func() bool {
		select {
		case <-finishCh:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*500)

	drain()

	// exit after trigger and cooldown completion
	p.repeatAfter = time.Hour
	p.cooldown = time.Millisecond

	go func() {
		p.Start()
		finishCh <- struct{}{}
	}()

	p.triggerCh <- struct{}{}

	<-resCh

	time.Sleep(time.Millisecond * 100) // give some time for the cooldown to complete
	p.stopCh <- struct{}{}

	assert.Eventually(t, func() bool {
		select {
		case <-finishCh:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*500)

	drain()
}

func Test_PeriodicExec_Stop(_ *testing.T) {
	p := PeriodicExec{
		stopCh: make(chan struct{}),
		supv:   xync.NewSupervisor(),
	}
	p.supv.Go(func(_ context.Context) {
		<-p.stopCh
	})
	p.Stop()
}

func Test_PeriodicExec_Trigger(_ *testing.T) {
	p := PeriodicExec{
		triggerCh: make(chan struct{}),
	}
	p.Trigger()
	p.Trigger() // doesn't block
}

func Test_Sleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	assert.False(t, Sleep(ctx, time.Millisecond))

	cancel()
	assert.True(t, Sleep(ctx, time.Minute))
}
