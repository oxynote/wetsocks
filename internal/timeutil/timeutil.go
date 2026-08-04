// Package timeutil implements helper functions for time-related logic
// which extends the functionality of the standard time package.
package timeutil

import (
	"context"
	"time"

	"github.com/jellydator/xync"
)

// PeriodicExec handles repeated execution of a function.
type PeriodicExec struct {
	repeatAfter time.Duration
	cooldown    time.Duration
	fn          func(context.Context)
	stopCh      chan struct{}
	triggerCh   chan struct{}
	supv        *xync.Supervisor
	fastStart   bool
}

// NewPeriodicExec creates a fresh instance of the PeriodicExec type.
// The cooldown parameter determines how long the periodic executer
// should wait after the Trigger method is called. This value cannot be
// greater than the 'repeatAfter' value and if it is, it is automatically
// set to the 'repeatAfter' value.
// The timer that uses the 'repeatAfter' value is (re)started
// after the active timer expires or the Trigger method is called but
// before the cooldown is activated.
// The recoveryFn function is used in case of a panic.
func NewPeriodicExec(
	repeatAfter, cooldown time.Duration,
	execFn func(context.Context),
	recoveryFn func(any),
	fastStart bool,
) *PeriodicExec {
	if cooldown > repeatAfter {
		cooldown = repeatAfter
	}

	return &PeriodicExec{
		repeatAfter: repeatAfter,
		cooldown:    cooldown,
		fn:          execFn,
		stopCh:      make(chan struct{}),
		triggerCh:   make(chan struct{}, 1), // we need to buffer 1 event during cooldown
		supv: xync.NewSupervisor(
			xync.WithSupervisorRecovery(recoveryFn),
		),
		fastStart: fastStart,
	}
}

// Start starts the repeated execution of the function.
// It blocks until Stop is called.
func (p *PeriodicExec) Start() {
	startAfter := p.repeatAfter

	if p.fastStart {
		startAfter = 0
	}

	timer := time.NewTimer(startAfter)
	stop := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
				// NOCOV: since the timer is created
				// and stopped inside this function, there
				// is no way to stop it from the outside to
				// trigger this case.
			default:
			}
		}
	}

	defer stop()

	for {
		var trg bool

		select {
		case <-p.stopCh:
			return
		case <-timer.C:
		case <-p.triggerCh:
			stop()

			trg = true
		}

		p.supv.Go(func(ctx context.Context) {
			p.fn(ctx)
		})
		timer.Reset(p.repeatAfter)

		if !trg {
			continue
		}

		cooldown := time.NewTimer(p.cooldown)

		select {
		case <-p.stopCh:
			if !cooldown.Stop() {
				select {
				case <-cooldown.C:
					// NOCOV: since the timer is created
					// and stopped inside this function,
					// there is no way to stop it from
					// the outside to trigger this case.
				default:
				}
			}

			return
		case <-cooldown.C:
		}
	}
}

// Stop stops the repeated execution of the function.
// It blocks until Start exits.
func (p *PeriodicExec) Stop() {
	p.stopCh <- struct{}{}
	p.supv.StopAndWait()
}

// Trigger forces the function to be executed even if the configured
// duration has not passed yet since the last execution.
func (p *PeriodicExec) Trigger() {
	select {
	case p.triggerCh <- struct{}{}:
	default:
	}
}
