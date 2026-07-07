package stream

import (
	"context"
	"math/rand"
	"time"
)

// maxRecreateBackoff caps exponential backoff between recreate
// attempts. Sized for fleet deployments: at 30s a 1000-camera setup
// recovering from a switch reboot would generate sustained
// reconnect traffic; 5 minutes lets the network settle.
const maxRecreateBackoff = 5 * time.Minute

// jitterFraction prevents thundering-herd reconnects when many
// cameras drop together (switch reboot, NAT timeout).
const jitterFraction = 0.25

// pullLoop runs PullMessages → decode → Events. After
// ReconnectAfterFailures consecutive errors it asks attemptRecreate
// to rebuild the subscription. The next batch's events carry
// AfterReconnect=true so consumers can suppress the Initialized
// replay ONVIF emits on a new subscription.
func (s *Stream) pullLoop(ctx context.Context) {
	var failures int
	recreateBackoff := s.opts.RetryBackoff
	var afterReconnect bool

	for {
		if ctx.Err() != nil {
			return
		}
		msgs, err := pullMessages(s.caller, s.getPullPoint(), s.opts)
		if err != nil {
			s.surfaceError(ErrPullFailed{Err: err})
			failures++
			if !s.opts.DisableReconnect && failures >= s.opts.ReconnectAfterFailures {
				justRecreated, cont := s.attemptRecreate(ctx, &failures, &recreateBackoff)
				if !cont {
					return
				}
				if justRecreated {
					afterReconnect = true
				}
				continue
			}
			if !sleepCtx(ctx, s.opts.RetryBackoff) {
				return
			}
			continue
		}
		failures = 0
		recreateBackoff = s.opts.RetryBackoff
		observedAt := s.now()
		for _, m := range msgs {
			ev := decode(m, s.opts.DeviceID, observedAt)
			if afterReconnect {
				ev.AfterReconnect = true
				// Clear once the camera transitions past the
				// Initialized replay to live events.
				if ev.Operation != PropertyInitialized {
					afterReconnect = false
				}
			}
			select {
			case <-ctx.Done():
				return
			case s.events <- ev:
			}
		}
	}
}

// attemptRecreate returns (justRecreated, cont). cont is false only
// when ctx cancelled during backoff so the caller exits the loop.
func (s *Stream) attemptRecreate(ctx context.Context, failures *int, backoff *time.Duration) (justRecreated, cont bool) {
	ref, err := createPullPoint(s.caller, s.opts)
	if err != nil {
		s.surfaceError(ErrRecreateFailed{Err: err})
		if !sleepCtx(ctx, jitter(*backoff)) {
			return false, false
		}
		*backoff *= 2
		if *backoff > maxRecreateBackoff {
			*backoff = maxRecreateBackoff
		}
		return false, true
	}
	s.setPullPoint(ref)
	*failures = 0
	*backoff = s.opts.RetryBackoff
	return true, true
}

// jitter perturbs d by ±jitterFraction so synchronised drops do not
// produce a synchronised reconnect surge.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Nanosecond
	}
	spread := float64(d) * jitterFraction
	delta := (rand.Float64()*2 - 1) * spread
	out := time.Duration(float64(d) + delta)
	if out <= 0 {
		out = time.Nanosecond
	}
	return out
}
