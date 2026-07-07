package stream

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kerberos-io/onvif"
)

// closeDrainTimeout bounds Close's wait for the pull and renew
// goroutines to exit. The loops block in caller.SendSoap which is not
// ctx-aware (the underlying http.Client is the only thing that can
// unblock them — see caller below). On a hung HTTP transport Close
// would otherwise wait forever; instead it returns an error and lets
// the calling agent move on.
const closeDrainTimeout = 5 * time.Second

// closeUnsubscribeTimeout bounds the Unsubscribe SOAP call issued by
// Close. A subscription expires at the camera once InitialTermination
// elapses without a renew, so a missed unsubscribe is at worst
// cosmetic.
const closeUnsubscribeTimeout = 5 * time.Second

// Options configures a Stream.
//
// Zero-value policy: every duration / int field treats zero as "use
// the default". To opt out of reconnect set DisableReconnect=true
// (ReconnectAfterFailures=0 would otherwise collide with the default
// injection). For unbuffered Events / Errors channels set
// BufferSize=-1.
type Options struct {
	DeviceID string
	// RawTopicFilter is the ONVIF ConcreteSet TopicExpression filter
	// passed verbatim to CreatePullPointSubscription. Callers should
	// normally leave this empty and rely on Classify for routing —
	// server-side filtering is fragile across vendors and empty is
	// required for AXIS.
	RawTopicFilter string
	// PullTimeout — zero means default (5s).
	PullTimeout time.Duration
	// MessageLimit — zero means default (32). Busy AXIS cameras with
	// many configured rules can burst beyond 10 per pull.
	MessageLimit int
	// InitialTermination — zero means default (60s).
	InitialTermination time.Duration
	// RenewMargin — larger margins tolerate slower networks at the
	// cost of more renew calls. Zero means default (10s).
	RenewMargin time.Duration
	// ReconnectAfterFailures — pull-points die for many reasons
	// (camera reboot, subscription GC after a renew miss, NAT
	// timeout); rebuilding the subscription is the only reliable
	// recovery. Zero means default (3). Set DisableReconnect=true
	// to disable.
	ReconnectAfterFailures int
	// DisableReconnect makes the pull loop retry against the
	// original endpoint until ctx is cancelled.
	DisableReconnect bool
	// RetryBackoff is the base sleep between pull/recreate failures.
	// Recreate failures double this up to maxRecreateBackoff. Zero
	// means default (1s).
	RetryBackoff time.Duration
	// BufferSize — zero means default (16); use -1 for unbuffered.
	BufferSize int
}

func defaultOptions() Options {
	return Options{
		PullTimeout:            5 * time.Second,
		MessageLimit:           32,
		InitialTermination:     60 * time.Second,
		RenewMargin:            10 * time.Second,
		ReconnectAfterFailures: 3,
		RetryBackoff:           time.Second,
		BufferSize:             16,
	}
}

func (o Options) withDefaults() Options {
	d := defaultOptions()
	if o.PullTimeout > 0 {
		d.PullTimeout = o.PullTimeout
	}
	if o.MessageLimit > 0 {
		d.MessageLimit = o.MessageLimit
	}
	if o.InitialTermination > 0 {
		d.InitialTermination = o.InitialTermination
	}
	if o.RenewMargin > 0 {
		d.RenewMargin = o.RenewMargin
	}
	if o.ReconnectAfterFailures > 0 {
		d.ReconnectAfterFailures = o.ReconnectAfterFailures
	}
	if o.RetryBackoff > 0 {
		d.RetryBackoff = o.RetryBackoff
	}
	switch {
	case o.BufferSize > 0:
		d.BufferSize = o.BufferSize
	case o.BufferSize < 0:
		d.BufferSize = 0
	}
	d.DeviceID = o.DeviceID
	d.RawTopicFilter = o.RawTopicFilter
	d.DisableReconnect = o.DisableReconnect
	return d
}

// subscriptionRef holds the result of CreatePullPointSubscription.
// AXIS encodes the subscription identity in RefParamsXML (a generic
// /onvif/services Address plus a <wsa:ReferenceParameters> child);
// other vendors put the identity in the Address itself, leaving
// RefParamsXML empty. Subscription-scoped requests must echo a
// non-empty RefParamsXML — see extractReferenceParameters.
//
// GrantedTermination is the absolute time the camera says the
// subscription will expire if not renewed. May be less than
// requested; renewLoop schedules from this rather than opts.
type subscriptionRef struct {
	Address            string
	RefParamsXML       string
	GrantedTermination time.Time
}

// caller is the *onvif.Device subset Stream depends on. Implementations
// must:
//
//   - Be safe for concurrent use — pull and renew goroutines call in
//     from separate goroutines. *onvif.Device satisfies this via
//     http.Client.
//   - Enforce a per-request timeout via the underlying HTTP client.
//     The methods do not take a ctx, so ctx-cancel cannot interrupt a
//     hung request; only the HTTP client's own timeout can. Close
//     bounds its drain wait at closeDrainTimeout to survive a misbehaving
//     caller, but a leaking goroutine remains until the HTTP call
//     eventually returns.
type caller interface {
	CallMethod(method any) (*http.Response, error)
	SendSoap(endpoint, body string) (*http.Response, error)
	SendSoapWithHeader(endpoint, body, headerXML string) (*http.Response, error)
}

type deviceCaller struct{ dev *onvif.Device }

func (d deviceCaller) CallMethod(m any) (*http.Response, error) {
	return d.dev.CallMethod(m)
}

func (d deviceCaller) SendSoap(endpoint, body string) (*http.Response, error) {
	return d.dev.SendSoap(endpoint, body)
}

func (d deviceCaller) SendSoapWithHeader(endpoint, body, headerXML string) (*http.Response, error) {
	return d.dev.SendSoapWithHeader(endpoint, body, headerXML)
}

// Stream owns a single ONVIF pull-point subscription. Safe for Close
// from any goroutine while readers consume Events / Errors. Close is
// idempotent.
type Stream struct {
	caller caller
	opts   Options

	pullPointMu sync.Mutex // guards pullPoint and gen
	pullPoint   subscriptionRef
	gen         uint64 // bumped on every setPullPoint so renews detect mid-flight recreate

	events chan Event
	errors chan error

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error

	// now is overridable so tests can make timestamps deterministic.
	now func() time.Time
}

func (s *Stream) getPullPoint() subscriptionRef {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	return s.pullPoint
}

func (s *Stream) setPullPoint(ref subscriptionRef) {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	s.pullPoint = ref
	s.gen++
}

// pullPointGen returns the current generation. Pair with
// updateGrantedTerminationIfGen so a renew result issued against a
// subscription that was rotated mid-flight (recreate path) is
// discarded instead of overwriting the new subscription's grant.
func (s *Stream) pullPointGen() uint64 {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	return s.gen
}

// snapshotPullPoint reads ref + gen under one lock so a concurrent
// setPullPoint can't slip in between two separate accessor calls and
// leave the caller with mismatched halves.
func (s *Stream) snapshotPullPoint() (subscriptionRef, uint64) {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	return s.pullPoint, s.gen
}

// updateGrantedTerminationIfGen writes the granted time only when the
// caller's snapshot is still current. Use setPullPoint to replace the
// full ref; this updates GrantedTermination in place after a renew
// response. Intentionally does not bump gen — that would defeat the
// rotation-detection it implements.
func (s *Stream) updateGrantedTerminationIfGen(gen uint64, t time.Time) {
	s.pullPointMu.Lock()
	defer s.pullPointMu.Unlock()
	if s.gen != gen {
		return
	}
	s.pullPoint.GrantedTermination = t
}

// NewStream creates a Stream and performs CreatePullPointSubscription
// synchronously so connectivity and authentication failures surface
// from NewStream rather than landing on Errors later.
//
// The returned Stream stops when ctx is cancelled or Close is called.
func NewStream(ctx context.Context, dev *onvif.Device, opts Options) (*Stream, error) {
	return newStream(ctx, deviceCaller{dev: dev}, opts)
}

func newStream(ctx context.Context, c caller, opts Options) (*Stream, error) {
	opts = opts.withDefaults()
	ref, err := createPullPoint(c, opts)
	if err != nil {
		return nil, fmt.Errorf("create pull point subscription: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &Stream{
		caller:    c,
		opts:      opts,
		pullPoint: ref,
		events:    make(chan Event, opts.BufferSize),
		errors:    make(chan error, opts.BufferSize),
		cancel:    cancel,
		done:      make(chan struct{}),
		now:       time.Now,
	}
	go s.run(runCtx)
	return s, nil
}

// Events returns the channel of decoded notifications. Closed when
// the Stream stops.
func (s *Stream) Events() <-chan Event { return s.events }

// Errors returns the channel of non-fatal errors. Sends are
// non-blocking; consumers that fall behind drop older errors. Closed
// when the Stream stops.
func (s *Stream) Errors() <-chan error { return s.errors }

// Close stops the background goroutines, waits up to closeDrainTimeout
// for them to exit, and then Unsubscribes from the camera (also bounded,
// by closeUnsubscribeTimeout). Subsequent calls are no-ops.
//
// If the drain times out the goroutines are likely wedged inside a
// non-ctx-aware caller.SendSoap; they will exit on their own once the
// HTTP call returns. Unsubscribe is skipped in that case — the
// subscription expires at the camera anyway.
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()

		select {
		case <-s.done:
		case <-time.After(closeDrainTimeout):
			s.closeErr = fmt.Errorf("close: pull/renew loops did not drain within %s (likely stuck in caller HTTP)", closeDrainTimeout)
			return
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- unsubscribePullPoint(s.caller, s.getPullPoint())
		}()
		select {
		case err := <-errCh:
			if err != nil {
				s.closeErr = fmt.Errorf("unsubscribe pull point: %w", err)
			}
		case <-time.After(closeUnsubscribeTimeout):
			s.closeErr = fmt.Errorf("unsubscribe pull point: timeout after %s", closeUnsubscribeTimeout)
		}
	})
	return s.closeErr
}

func (s *Stream) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.renewLoop(ctx)
	}()
	s.pullLoop(ctx)
	wg.Wait()

	// Explicit close order after both goroutines have exited so a
	// future maintainer extending this function does not rely on
	// defer-ordering for channel-close safety.
	close(s.errors)
	close(s.events)
	close(s.done)
}

func (s *Stream) surfaceError(err error) {
	select {
	case s.errors <- err:
	default:
	}
}

// sleepCtx returns false if ctx was cancelled, true if d elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
