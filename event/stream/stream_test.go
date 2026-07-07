package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fakeCaller --------------------------------------------------------

// fakeCaller is a test double for the caller interface. Each method
// returns the next queued response; when the queue is exhausted it falls
// back to a default response so the indefinite pull loop does not
// require tests to enumerate every call.
//
// blockUnsubscribe, when non-nil, causes SendSoap calls whose body
// contains "Unsubscribe" to block until the channel is closed.
// blockAllSendSoap, when non-nil, blocks every SendSoap call until
// closed (simulates a hung HTTP transport).
type fakeCaller struct {
	mu               sync.Mutex
	callMethodResps  []fakeResp
	sendSoapResps    []fakeResp
	defaultSendSoap  fakeResp
	defaultCall      fakeResp
	callMethodCalls  []any
	sendSoapCalls    [][2]string
	sendSoapHeaders  []string
	blockUnsubscribe chan struct{}
	blockAllSendSoap chan struct{}
}

type fakeResp struct {
	body string
	err  error
}

func newFakeCaller() *fakeCaller {
	return &fakeCaller{
		// Default: indefinite empty pulls, indefinite OK unsubscribes.
		defaultSendSoap: fakeResp{body: pullMessagesResp()},
		defaultCall:     fakeResp{err: errors.New("fakeCaller: no default CallMethod response")},
	}
}

func (f *fakeCaller) queueCallMethod(body string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callMethodResps = append(f.callMethodResps, fakeResp{body: body, err: err})
}

func (f *fakeCaller) queueSendSoap(body string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendSoapResps = append(f.sendSoapResps, fakeResp{body: body, err: err})
}

func (f *fakeCaller) CallMethod(m any) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callMethodCalls = append(f.callMethodCalls, m)
	r := f.defaultCall
	if len(f.callMethodResps) > 0 {
		r = f.callMethodResps[0]
		f.callMethodResps = f.callMethodResps[1:]
	}
	// Mirror networking.SendSoap*: a 4xx/5xx returns body alongside
	// err. Tests opt into that shape by queueing body + err together.
	if r.err != nil && r.body == "" {
		return nil, r.err
	}
	return &http.Response{Body: io.NopCloser(strings.NewReader(r.body))}, r.err
}

func (f *fakeCaller) SendSoap(endpoint, body string) (*http.Response, error) {
	f.mu.Lock()
	f.sendSoapCalls = append(f.sendSoapCalls, [2]string{endpoint, body})
	r := f.defaultSendSoap
	if len(f.sendSoapResps) > 0 {
		r = f.sendSoapResps[0]
		f.sendSoapResps = f.sendSoapResps[1:]
	}
	block := f.blockUnsubscribe
	blockAll := f.blockAllSendSoap
	f.mu.Unlock()

	if blockAll != nil {
		<-blockAll
	}
	if block != nil && strings.Contains(body, "Unsubscribe") {
		<-block
	}

	if r.err != nil && r.body == "" {
		return nil, r.err
	}
	return &http.Response{Body: io.NopCloser(strings.NewReader(r.body))}, r.err
}

// SendSoapWithHeader delegates body+endpoint recording to SendSoap so
// existing assertions on sendSoapCalls keep working, and records the
// header XML in a parallel slice for ref-params wiring tests.
func (f *fakeCaller) SendSoapWithHeader(endpoint, body, headerXML string) (*http.Response, error) {
	f.mu.Lock()
	f.sendSoapHeaders = append(f.sendSoapHeaders, headerXML)
	f.mu.Unlock()
	return f.SendSoap(endpoint, body)
}

func (f *fakeCaller) sendSoapCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sendSoapCalls)
}

// --- fixture SOAP envelopes -------------------------------------------

// createPullPointResp is the minimal SOAP envelope the lib's existing
// xml.Decoder + getXMLNode path can extract a pull-point address from.
// Intentionally omits <TerminationTime> so renewLoop falls back to
// opts.InitialTermination — tests that drive renew timing depend on
// that path. Tests that need the camera-granted termination capture
// path use a dedicated fixture instead.
const createPullPointResp = `<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body>
    <tev:CreatePullPointSubscriptionResponse>
      <tev:SubscriptionReference>
        <wsa:Address>http://camera.local/onvif/Events/PullSub_1</wsa:Address>
      </tev:SubscriptionReference>
    </tev:CreatePullPointSubscriptionResponse>
  </env:Body>
</env:Envelope>`

func pullMessagesResp(messages ...string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
              xmlns:tev="http://www.onvif.org/ver10/events/wsdl"
              xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2"
              xmlns:tt="http://www.onvif.org/ver10/schema">
  <env:Body>
    <tev:PullMessagesResponse>
      <tev:CurrentTime>2026-05-21T10:30:05Z</tev:CurrentTime>
      <tev:TerminationTime>2026-05-21T10:31:05Z</tev:TerminationTime>
      ` + strings.Join(messages, "\n") + `
    </tev:PullMessagesResponse>
  </env:Body>
</env:Envelope>`
}

func motionMsg(value string) string {
	return `<wsnt:NotificationMessage>
  <wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">tns1:RuleEngine/CellMotionDetector/Motion</wsnt:Topic>
  <wsnt:Message>
    <tt:Message PropertyOperation="Changed" UtcTime="2026-05-21T10:30:00Z">
      <tt:Source>
        <tt:SimpleItem Name="VideoSourceConfigurationToken" Value="VSC0"/>
      </tt:Source>
      <tt:Data>
        <tt:SimpleItem Name="IsMotion" Value="` + value + `"/>
      </tt:Data>
    </tt:Message>
  </wsnt:Message>
</wsnt:NotificationMessage>`
}

const unsubscribeResp = `<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
              xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2">
  <env:Body>
    <wsnt:UnsubscribeResponse/>
  </env:Body>
</env:Envelope>`

// --- helpers -----------------------------------------------------------

// receive waits up to d for an event on ch, failing the test if none
// arrives.
func receive(t *testing.T, ch <-chan Event, d time.Duration) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("event channel closed before receiving")
		}
		return ev
	case <-time.After(d):
		t.Fatalf("timed out waiting for event after %s", d)
	}
	return Event{} // unreachable
}

// --- tests -------------------------------------------------------------

func TestNewStream_CreatesPullPointAtConstruction(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Queue an empty pull so the run loop can spin without exploding.
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)
	require.NotNil(t, s)
	require.NoError(t, s.Close())

	// CreatePullPointSubscription was called exactly once.
	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.callMethodCalls, 1, "expected one CallMethod call (CreatePullPointSubscription)")
}

func TestStream_DeliversDecodedEvents(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)
	// Provide subsequent empty pulls so the loop doesn't starve before Close.
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)
	defer s.Close()

	ev := receive(t, s.Events(), 2*time.Second)
	assert.Equal(t, KindMotion, ev.Kind)
	assert.Equal(t, StateActive, ev.State)
	assert.Equal(t, "cam-1", ev.DeviceID)
	assert.Equal(t, "tns1:RuleEngine/CellMotionDetector/Motion", ev.Topic)
	assert.Equal(t, "VSC0", ev.Source["VideoSourceConfigurationToken"])
	assert.Equal(t, "true", ev.Data["IsMotion"])
}

func TestStream_PullsAgainstSubscriptionAddress(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)

	// Wait until at least one pull happened, then close.
	for i := 0; i < 50 && fc.sendSoapCallCount() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, s.Close())

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.NotEmpty(t, fc.sendSoapCalls, "expected at least one PullMessages SendSoap call")
	endpoint := fc.sendSoapCalls[0][0]
	assert.Equal(t, "http://camera.local/onvif/Events/PullSub_1", endpoint,
		"PullMessages must target the SubscriptionReference Address returned by CreatePullPoint")
	// Last call (Close) should target the same endpoint with an Unsubscribe body.
	last := fc.sendSoapCalls[len(fc.sendSoapCalls)-1]
	assert.Equal(t, "http://camera.local/onvif/Events/PullSub_1", last[0])
	assert.Contains(t, last[1], "Unsubscribe")
}

func TestNewStream_ReturnsErrorWhenCreatePullPointFails(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod("", errors.New("network down"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestStream_ClosedContextStopsRunLoop(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Many empty pulls so the loop is hot when we cancel.
	for i := 0; i < 20; i++ {
		fc.queueSendSoap(pullMessagesResp(), nil)
	}
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)

	// Wait for at least one pull.
	for i := 0; i < 50 && fc.sendSoapCallCount() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	// Close should still complete cleanly; the goroutine must drain.
	require.NoError(t, s.Close())

	// Events channel must close so consumers can range-loop safely.
	select {
	case _, ok := <-s.Events():
		assert.False(t, ok, "Events channel should be closed after Close()")
	case <-time.After(time.Second):
		t.Fatal("Events channel was not closed within 1s")
	}
}

func TestStream_PullErrorSurfacedOnErrorsChannel(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueSendSoap("", errors.New("transient pull failure"))
	// Then a clean pull so the loop keeps running.
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)
	fc.queueSendSoap(pullMessagesResp(), nil)
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)
	defer s.Close()

	select {
	case e := <-s.Errors():
		assert.Contains(t, e.Error(), "transient pull failure")
	case <-time.After(2 * time.Second):
		t.Fatal("expected an error on the Errors channel")
	}
	// After the transient failure the loop continued and decoded.
	ev := receive(t, s.Events(), 2*time.Second)
	assert.Equal(t, KindMotion, ev.Kind)
}

func TestStream_OptionsApplyDefaults(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 5*time.Second, o.PullTimeout)
	assert.Equal(t, 32, o.MessageLimit)
	assert.Equal(t, 60*time.Second, o.InitialTermination)
	assert.Equal(t, 16, o.BufferSize)
}

func TestStream_DoesNotPanicOnPullExitingDuringClose(t *testing.T) {
	// Regression guard: Close should not race with the run goroutine
	// in a way that double-closes the events/errors channels.
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	for i := 0; i < 5; i++ {
		fc.queueSendSoap(pullMessagesResp(), nil)
	}
	fc.queueSendSoap(unsubscribeResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{DeviceID: "cam-1"})
	require.NoError(t, err)
	assert.NotPanics(t, func() {
		require.NoError(t, s.Close())
		// Double-close should be a no-op, not a panic.
		_ = s.Close()
	})
}

// --- Close error / timeout paths -------------------------------------

func TestClose_ReturnsUnsubscribeError(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("simulated transport failure")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	require.NoError(t, err)

	err = s.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsubscribe pull point")
	assert.Contains(t, err.Error(), "simulated transport failure")
}

func TestClose_BoundedByTimeoutOnHungUnsubscribe(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	block := make(chan struct{})
	defer close(block) // release the hung Unsubscribe so the fake's goroutine exits
	fc.mu.Lock()
	fc.blockUnsubscribe = block
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	require.NoError(t, err)

	start := time.Now()
	err = s.Close()
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Less(t, elapsed, closeUnsubscribeTimeout+time.Second,
		"Close exceeded bound (%s); expected ~%s", elapsed, closeUnsubscribeTimeout)
}

// --- NewStream edge cases --------------------------------------------

func TestNewStream_CtxAlreadyCancelled(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before NewStream

	s, err := newStream(ctx, fc, Options{InitialTermination: 30 * time.Second})
	require.NoError(t, err)
	require.NotNil(t, s)

	select {
	case _, ok := <-s.Events():
		assert.False(t, ok, "events channel should be closed when ctx is pre-cancelled")
	case <-time.After(time.Second):
		t.Fatal("events channel was not closed within 1s")
	}
	_ = s.Close()
}

// --- fakeCaller self-test --------------------------------------------

func TestFakeCaller_QueueThenDefaultFallback(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap("first", nil)
	fc.queueSendSoap("second", nil)

	r1, err := fc.SendSoap("ep", "body")
	require.NoError(t, err)
	b1 := make([]byte, 10)
	n, _ := r1.Body.Read(b1)
	assert.Equal(t, "first", string(b1[:n]))

	r2, _ := fc.SendSoap("ep", "body")
	b2 := make([]byte, 10)
	n, _ = r2.Body.Read(b2)
	assert.Equal(t, "second", string(b2[:n]))

	// Queue is exhausted; default kicks in.
	r3, err := fc.SendSoap("ep", "body")
	require.NoError(t, err)
	require.NotNil(t, r3)
	b3 := make([]byte, 2048)
	n, _ = r3.Body.Read(b3)
	assert.Contains(t, string(b3[:n]), "PullMessagesResponse",
		"default SendSoap should be an empty PullMessagesResponse envelope")
}

func TestClose_BoundedWhenLoopsStuckOnHungHTTP(t *testing.T) {
	// Simulates a hung HTTP transport: every SendSoap blocks
	// indefinitely. The pull and renew loops are wedged inside
	// SendSoap and ctx-cancel cannot unblock them. Close must still
	// return within its bounded budget so the agent's shutdown does
	// not hang.
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	blockAll := make(chan struct{})
	defer close(blockAll)
	fc.mu.Lock()
	fc.blockAllSendSoap = blockAll
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:        100 * time.Millisecond,
		InitialTermination: 30 * time.Second,
	})
	require.NoError(t, err)

	// Wait until pullLoop is actually parked inside the blocked
	// SendSoap. Without this, Close races with the loop's first
	// iteration and exits via the ctx pre-check instead of
	// exercising the drain-timeout path.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && fc.sendSoapCallCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, fc.sendSoapCallCount(), 1, "pullLoop never reached SendSoap")

	start := time.Now()
	err = s.Close()
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drain", "expected a drain-timeout error")
	// Total budget is closeDrainTimeout for the wait + ~0 for unsubscribe
	// (which is skipped when drain times out). Give plenty of slack for
	// scheduling on a loaded CI machine.
	assert.Less(t, elapsed, closeDrainTimeout+2*time.Second,
		"Close exceeded bound (%s); expected ~%s", elapsed, closeDrainTimeout)
}
