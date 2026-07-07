package stream

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countSendSoapMatching counts how many recorded SendSoap calls have a
// body containing needle. Safe to call concurrently with the run loop.
func countSendSoapMatching(fc *fakeCaller, needle string) int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	n := 0
	for _, c := range fc.sendSoapCalls {
		if strings.Contains(c[1], needle) {
			n++
		}
	}
	return n
}

func TestStream_RenewsSubscriptionBeforeExpiry(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 100 ms termination with 10 ms margin -> renew every ~90 ms.
	s, err := newStream(ctx, fc, Options{
		DeviceID:           "cam-1",
		InitialTermination: 100 * time.Millisecond,
		RenewMargin:        10 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	var renewCount int
	for time.Now().Before(deadline) {
		renewCount = countSendSoapMatching(fc, "Renew")
		if renewCount >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, renewCount, 1, "expected at least one Renew SendSoap call within 500ms")
}

func TestStream_RenewSendsToSubscriptionEndpoint(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := newStream(ctx, fc, Options{
		InitialTermination: 80 * time.Millisecond,
		RenewMargin:        10 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if countSendSoapMatching(fc, "Renew") >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	var renewEndpoint string
	for _, c := range fc.sendSoapCalls {
		if strings.Contains(c[1], "Renew") {
			renewEndpoint = c[0]
			break
		}
	}
	require.NotEmpty(t, renewEndpoint, "no Renew call found")
	assert.Equal(t, "http://camera.local/onvif/Events/PullSub_1", renewEndpoint,
		"Renew must target the SubscriptionReference Address")
}

func TestStream_RenewMarginAppliesDefault(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 10*time.Second, o.RenewMargin)
}

func TestStream_RenewErrorSurfacedOnErrorsChannel(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	// Defaults return empty pulls indefinitely so the pull loop is clean.
	// Override defaultSendSoap on the fly to return a Renew error for
	// any body that looks like a Renew. We do that by tagging the
	// default response with an err, then resetting after capturing one.
	// Simpler: just queue several explicit Renew-error responses; the
	// fake's queue is consumed in FIFO and the pull body never matches
	// 'Renew', so queued errors will land on the renew call only if
	// queued before any pulls. To bias the order we drain via a custom
	// default.
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errInjected{}}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := newStream(ctx, fc, Options{
		InitialTermination: 80 * time.Millisecond,
		RenewMargin:        10 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case e := <-s.Errors():
		assert.Contains(t, e.Error(), "injected")
	case <-time.After(time.Second):
		t.Fatal("expected an error on Errors channel from failing Renew/pull")
	}
}

// errInjected is a sentinel error type so the test message has a stable
// substring without depending on a wrapped string match.
type errInjected struct{}

func (errInjected) Error() string { return "injected fake error" }

func TestRenew_SendsAbsoluteDateTimeNotDuration(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		InitialTermination: 30 * time.Millisecond,
		RenewMargin:        5 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if countSendSoapMatching(fc, "Renew") >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	var renewBody string
	for _, c := range fc.sendSoapCalls {
		if strings.Contains(c[1], "Renew") {
			renewBody = c[1]
			break
		}
	}
	require.NotEmpty(t, renewBody, "no Renew call observed")
	// Absolute form is "YYYY-MM-DDTHH:MM:SSZ" not "PTnS".
	assert.NotContains(t, renewBody, "PT", "Renew should not send relative duration; some firmwares reject it")
	assert.Regexp(t, `\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`, renewBody, "Renew should send absolute RFC3339 UTC")
}

// --- Wiring: renew surfaces SOAP fault detail -------------------------

func TestRenewPullPoint_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap(renewFaultBody, errors.New("400 Bad Request"))
	_, err := renewPullPoint(fc, subscriptionRef{Address: "http://camera/sub"}, defaultOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "renew-specific complaint",
		"renewPullPoint must enrich transport errors with the camera's SOAP fault")
}

const renewFaultBody = `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code><env:Value>env:Sender</env:Value></env:Code>
    <env:Reason><env:Text xml:lang="en">renew-specific complaint</env:Text></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`

func TestRenewPullPoint_EchoesRefParamsWithIsReferenceParameter(t *testing.T) {
	ref := subscriptionRef{
		Address:      "http://192.168.1.10/onvif/services",
		RefParamsXML: `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing"><dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId></wsa:ReferenceParameters>`,
	}
	fc := newFakeCaller()
	_, err := renewPullPoint(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Contains(t, hdr, "SubscriptionId")
	assert.Contains(t, hdr, "297")
	assert.Contains(t, hdr, `IsReferenceParameter="true"`)
}

// Regression: when GrantedTermination has just passed (renew failed
// at or after the deadline), nextRenewInterval floors to one second
// and the loop hammers the camera at 1 Hz until reconnect. Original
// ticker design retried at the configured cadence regardless. After
// a failure the loop must use a backoff decoupled from the stale
// grant.
func TestNextRenewIntervalAfterError_BacksOffAtLeastRetryBackoff(t *testing.T) {
	opts := Options{RetryBackoff: time.Second}
	got := nextRenewIntervalAfterError(opts)
	assert.GreaterOrEqual(t, got, opts.RetryBackoff,
		"failure path must back off at least RetryBackoff, not 1s floor on stale grant")
}

func TestNextRenewIntervalAfterError_DefaultsToOneSecondWhenRetryBackoffZero(t *testing.T) {
	got := nextRenewIntervalAfterError(Options{})
	assert.Equal(t, time.Second, got,
		"zero RetryBackoff must yield the safety floor, not a tight 0-duration sleep")
}

// Lost-update race: renew snapshots the ref, the SOAP call returns,
// and meanwhile attemptRecreate replaced pullPoint with a fresh
// subscription. If renew blindly writes the OLD subscription's
// granted time onto the NEW subscription, the new schedule is wrong.
// Update must be conditioned on "same subscription as when I read."
func TestUpdateGrantedTermination_DropsWriteWhenSubscriptionRotated(t *testing.T) {
	// s.now is intentionally nil — this test does not exercise any
	// time-dependent path; only the gen-counter accessors.
	s := &Stream{}
	original := subscriptionRef{Address: "http://camera/sub-A"}
	s.setPullPoint(original)
	gen := s.pullPointGen()

	// Simulate recreate happening between snapshot and write.
	s.setPullPoint(subscriptionRef{Address: "http://camera/sub-B"})

	// Old generation's renew result must NOT overwrite sub-B's grant.
	bogus := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	s.updateGrantedTerminationIfGen(gen, bogus)

	assert.True(t, s.getPullPoint().GrantedTermination.IsZero(),
		"stale renew result must be discarded after a subscription rotation")
}

func TestUpdateGrantedTermination_AppliesWhenGenMatches(t *testing.T) {
	// s.now is intentionally nil — gen-counter path only.
	s := &Stream{}
	s.setPullPoint(subscriptionRef{Address: "http://camera/sub"})
	gen := s.pullPointGen()
	t1 := time.Date(2026, 5, 27, 13, 0, 0, 0, time.UTC)
	s.updateGrantedTerminationIfGen(gen, t1)
	assert.Equal(t, t1, s.getPullPoint().GrantedTermination)
}

// renewLoop must capture (ref, gen) atomically. Two separate
// getPullPoint() + pullPointGen() reads leave a window in which a
// concurrent setPullPoint advances gen between the two reads — the
// renew then runs against ref-N but believes its captured gen is N+1,
// and updateGrantedTerminationIfGen("succeeds") writing the old
// subscription's grant onto the new one. Single snapshot closes it.
func TestSnapshotPullPoint_AtomicReadOfRefAndGen(t *testing.T) {
	// s.now is intentionally nil — gen-counter path only.
	s := &Stream{}
	s.setPullPoint(subscriptionRef{Address: "A"}) // gen=1
	ref, gen := s.snapshotPullPoint()
	assert.Equal(t, "A", ref.Address)
	assert.Equal(t, uint64(1), gen)

	s.setPullPoint(subscriptionRef{Address: "B"}) // gen=2
	ref, gen = s.snapshotPullPoint()
	assert.Equal(t, "B", ref.Address)
	assert.Equal(t, uint64(2), gen)
}
