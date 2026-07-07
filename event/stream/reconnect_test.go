package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPullPointRespAlt mirrors the first fixture but returns a
// different SubscriptionReference Address so a test can prove that
// subsequent pulls hit the recreated endpoint. Like createPullPointResp,
// it intentionally omits <TerminationTime> so renew scheduling stays
// driven by opts.
const createPullPointRespAlt = `<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body>
    <tev:CreatePullPointSubscriptionResponse>
      <tev:SubscriptionReference>
        <wsa:Address>http://camera.local/onvif/Events/PullSub_2</wsa:Address>
      </tev:SubscriptionReference>
    </tev:CreatePullPointSubscriptionResponse>
  </env:Body>
</env:Envelope>`

// --- Recreate after pull failures ------------------------------------

func TestStream_RecreatesSubscriptionAfterRepeatedPullErrors(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueCallMethod(createPullPointRespAlt, nil)

	fc.queueSendSoap("", errors.New("transient failure"))
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		DeviceID:               "cam-1",
		PullTimeout:            50 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	ev := receive(t, s.Events(), 2*time.Second)
	assert.Equal(t, KindMotion, ev.Kind)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.callMethodCalls, 2,
		"expected exactly 2 CallMethod calls (initial + recreate)")
	var newEndpointPulls int
	for _, c := range fc.sendSoapCalls {
		if c[0] == "http://camera.local/onvif/Events/PullSub_2" {
			newEndpointPulls++
		}
	}
	assert.GreaterOrEqual(t, newEndpointPulls, 1,
		"expected pulls against the recreated subscription endpoint")
}

func TestStream_BackoffWhenRecreateFails(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.mu.Lock()
	fc.defaultCall = fakeResp{err: errors.New("recreate fail")}
	fc.defaultSendSoap = fakeResp{err: errors.New("pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            10 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(2 * time.Second)
	var calls atomic.Int32
	for time.Now().Before(deadline) {
		fc.mu.Lock()
		calls.Store(int32(len(fc.callMethodCalls)))
		fc.mu.Unlock()
		if calls.Load() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, calls.Load(), int32(4),
		"expected stream to retry recreate (>=3 retries on top of the initial create)")
}

func TestStream_ReconnectAfterFailuresDefault(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 3, o.ReconnectAfterFailures)
}

func TestStream_RetryBackoffDefault(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, time.Second, o.RetryBackoff)
}

func TestStream_DisableReconnectKeepsRetryingOriginalEndpoint(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:        10 * time.Millisecond,
		RetryBackoff:       10 * time.Millisecond,
		InitialTermination: 30 * time.Second,
		DisableReconnect:   true,
	})
	require.NoError(t, err)
	defer s.Close()

	time.Sleep(200 * time.Millisecond)
	fc.mu.Lock()
	calls := len(fc.callMethodCalls)
	fc.mu.Unlock()
	assert.Equal(t, 1, calls, "DisableReconnect must prevent recreate; got %d CallMethod calls", calls)
}

func TestStream_RecreateResetsFailuresAndBackoffOnSuccess(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueCallMethod(createPullPointRespAlt, nil)
	fc.queueSendSoap("", errors.New("first failure"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            10 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	time.Sleep(200 * time.Millisecond)
	fc.mu.Lock()
	calls := len(fc.callMethodCalls)
	fc.mu.Unlock()
	assert.Equal(t, 2, calls,
		"after one failure + successful recreate, no further recreates expected; got %d", calls)
}

func TestStream_PullPointMutationVisibleToRenewLoopUnderRace(t *testing.T) {
	// Drives the pullPoint write-by-pullLoop / read-by-renewLoop race
	// so -race actually exercises the mutex critical sections.
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	for i := 0; i < 50; i++ {
		fc.queueCallMethod(createPullPointRespAlt, nil)
	}
	fc.mu.Lock()
	fc.defaultSendSoap = fakeResp{err: errors.New("recurring pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            5 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           1 * time.Millisecond,
		InitialTermination:     20 * time.Millisecond,
		RenewMargin:            2 * time.Millisecond,
	})
	require.NoError(t, err)
	defer s.Close()

	time.Sleep(300 * time.Millisecond)
}

// --- Typed errors from the reconnect path ----------------------------

func TestStream_PullErrorIsTypedErrPullFailed(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueSendSoap("", errors.New("transient"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:        50 * time.Millisecond,
		RetryBackoff:       10 * time.Millisecond,
		InitialTermination: 30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case e := <-s.Errors():
		var pullErr ErrPullFailed
		require.True(t, errors.As(e, &pullErr), "expected ErrPullFailed, got %T: %v", e, e)
		assert.Contains(t, pullErr.Err.Error(), "transient")
	case <-time.After(time.Second):
		t.Fatal("expected ErrPullFailed on Errors channel")
	}
}

func TestStream_RecreateErrorIsTypedErrRecreateFailed(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.mu.Lock()
	fc.defaultCall = fakeResp{err: errors.New("recreate fail")}
	fc.defaultSendSoap = fakeResp{err: errors.New("pull fail")}
	fc.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            10 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	deadline := time.Now().Add(time.Second)
	var sawRecreate bool
	for time.Now().Before(deadline) && !sawRecreate {
		select {
		case e := <-s.Errors():
			var rec ErrRecreateFailed
			if errors.As(e, &rec) {
				sawRecreate = true
				assert.Contains(t, rec.Err.Error(), "recreate fail")
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	assert.True(t, sawRecreate, "expected at least one ErrRecreateFailed on Errors")
}

func TestStream_AfterReconnectFlagSetOnFirstPostRecreateBatch(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	fc.queueCallMethod(createPullPointRespAlt, nil)

	fc.queueSendSoap("", errors.New("transient"))
	fc.queueSendSoap(pullMessagesResp(motionMsg("true")), nil)
	fc.queueSendSoap(pullMessagesResp(motionMsg("false")), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newStream(ctx, fc, Options{
		PullTimeout:            50 * time.Millisecond,
		ReconnectAfterFailures: 1,
		RetryBackoff:           10 * time.Millisecond,
		InitialTermination:     30 * time.Second,
	})
	require.NoError(t, err)
	defer s.Close()

	ev1 := receive(t, s.Events(), 2*time.Second)
	assert.True(t, ev1.AfterReconnect, "first event after recreate must carry AfterReconnect=true")
	assert.Equal(t, StateActive, ev1.State)

	ev2 := receive(t, s.Events(), 2*time.Second)
	assert.False(t, ev2.AfterReconnect, "subsequent events should not carry AfterReconnect")
	assert.Equal(t, StateInactive, ev2.State)
}

// --- Jitter ----------------------------------------------------------

func TestJitter_StaysWithinFraction(t *testing.T) {
	const base = time.Second
	low := time.Duration(float64(base) * (1 - jitterFraction))
	high := time.Duration(float64(base) * (1 + jitterFraction))
	for i := 0; i < 200; i++ {
		got := jitter(base)
		assert.GreaterOrEqual(t, got, low, "iteration %d", i)
		assert.LessOrEqual(t, got, high, "iteration %d", i)
	}
}

func TestJitter_ZeroAndNegativeReturnPositive(t *testing.T) {
	assert.Greater(t, jitter(0), time.Duration(0))
	assert.Greater(t, jitter(-time.Second), time.Duration(0))
}

func TestJitter_VariesAcrossCalls(t *testing.T) {
	first := jitter(time.Second)
	allEqual := true
	for i := 0; i < 10; i++ {
		if jitter(time.Second) != first {
			allEqual = false
			break
		}
	}
	assert.False(t, allEqual, "jitter is producing a constant; rand seed not working")
}

func TestMaxRecreateBackoff_Is5Minutes(t *testing.T) {
	assert.Equal(t, 5*time.Minute, maxRecreateBackoff)
}
