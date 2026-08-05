package stream

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateClientTimeout — PullMessages is a long-poll: the camera
// holds the connection open for PullTimeout waiting for an event. An
// http.Client.Timeout covers the whole exchange and starts before the
// camera has parsed the request, so a client ceiling at or below
// PullTimeout loses the race on every quiet interval and the pull can
// only ever fail. This shipped once (both were 5s) and presented as a
// slow camera rather than a misconfiguration.
//
// Strict inequality is not enough: the client also has to cover dial,
// TLS and the response transfer, which on a cellular bearer runs to
// hundreds of milliseconds. Hence a real headroom floor.
func TestValidateClientTimeout(t *testing.T) {
	tests := []struct {
		name    string
		client  time.Duration
		pull    time.Duration
		wantErr bool
	}{
		{"unbounded client is the caller's risk, not an error", 0, 30 * time.Second, false},
		{"comfortable headroom", 40 * time.Second, 30 * time.Second, false},
		{"exactly the minimum headroom", 30*time.Second + minClientHeadroom, 30 * time.Second, false},
		{"a hair under the minimum headroom", 30*time.Second + minClientHeadroom - time.Millisecond, 30 * time.Second, true},
		{"strictly greater but no headroom", 30*time.Second + time.Millisecond, 30 * time.Second, true},
		{"equal timeouts always lose", 5 * time.Second, 5 * time.Second, true},
		{"client below pull", 4 * time.Second, 30 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClientTimeout(tt.client, tt.pull)
			if tt.wantErr {
				require.Error(t, err, "client=%s pull=%s must be rejected", tt.client, tt.pull)
				assert.ErrorIs(t, err, ErrInvalidOptions,
					"callers need a sentinel to tell a permanent misconfiguration from a transient failure")
				assert.Contains(t, err.Error(), "PullTimeout",
					"the error must name the option the caller has to change")
				return
			}
			assert.NoError(t, err, "client=%s pull=%s must be accepted", tt.client, tt.pull)
		})
	}
}

// TestErrInvalidOptions_IsDistinctFromStreamErrors — the pull/renew/
// recreate errors are transient and callers retry them. A bad Options
// never becomes valid by retrying, so it must not be mistaken for one.
func TestErrInvalidOptions_IsDistinctFromStreamErrors(t *testing.T) {
	err := validateClientTimeout(5*time.Second, 5*time.Second)
	require.Error(t, err)

	var pull ErrPullFailed
	var renew ErrRenewFailed
	var recreate ErrRecreateFailed
	assert.False(t, errors.As(err, &pull))
	assert.False(t, errors.As(err, &renew))
	assert.False(t, errors.As(err, &recreate))
}
