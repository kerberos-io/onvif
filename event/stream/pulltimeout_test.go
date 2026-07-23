package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateClientTimeout — PullMessages is a long-poll: the camera
// holds the connection open for PullTimeout waiting for an event. An
// http.Client.Timeout covers the whole exchange and starts before the
// camera has even parsed the request, so a client ceiling at or below
// PullTimeout loses the race on every quiet interval and the pull can
// only ever fail. This shipped once (both were 5s) and presented as a
// slow camera rather than a misconfiguration.
func TestValidateClientTimeout(t *testing.T) {
	tests := []struct {
		name    string
		client  time.Duration
		pull    time.Duration
		wantErr bool
	}{
		{"unbounded client is fine", 0, 30 * time.Second, false},
		{"comfortable headroom", 40 * time.Second, 30 * time.Second, false},
		{"strictly greater is accepted", 30*time.Second + time.Millisecond, 30 * time.Second, false},
		{"equal timeouts always lose", 5 * time.Second, 5 * time.Second, true},
		{"client below pull", 4 * time.Second, 30 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClientTimeout(tt.client, tt.pull)
			if tt.wantErr {
				require.Error(t, err, "client=%s pull=%s must be rejected", tt.client, tt.pull)
				assert.Contains(t, err.Error(), "PullTimeout",
					"the error must name the option the caller has to change")
				return
			}
			assert.NoError(t, err, "client=%s pull=%s must be accepted", tt.client, tt.pull)
		})
	}
}
