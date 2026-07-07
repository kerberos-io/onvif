package stream

import (
	"testing"
	"time"
)

// waitFor polls cond at 10ms intervals up to d. Fails the test with msg
// if cond never returns true. Centralises the pattern that appears in
// renew/reconnect/stream tests so retries are uniform.
func waitFor(t *testing.T, d time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waitFor timed out after %s: %s", d, msg)
}
