package stream

import (
	"testing"

	"go.uber.org/goleak"
)

// Catches any pull, renew, or recreate goroutine that outlives its
// Stream — a regression class that's silent in production until
// goroutine count drifts up and triggers OOM.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
