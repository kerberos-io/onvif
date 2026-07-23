package stream

import (
	"fmt"
	"time"

	"github.com/kerberos-io/onvif"
)

// validateClientTimeout rejects an HTTP client ceiling that cannot
// outlast the PullMessages long-poll.
//
// PullMessages asks the camera to hold the connection open for up to
// PullTimeout. http.Client.Timeout bounds the entire exchange — dial,
// write, and the wait for response headers — and starts before the
// camera has parsed the request, so it always expires first when the
// two are equal. The pull then fails on every interval with no event,
// and the subscription survives only by being recreated after
// ReconnectAfterFailures, which replays the camera's whole property
// state each time. A zero client timeout means unbounded, which is safe
// here because the pull loop is already bounded by ctx.
func validateClientTimeout(clientTimeout, pullTimeout time.Duration) error {
	if clientTimeout == 0 || clientTimeout > pullTimeout {
		return nil
	}
	return fmt.Errorf(
		"http.Client.Timeout (%s) must exceed PullTimeout (%s): PullMessages is a long-poll and the client would abort every quiet pull; raise the client timeout above PullTimeout or leave it zero",
		clientTimeout, pullTimeout)
}

// clientTimeoutOf reports the device's HTTP client ceiling, or 0 when
// the SDK is using its own default (unbounded) client.
func clientTimeoutOf(dev *onvif.Device) time.Duration {
	if dev == nil {
		return 0
	}
	c := dev.GetDeviceParams().HttpClient
	if c == nil {
		return 0
	}
	return c.Timeout
}
