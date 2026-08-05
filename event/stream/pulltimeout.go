package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/kerberos-io/onvif"
)

// ErrInvalidOptions marks a configuration that cannot succeed. Callers
// retry the pull/renew/recreate errors; retrying this one never helps,
// so it is a distinct sentinel they can short-circuit on.
var ErrInvalidOptions = errors.New("stream: invalid options")

// minClientHeadroom is how far http.Client.Timeout must exceed
// PullTimeout. The client ceiling covers dial, TLS and the response
// transfer on top of the poll it has to outlast, and starts before the
// camera has parsed the request; on a cellular bearer that overhead
// runs to hundreds of milliseconds.
const minClientHeadroom = 5 * time.Second

// validateClientTimeout rejects a client ceiling that cannot outlast
// the PullMessages long-poll plus minClientHeadroom.
//
// Zero means unbounded and is accepted: it is the SDK's default when a
// caller passes no client, so rejecting it would break every default
// consumer. Note it is not risk-free — the caller interface documents
// that ctx cannot interrupt an in-flight SOAP call, so only the client
// timeout can unwedge a stalled camera.
func validateClientTimeout(clientTimeout, pullTimeout time.Duration) error {
	if clientTimeout == 0 || clientTimeout >= pullTimeout+minClientHeadroom {
		return nil
	}
	return fmt.Errorf(
		"%w: http.Client.Timeout (%s) must exceed PullTimeout (%s) by at least %s; PullMessages is a long-poll and the client would abort every quiet pull",
		ErrInvalidOptions, clientTimeout, pullTimeout, minClientHeadroom)
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
