package stream

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// renewLoop sleeps until the next deadline (camera-granted termination
// minus RenewMargin), renews, and repeats. On failure it backs off via
// nextRenewIntervalAfterError so the loop doesn't busy-loop against
// the 1s floor when the previous grant has just expired. A permanently
// failing renew lets the subscription die at the camera; the pull
// loop's reconnect path then recreates it — recreate is the only
// reliable recovery once a subscription is GC'd.
func (s *Stream) renewLoop(ctx context.Context) {
	for {
		ref, gen := s.snapshotPullPoint()
		if !sleepCtx(ctx, nextRenewInterval(ref.GrantedTermination, s.opts, s.now())) {
			return
		}
		granted, err := renewPullPoint(s.caller, s.getPullPoint(), s.opts)
		if err != nil {
			s.surfaceError(ErrRenewFailed{Err: err})
			if !sleepCtx(ctx, nextRenewIntervalAfterError(s.opts)) {
				return
			}
			continue
		}
		if !granted.IsZero() {
			s.updateGrantedTerminationIfGen(gen, granted)
		}
	}
}

// nextRenewInterval prefers the camera-granted termination so we never
// schedule a renew past the actual expiry, with opts.InitialTermination
// as the fallback when the camera didn't supply one.
func nextRenewInterval(granted time.Time, opts Options, now time.Time) time.Duration {
	var base time.Duration
	if !granted.IsZero() {
		base = granted.Sub(now)
	} else {
		base = opts.InitialTermination
	}
	d := base - opts.RenewMargin
	if d <= 0 {
		d = base / 2
	}
	if d <= 0 {
		d = time.Second
	}
	return d
}

// nextRenewIntervalAfterError returns the post-failure sleep. The
// grant is typically already in the past by the time renew has failed
// once, so nextRenewInterval would floor to 1s and hammer the camera.
// Recovery is the pull loop's reconnect path; we just need to not
// accelerate retries past the configured RetryBackoff.
func nextRenewIntervalAfterError(opts Options) time.Duration {
	if opts.RetryBackoff > 0 {
		return opts.RetryBackoff
	}
	return time.Second
}

// renewPullPoint sends Renew with an absolute UTC TerminationTime.
// WS-BaseNotification §6.1.1 also allows xsd:duration but older
// Hikvision, some Dahua and some Bosch firmwares reject the
// relative form. Returns the camera-granted TerminationTime parsed
// from the response (zero on absence) so the caller can reschedule.
func renewPullPoint(c caller, ref subscriptionRef, opts Options) (time.Time, error) {
	absoluteEnd := time.Now().UTC().Add(opts.InitialTermination).Format("2006-01-02T15:04:05Z")
	req := event.Renew{TerminationTime: xsd.String(absoluteEnd)}
	body, err := xml.Marshal(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("marshal Renew: %w", err)
	}
	headerXML, err := buildRefParamsHeader(ref.RefParamsXML)
	if err != nil {
		return time.Time{}, fmt.Errorf("build ref params header: %w", err)
	}
	resp, err := c.SendSoapWithHeader(ref.Address, string(body), headerXML)
	if err != nil {
		return time.Time{}, enrichSOAPErr(resp, err)
	}
	respBody, err := readClose(resp)
	if err != nil {
		return time.Time{}, err
	}
	return extractTerminationTime(respBody), nil
}
