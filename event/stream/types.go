package stream

import (
	"fmt"
	"time"
)

// Kind is the normalized category of an ONVIF event, independent of the
// camera vendor's topic naming.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindMotion
	KindTampering
	// KindImageQuality covers VideoSource imaging alarms. Kept separate
	// from KindTampering because they fire on legitimate sunset / dawn /
	// condensation transitions, not on interference.
	KindImageQuality
	KindDigitalInput
	KindDigitalOutput
	KindObjectDetected
	KindAudioAlarm
)

func (k Kind) String() string {
	switch k {
	case KindUnknown:
		return "Unknown"
	case KindMotion:
		return "Motion"
	case KindTampering:
		return "Tampering"
	case KindImageQuality:
		return "ImageQuality"
	case KindDigitalInput:
		return "DigitalInput"
	case KindDigitalOutput:
		return "DigitalOutput"
	case KindObjectDetected:
		return "ObjectDetected"
	case KindAudioAlarm:
		return "AudioAlarm"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// State is the active/inactive level carried by a boolean ONVIF property
// event. StateUnknown is used both when the value cannot be parsed and
// when the topic is edge-triggered and carries no boolean state.
type State uint8

const (
	StateUnknown State = iota
	StateActive
	StateInactive
)

func (s State) String() string {
	switch s {
	case StateUnknown:
		return "Unknown"
	case StateActive:
		return "Active"
	case StateInactive:
		return "Inactive"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

// PropertyOperation mirrors the wsnt:PropertyOperation attribute.
// PropertyUnknown covers both "absent on the wire" (the attribute is
// optional) and "unrecognised value".
type PropertyOperation uint8

const (
	PropertyUnknown PropertyOperation = iota
	PropertyInitialized
	PropertyChanged
	PropertyDeleted
)

func (p PropertyOperation) String() string {
	switch p {
	case PropertyUnknown:
		return "Unknown"
	case PropertyInitialized:
		return "Initialized"
	case PropertyChanged:
		return "Changed"
	case PropertyDeleted:
		return "Deleted"
	default:
		return fmt.Sprintf("PropertyOperation(%d)", uint8(p))
	}
}

// Event is a single normalized notification from an ONVIF device.
//
// Source and Data are maps because ONVIF notifications can carry
// multiple SimpleItems — AXIS Object Analytics emits active+classType+
// confidence in one Data list, DigitalInput carries InputToken in Source
// and LogicalState in Data.
type Event struct {
	Kind      Kind
	State     State
	Operation PropertyOperation
	DeviceID  string
	Source    map[string]string
	Data      map[string]string
	Topic     string
	Timestamp time.Time
	// DeviceTime is the camera-reported wsnt:UtcTime. Cameras drift —
	// prefer Timestamp for ordering and DeviceTime only for forensics or
	// cross-camera correlation when the caller manages NTP.
	DeviceTime time.Time
	// AfterReconnect is true for events delivered after the Stream
	// silently recreated its subscription. Cameras replay current state
	// with PropertyInitialized on a new subscription; watch this flag to
	// suppress duplicate edge-detection. Cleared on the first non-
	// Initialized event.
	AfterReconnect bool
}

// Op identifies which Stream operation failed.
type Op string

const (
	OpPull     Op = "pull"
	OpRenew    Op = "renew"
	OpRecreate Op = "recreate"
)

// ErrPullFailed wraps a transient PullMessages failure. The pull loop
// surfaces it and continues.
type ErrPullFailed struct{ Err error }

func (e ErrPullFailed) Error() string { return fmt.Sprintf("pull messages: %v", e.Err) }
func (e ErrPullFailed) Unwrap() error { return e.Err }
func (ErrPullFailed) Op() Op          { return OpPull }

// ErrRenewFailed wraps a Renew SOAP failure. Recovered implicitly: a
// permanently failing renew lets the subscription die, pull starts
// failing, and the reconnect path recreates it.
type ErrRenewFailed struct{ Err error }

func (e ErrRenewFailed) Error() string { return fmt.Sprintf("renew pull point: %v", e.Err) }
func (e ErrRenewFailed) Unwrap() error { return e.Err }
func (ErrRenewFailed) Op() Op          { return OpRenew }

// ErrRecreateFailed wraps a failed CreatePullPointSubscription. Consumers
// seeing this repeatedly should consider the camera offline.
type ErrRecreateFailed struct{ Err error }

func (e ErrRecreateFailed) Error() string { return fmt.Sprintf("recreate pull point: %v", e.Err) }
func (e ErrRecreateFailed) Unwrap() error { return e.Err }
func (ErrRecreateFailed) Op() Op          { return OpRecreate }
