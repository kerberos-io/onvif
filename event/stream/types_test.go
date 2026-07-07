package stream

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKindString(t *testing.T) {
	tests := []struct {
		name string
		kind Kind
		want string
	}{
		{"unknown", KindUnknown, "Unknown"},
		{"motion", KindMotion, "Motion"},
		{"tampering", KindTampering, "Tampering"},
		{"image_quality", KindImageQuality, "ImageQuality"},
		{"digital_input", KindDigitalInput, "DigitalInput"},
		{"digital_output", KindDigitalOutput, "DigitalOutput"},
		{"object_detected", KindObjectDetected, "ObjectDetected"},
		{"audio_alarm", KindAudioAlarm, "AudioAlarm"},
		{"out_of_range", Kind(255), "Kind(255)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.kind.String())
		})
	}
}

func TestKindStringsAreUnique(t *testing.T) {
	seen := map[string]Kind{}
	for k := KindUnknown; k <= KindAudioAlarm; k++ {
		s := k.String()
		prev, dup := seen[s]
		assert.False(t, dup, "duplicate String %q for Kind(%d) and Kind(%d)", s, prev, k)
		seen[s] = k
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{"unknown", StateUnknown, "Unknown"},
		{"active", StateActive, "Active"},
		{"inactive", StateInactive, "Inactive"},
		{"out_of_range", State(255), "State(255)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.state.String())
		})
	}
}

func TestPropertyOperationString(t *testing.T) {
	tests := []struct {
		name string
		op   PropertyOperation
		want string
	}{
		{"unknown", PropertyUnknown, "Unknown"},
		{"initialized", PropertyInitialized, "Initialized"},
		{"changed", PropertyChanged, "Changed"},
		{"deleted", PropertyDeleted, "Deleted"},
		{"out_of_range", PropertyOperation(255), "PropertyOperation(255)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.String())
		})
	}
}

func TestEventZeroValue(t *testing.T) {
	var e Event
	assert.Equal(t, KindUnknown, e.Kind)
	assert.Equal(t, StateUnknown, e.State)
	assert.Equal(t, PropertyUnknown, e.Operation)
	assert.Empty(t, e.DeviceID)
	assert.Nil(t, e.Source)
	assert.Nil(t, e.Data)
	assert.Empty(t, e.Topic)
	assert.True(t, e.Timestamp.IsZero())
	assert.True(t, e.DeviceTime.IsZero())
}

func TestEventFieldAssignmentRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	deviceTime := now.Add(-2 * time.Second)
	e := Event{
		Kind:       KindMotion,
		State:      StateActive,
		Operation:  PropertyChanged,
		DeviceID:   "axis-camera-01",
		Source:     map[string]string{"InputToken": "DI1"},
		Data:       map[string]string{"LogicalState": "true"},
		Topic:      "tns1:Device/Trigger/DigitalInput",
		Timestamp:  now,
		DeviceTime: deviceTime,
	}
	assert.Equal(t, KindMotion, e.Kind)
	assert.Equal(t, StateActive, e.State)
	assert.Equal(t, PropertyChanged, e.Operation)
	assert.Equal(t, "axis-camera-01", e.DeviceID)
	assert.Equal(t, "DI1", e.Source["InputToken"])
	assert.Equal(t, "true", e.Data["LogicalState"])
	assert.Equal(t, "tns1:Device/Trigger/DigitalInput", e.Topic)
	assert.True(t, e.Timestamp.Equal(now))
	assert.True(t, e.DeviceTime.Equal(deviceTime))
}

// --- Typed errors -----------------------------------------------------

func TestTypedErrors_UnwrapAndOp(t *testing.T) {
	inner := errors.New("boom")
	tests := []struct {
		name string
		err  error
		op   Op
	}{
		{"pull", ErrPullFailed{Err: inner}, OpPull},
		{"renew", ErrRenewFailed{Err: inner}, OpRenew},
		{"recreate", ErrRecreateFailed{Err: inner}, OpRecreate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, errors.Is(tc.err, inner), "errors.Is should unwrap to inner")
			assert.Contains(t, tc.err.Error(), "boom")
			if e, ok := tc.err.(interface{ Op() Op }); ok {
				assert.Equal(t, tc.op, e.Op())
			} else {
				t.Fatalf("%T does not expose Op()", tc.err)
			}
		})
	}
}
