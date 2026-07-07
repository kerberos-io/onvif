package stream

import (
	"strings"
	"testing"
	"time"

	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
	"github.com/stretchr/testify/assert"
)

// msg builds a NotificationMessage from the topic and a (PropertyOperation,
// UtcTime, source items, data items) tuple so tests stay short and intent
// is visible at the call site.
func msg(topic, propOp, utcTime string, source, data map[string]string) event.NotificationMessage {
	toItems := func(m map[string]string) []event.SimpleItem {
		if len(m) == 0 {
			return nil
		}
		items := make([]event.SimpleItem, 0, len(m))
		for k, v := range m {
			items = append(items, event.SimpleItem{
				Name:  xsd.AnyType(k),
				Value: xsd.AnyType(v),
			})
		}
		return items
	}
	return event.NotificationMessage{
		Topic: event.Topic{TopicKinds: xsd.String(topic)},
		Message: event.MessageBody{
			Message: event.MessageDescription{
				PropertyOperation: xsd.AnyType(propOp),
				UtcTime:           xsd.AnyType(utcTime),
				Source:            event.Source{SimpleItem: toItems(source)},
				Data:              event.Data{SimpleItem: toItems(data)},
			},
		},
	}
}

func TestDecode_MotionActive(t *testing.T) {
	observedAt := time.Date(2026, 5, 21, 10, 30, 1, 0, time.UTC)
	in := msg(
		"tns1:RuleEngine/CellMotionDetector/Motion",
		"Changed",
		"2026-05-21T10:30:00Z",
		map[string]string{
			"VideoSourceConfigurationToken": "VideoSourceConfigToken0",
			"Rule":                          "MyMotionRule",
		},
		map[string]string{"IsMotion": "true"},
	)

	ev := decode(in, "axis-cam-01", observedAt)

	assert.Equal(t, KindMotion, ev.Kind)
	assert.Equal(t, StateActive, ev.State)
	assert.Equal(t, PropertyChanged, ev.Operation)
	assert.Equal(t, "axis-cam-01", ev.DeviceID)
	assert.Equal(t, "VideoSourceConfigToken0", ev.Source["VideoSourceConfigurationToken"])
	assert.Equal(t, "MyMotionRule", ev.Source["Rule"])
	assert.Equal(t, "true", ev.Data["IsMotion"])
	assert.Equal(t, "tns1:RuleEngine/CellMotionDetector/Motion", ev.Topic)
	assert.True(t, ev.Timestamp.Equal(observedAt))
	assert.Equal(t, time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC), ev.DeviceTime)
}

func TestDecode_MotionInactive(t *testing.T) {
	in := msg(
		"tns1:VideoSource/MotionAlarm",
		"Changed",
		"",
		nil,
		map[string]string{"State": "false"},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindMotion, ev.Kind)
	assert.Equal(t, StateInactive, ev.State)
}

func TestDecode_HanwhaNumericMotionValue(t *testing.T) {
	// Hanwha emits xsd:string values "0"/"1" instead of xsd:boolean.
	in := msg(
		"tns1:VideoAnalytics/tnssamsung:MotionDetection",
		"Changed",
		"",
		nil,
		map[string]string{"Motion": "1"},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindMotion, ev.Kind)
	assert.Equal(t, StateActive, ev.State)
}

func TestDecode_AvigilonActiveLiteral(t *testing.T) {
	// Avigilon and a handful of older firmwares emit "active"/"inactive"
	// as the Data value rather than a boolean.
	in := msg(
		"tns1:Device/tns1:Trigger/tns1:Relay",
		"Changed",
		"",
		map[string]string{"RelayToken": "Relay-1"},
		map[string]string{"LogicalState": "active"},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindDigitalOutput, ev.Kind)
	assert.Equal(t, StateActive, ev.State)
	assert.Equal(t, "Relay-1", ev.Source["RelayToken"])
}

func TestDecode_AxisObjectAnalyticsMultiItem(t *testing.T) {
	// AOA emits active + classType + confidence in the same Data list.
	// The decoder must preserve every item; State picks the first
	// boolean-like value, which is 'active'.
	in := msg(
		"tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1",
		"Changed",
		"",
		map[string]string{"Source": "device1Scene1"},
		map[string]string{
			"active":     "1",
			"classType":  "Human",
			"confidence": "92",
		},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindObjectDetected, ev.Kind)
	assert.Equal(t, StateActive, ev.State)
	assert.Equal(t, "Human", ev.Data["classType"])
	assert.Equal(t, "92", ev.Data["confidence"])
	assert.Equal(t, "1", ev.Data["active"])
}

func TestDecode_LineDetectorCrossedHasNoState(t *testing.T) {
	// Edge-triggered topic — Data carries ObjectId, not a boolean. State
	// must remain Unknown so consumers do not misread it as level-Active.
	in := msg(
		"tns1:RuleEngine/LineDetector/Crossed",
		"Changed",
		"",
		map[string]string{"VideoSourceConfigurationToken": "vsct0", "Rule": "LineRule"},
		map[string]string{"ObjectId": "42"},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindObjectDetected, ev.Kind)
	assert.Equal(t, StateUnknown, ev.State)
	assert.Equal(t, "42", ev.Data["ObjectId"])
}

func TestDecode_UnknownTopicStillPreservesWireData(t *testing.T) {
	// Kind unknown does not mean discard: consumers may want to log or
	// route on the raw topic when classification misses.
	in := msg(
		"tns1:UserAlarm/IVA",
		"",
		"",
		nil,
		map[string]string{"Custom": "true"},
	)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, KindUnknown, ev.Kind)
	assert.Equal(t, "tns1:UserAlarm/IVA", ev.Topic)
	assert.Equal(t, "true", ev.Data["Custom"])
}

func TestDecode_PropertyOperationVariants(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want PropertyOperation
	}{
		{"initialized", "Initialized", PropertyInitialized},
		{"changed", "Changed", PropertyChanged},
		{"deleted", "Deleted", PropertyDeleted},
		{"absent", "", PropertyUnknown},
		{"unrecognised", "Bogus", PropertyUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", tc.in, "", nil, nil)
			ev := decode(in, "dev", time.Now())
			assert.Equal(t, tc.want, ev.Operation)
		})
	}
}

func TestDecode_DeviceTimeParsing(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Time
	}{
		{"rfc3339_utc", "2026-05-21T10:30:00Z", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
		{"rfc3339_with_offset", "2026-05-21T12:30:00+02:00", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
		{"rfc3339_subsecond", "2026-05-21T10:30:00.500Z", time.Date(2026, 5, 21, 10, 30, 0, 500_000_000, time.UTC)},
		{"absent", "", time.Time{}},
		{"unparseable", "not-a-date", time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", tc.in, nil, nil)
			ev := decode(in, "dev", time.Now())
			if tc.want.IsZero() {
				assert.True(t, ev.DeviceTime.IsZero(), "DeviceTime=%v", ev.DeviceTime)
			} else {
				assert.True(t, ev.DeviceTime.Equal(tc.want), "got=%v want=%v", ev.DeviceTime, tc.want)
			}
		})
	}
}

func TestDecode_EmptySourceAndDataYieldNilMaps(t *testing.T) {
	// Matches the zero-value contract in types_test.go: callers can
	// safely len() and index into Source/Data without nil-checking, but
	// we do not allocate an empty map for empty notifications.
	in := msg("tns1:VideoSource/MotionAlarm", "Changed", "", nil, nil)
	ev := decode(in, "dev", time.Now())
	assert.Nil(t, ev.Source)
	assert.Nil(t, ev.Data)
}

func TestDecode_StateValueIsCaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  State
	}{
		{"true_lower", "true", StateActive},
		{"true_upper", "TRUE", StateActive},
		{"true_mixed", "True", StateActive},
		{"false_lower", "false", StateInactive},
		{"false_mixed", "False", StateInactive},
		{"active_mixed", "Active", StateActive},
		{"inactive_mixed", "Inactive", StateInactive},
		{"one", "1", StateActive},
		{"zero", "0", StateInactive},
		{"empty", "", StateUnknown},
		{"nonsense", "maybe", StateUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", "",
				nil, map[string]string{"State": tc.value})
			ev := decode(in, "dev", time.Now())
			assert.Equal(t, tc.want, ev.State)
		})
	}
}

// --- Edge cases for state extraction and time parsing ----------------

func TestDecode_PropertyOperationIsCaseSensitive(t *testing.T) {
	// Per WS-Notification §3.3 PropertyOperation values are
	// 'Initialized' / 'Changed' / 'Deleted'. Lowercased forms are
	// malformed and should fall through to PropertyUnknown.
	in := msg("tns1:VideoSource/MotionAlarm", "changed", "", nil, nil)
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, PropertyUnknown, ev.Operation)
}

func TestDecode_StateValueTrimsWhitespace(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  State
	}{
		{"leading_trailing", "  true  ", StateActive},
		{"tab_newline", "\ttrue\n", StateActive},
		{"only_spaces", "   ", StateUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", "",
				nil, map[string]string{"State": tc.value})
			ev := decode(in, "dev", time.Now())
			assert.Equal(t, tc.want, ev.State)
		})
	}
}

func TestDecode_SimpleItemEmptyValueIsUnknownState(t *testing.T) {
	in := msg("tns1:VideoSource/MotionAlarm", "Changed", "",
		nil, map[string]string{"State": ""})
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, StateUnknown, ev.State)
	v, ok := ev.Data["State"]
	assert.True(t, ok)
	assert.Equal(t, "", v)
}

func TestDecode_DeviceTimeAdditionalLayouts(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Time
	}{
		{"compact_offset", "2026-05-21T12:30:00+0200", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
		{"compact_offset_subsec", "2026-05-21T12:30:00.500+0200", time.Date(2026, 5, 21, 10, 30, 0, 500_000_000, time.UTC)},
		{"naked_no_tz", "2026-05-21T10:30:00", time.Date(2026, 5, 21, 10, 30, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := msg("tns1:VideoSource/MotionAlarm", "Changed", tc.in, nil, nil)
			ev := decode(in, "dev", time.Now())
			assert.True(t, ev.DeviceTime.Equal(tc.want),
				"input=%q got=%v want=%v", tc.in, ev.DeviceTime, tc.want)
		})
	}
}

func TestDecode_DeviceTimeStillRejectsNonsense(t *testing.T) {
	for _, s := range []string{"hello", "2026-13-45T99:99:99", strings.Repeat("9", 50)} {
		in := msg("tns1:VideoSource/MotionAlarm", "Changed", s, nil, nil)
		ev := decode(in, "dev", time.Now())
		assert.True(t, ev.DeviceTime.IsZero(), "input=%q should yield zero, got %v", s, ev.DeviceTime)
	}
}

// --- First-boolean-wins with explicit slice order --------------------

type pair struct{ k, v string }

func simpleItemsFromPairs(pairs []pair) []event.SimpleItem {
	out := make([]event.SimpleItem, len(pairs))
	for i, p := range pairs {
		out[i] = event.SimpleItem{
			Name:  xsd.AnyType(p.k),
			Value: xsd.AnyType(p.v),
		}
	}
	return out
}

func TestExtractState_FirstBooleanLikeWins(t *testing.T) {
	// Documented behaviour: when multiple Data items have boolean-like
	// values, the first by slice order wins. Use explicit slice
	// construction so the assertion does not depend on map iteration
	// order.
	in := msg("tns1:VideoSource/MotionAlarm", "Changed", "", nil, nil)
	in.Message.Message.Data.SimpleItem = simpleItemsFromPairs([]pair{
		{"ObjectId", "42"},
		{"State", "true"},
		{"Trailer", "false"},
	})
	ev := decode(in, "dev", time.Now())
	assert.Equal(t, StateActive, ev.State,
		"first boolean-like value (State=true) must win, not Trailer=false")
}
