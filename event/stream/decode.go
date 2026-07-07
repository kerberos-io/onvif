package stream

import (
	"strings"
	"time"

	"github.com/kerberos-io/onvif/event"
)

// decode converts a single ONVIF NotificationMessage into a normalized
// Event. Topic, Source and Data are always populated even when Kind is
// KindUnknown so consumers can fall back to the wire form.
func decode(msg event.NotificationMessage, deviceID string, observedAt time.Time) Event {
	topic := string(msg.Topic.TopicKinds)
	desc := msg.Message.Message
	return Event{
		Kind:       Classify(topic),
		State:      extractState(desc.Data.SimpleItem),
		Operation:  parsePropertyOperation(string(desc.PropertyOperation)),
		DeviceID:   deviceID,
		Source:     simpleItemsToMap(desc.Source.SimpleItem),
		Data:       simpleItemsToMap(desc.Data.SimpleItem),
		Topic:      topic,
		Timestamp:  observedAt,
		DeviceTime: parseDeviceTime(string(desc.UtcTime)),
	}
}

// simpleItemsToMap returns nil for an empty list so empty notifications
// do not allocate.
func simpleItemsToMap(items []event.SimpleItem) map[string]string {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]string, len(items))
	for _, it := range items {
		m[string(it.Name)] = string(it.Value)
	}
	return m
}

// extractState scans Data items for a boolean-like value, returning the
// first match. Returns StateUnknown for edge-triggered topics like
// LineDetector/Crossed whose Data carries only an ObjectId.
func extractState(items []event.SimpleItem) State {
	for _, it := range items {
		switch strings.ToLower(strings.TrimSpace(string(it.Value))) {
		case "true", "1", "active":
			return StateActive
		case "false", "0", "inactive":
			return StateInactive
		}
	}
	return StateUnknown
}

// parsePropertyOperation returns PropertyUnknown for absent (optional
// per WS-Notification) or unrecognised values.
func parsePropertyOperation(s string) PropertyOperation {
	switch s {
	case "Initialized":
		return PropertyInitialized
	case "Changed":
		return PropertyChanged
	case "Deleted":
		return PropertyDeleted
	default:
		return PropertyUnknown
	}
}

// parseDeviceTime parses wsnt:UtcTime, returning the zero time when
// absent or unparseable. Real cameras emit several flavours: with /
// without sub-seconds, colon or compact ("+0200") offsets, and some
// older Hikvision firmwares omit the timezone entirely (treated as
// UTC per WS-BaseNotification which mandates UTC for UtcTime).
func parseDeviceTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range deviceTimeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

var deviceTimeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999-0700", // Geovision
	"2006-01-02T15:04:05-0700",     // some Dahua
	"2006-01-02T15:04:05.999",
	"2006-01-02T15:04:05", // older Hikvision (no timezone)
}
