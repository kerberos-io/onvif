package stream

import "strings"

// Classify maps an ONVIF topic string to the normalized Kind. Returns
// KindUnknown when no rule matches.
//
// The classifier strips XML-namespace prefixes from each "/"-separated
// segment so it is robust to vendor namespaces (tns1:, tnsaxis:,
// tnssamsung:, ...). Matching is case-sensitive — ONVIF topics are
// case-sensitive per the spec.
//
// Sources cross-checked when building the rule set below:
//   - ONVIF Topic Namespace XML
//     https://www.onvif.org/onvif/ver10/topics/topicns.xml
//   - ONVIF Analytics Service Spec
//     https://www.onvif.org/specs/srv/analytics/ONVIF-VideoAnalytics-Service-Spec-v220.pdf
//   - ONVIF Device IO Service Spec
//     https://www.onvif.org/specs/srv/io/ONVIF-DeviceIo-Service-Spec.pdf
//   - openvideolibs/onvif-parsers (Apache-2.0) — empirical topic table
//     extracted from Home Assistant
//     https://github.com/openvideolibs/onvif-parsers
func Classify(topic string) Kind {
	if topic == "" {
		return KindUnknown
	}
	canonical := canonicalizeTopic(topic)
	for _, rule := range topicRules {
		if strings.Contains(canonical, rule.needle) {
			return rule.kind
		}
	}
	return KindUnknown
}

// canonicalizeTopic strips the XML-namespace prefix from each
// "/"-separated segment, collapsing Avigilon's per-segment-prefixed
// form ("tns1:Device/tns1:Trigger/tns1:Relay") and the plain form
// ("tns1:Device/Trigger/Relay") to the same matchable path.
func canonicalizeTopic(topic string) string {
	segments := strings.Split(topic, "/")
	for i, seg := range segments {
		if idx := strings.Index(seg, ":"); idx >= 0 {
			segments[i] = seg[idx+1:]
		}
	}
	return strings.Join(segments, "/")
}

// topicRules is evaluated in order — first match wins. Keep more
// specific rules ahead of broader ones. LineDetector/Crossed is
// edge-triggered (no boolean State); the decoder leaves State as
// StateUnknown for it.
var topicRules = []struct {
	needle string
	kind   Kind
}{
	// tns1:VideoSource/MotionAlarm — Profile S basic motion.
	// https://www.onvif.org/ver10/topics/topicns.xml
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"VideoSource/MotionAlarm", KindMotion},

	// tns1:VideoAnalytics/MotionAlarm — Bosch publishes motion under
	// VideoAnalytics rather than VideoSource.
	// https://media.boschsecurity.com/fs/media/pb/media/partners_1/integration_tools_1/developer/bosch-metadata-and-iva-events.pdf
	{"VideoAnalytics/MotionAlarm", KindMotion},

	// tns1:VideoAnalytics/tnssamsung:MotionDetection — Hanwha vendor.
	// https://github.com/home-assistant/core/issues/66493
	{"VideoAnalytics/MotionDetection", KindMotion},

	// tns1:RuleEngine/CellMotionDetector/Motion — ONVIF Analytics
	// standard cell-motion rule.
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.3
	// https://www.hikvisioneurope.com/eu/portal/portal/Technical%20Materials/24%20How%20To/CCTV/How%20to%20solve%20third%20party%20camera%20motion%20detection%20issue.pdf
	{"CellMotionDetector/Motion", KindMotion},

	// tns1:RuleEngine/MotionRegionDetector/Motion — AXIS region rule.
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"MotionRegionDetector/Motion", KindMotion},

	// AXIS Guard suite — vendor analytics apps with Camera<N>Profile<ID>
	// suffixes. Treated as motion so they can drive motion-triggered
	// recording on cameras using these apps instead of basic VMD.
	// https://developer.axis.com/vapix/applications/motion-guard
	{"CameraApplicationPlatform/MotionGuard/", KindMotion},
	{"CameraApplicationPlatform/FenceGuard/", KindMotion},
	{"CameraApplicationPlatform/LoiteringGuard/", KindMotion},

	// tns1:RuleEngine/TamperDetector/Tamper — standard ONVIF tamper rule.
	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.5
	{"TamperDetector/Tamper", KindTampering},

	// tns1:VideoSource/GlobalSceneChange/ImagingService — the proper
	// lens-cover signal on firmwares without TamperDetector.
	// https://www.onvif.org/ver10/topics/topicns.xml
	{"GlobalSceneChange", KindTampering},

	// tns1:VideoAnalytics/tnssamsung:TamperingDetection — Hanwha.
	// https://github.com/home-assistant/core/issues/66493
	{"VideoAnalytics/TamperingDetection", KindTampering},

	// VideoSource/ImageToo* — imaging-quality alarms. See KindImageQuality
	// for the rationale on splitting these out from KindTampering.
	// https://www.onvif.org/ver10/topics/topicns.xml
	{"VideoSource/ImageTooDark", KindImageQuality},
	{"VideoSource/ImageTooBright", KindImageQuality},
	{"VideoSource/ImageTooBlurry", KindImageQuality},

	// tns1:Device/Trigger/DigitalInput — standard. Avigilon's per-segment-
	// prefixed serialisation ("tns1:Device/tns1:Trigger/tns1:DigitalInput")
	// folds to the same canonical path.
	// ONVIF-DeviceIo-Service-Spec.pdf §5.2
	{"Trigger/DigitalInput", KindDigitalInput},
	// ONVIF-DeviceIo-Service-Spec.pdf §5.3
	{"Trigger/Relay", KindDigitalOutput},

	// tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario<N>
	// — Scenario suffixes are numeric per AOA configuration. Prefix-match
	// because of the dynamic suffix.
	// https://developer.axis.com/analytics/axis-object-analytics/how-to-guides/axis-object-analytics-counting-data/
	{"ObjectAnalytics/", KindObjectDetected},

	// ONVIF-VideoAnalytics-Service-Spec-v220.pdf §5.4
	{"LineDetector/Crossed", KindObjectDetected},
	{"FieldDetector/ObjectsInside", KindObjectDetected},

	// tns1:RuleEngine/MyRuleDetector/<RuleName> — vendor rules under the
	// ONVIF MyRuleDetector container. Explicitly whitelisted because the
	// same container also carries non-object rules (Bosch Counter,
	// Occupancy) that must not classify as ObjectDetected.
	// https://media.boschsecurity.com/fs/media/pb/media/partners_1/integration_tools_1/developer/bosch-metadata-and-iva-events.pdf
	{"MyRuleDetector/HumanDetect", KindObjectDetected},
	{"MyRuleDetector/VehicleDetect", KindObjectDetected},
	{"MyRuleDetector/PeopleDetect", KindObjectDetected},
	{"MyRuleDetector/ObjectsInside", KindObjectDetected},
	{"MyRuleDetector/FaceDetect", KindObjectDetected},

	{"Audio/DetectedSound", KindAudioAlarm},
	// https://developer.axis.com/vapix/network-video/event-and-action-services/
	{"AudioSource/TriggerLevel", KindAudioAlarm},
	{"AudioAnalytics/SoundDetection", KindAudioAlarm},
}
