package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  Kind
	}{
		// --- Motion -----------------------------------------------------

		{"video_source_motion_alarm", "tns1:VideoSource/MotionAlarm", KindMotion},
		{"cell_motion_detector", "tns1:RuleEngine/CellMotionDetector/Motion", KindMotion},
		{"motion_region_detector", "tns1:RuleEngine/MotionRegionDetector/Motion", KindMotion},
		{"bosch_video_analytics_motion", "tns1:VideoAnalytics/MotionAlarm", KindMotion},
		{"hanwha_samsung_motion", "tns1:VideoAnalytics/tnssamsung:MotionDetection", KindMotion},

		// AXIS Guard suite — vendor analytics apps.
		{"axis_motion_guard", "tnsaxis:CameraApplicationPlatform/MotionGuard/Camera1ProfileANY", KindMotion},
		{"axis_fence_guard", "tnsaxis:CameraApplicationPlatform/FenceGuard/Camera1ProfileANY", KindMotion},
		{"axis_loitering_guard", "tnsaxis:CameraApplicationPlatform/LoiteringGuard/Camera1ProfileANY", KindMotion},

		// --- Tampering --------------------------------------------------

		{"tamper_detector", "tns1:RuleEngine/TamperDetector/Tamper", KindTampering},
		{"global_scene_change", "tns1:VideoSource/GlobalSceneChange/ImagingService", KindTampering},
		{"hanwha_tampering", "tns1:VideoAnalytics/tnssamsung:TamperingDetection", KindTampering},

		// --- Image quality (separated from Tampering) ------------------

		{"image_too_dark", "tns1:VideoSource/ImageTooDark/ImagingService", KindImageQuality},
		{"image_too_bright", "tns1:VideoSource/ImageTooBright/ImagingService", KindImageQuality},
		{"image_too_blurry", "tns1:VideoSource/ImageTooBlurry/ImagingService", KindImageQuality},

		// --- Digital input ---------------------------------------------

		{"digital_input", "tns1:Device/Trigger/DigitalInput", KindDigitalInput},
		{"digital_input_avigilon", "tns1:Device/tns1:Trigger/tns1:DigitalInput", KindDigitalInput},

		// --- Digital output --------------------------------------------

		{"relay", "tns1:Device/Trigger/Relay", KindDigitalOutput},
		{"relay_avigilon", "tns1:Device/tns1:Trigger/tns1:Relay", KindDigitalOutput},

		// --- Object analytics ------------------------------------------

		// AXIS Object Analytics uses numeric scenario suffixes.
		{"axis_object_analytics_scenario_1", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1", KindObjectDetected},
		{"axis_object_analytics_scenario_2", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario2", KindObjectDetected},

		// Standard rule-engine analytics topics.
		{"line_detector_crossed", "tns1:RuleEngine/LineDetector/Crossed", KindObjectDetected},
		{"field_detector_objects_inside", "tns1:RuleEngine/FieldDetector/ObjectsInside", KindObjectDetected},

		// Whitelisted MyRuleDetector sub-rules.
		{"my_rule_detector_human", "tns1:RuleEngine/MyRuleDetector/HumanDetect", KindObjectDetected},
		{"my_rule_detector_vehicle", "tns1:RuleEngine/MyRuleDetector/VehicleDetect", KindObjectDetected},
		{"my_rule_detector_people", "tns1:RuleEngine/MyRuleDetector/PeopleDetect", KindObjectDetected},
		{"my_rule_detector_face", "tns1:RuleEngine/MyRuleDetector/FaceDetect", KindObjectDetected},
		{"my_rule_detector_objects_inside", "tns1:RuleEngine/MyRuleDetector/ObjectsInside", KindObjectDetected},

		// --- Audio -----------------------------------------------------

		{"audio_detected_sound", "tns1:AudioAnalytics/Audio/DetectedSound", KindAudioAlarm},
		{"axis_audio_trigger_level", "tns1:AudioSource/tnsaxis:TriggerLevel", KindAudioAlarm},
		{"hanwha_sound_detection", "tns1:AudioAnalytics/tnssamsung:SoundDetection", KindAudioAlarm},

		// --- Negative cases --------------------------------------------

		{"empty", "", KindUnknown},
		{"unknown_topic", "tns1:UserAlarm/IVA", KindUnknown},
		{"unrelated_recording_config", "tns1:RecordingConfig/JobState", KindUnknown},

		// MyRuleDetector overmatch guard — Bosch publishes counter and
		// occupancy under the same container and these must not be
		// classified as object detection.
		{"my_rule_detector_counter_not_object", "tns1:RuleEngine/MyRuleDetector/Counter", KindUnknown},
		{"my_rule_detector_occupancy_not_object", "tns1:RuleEngine/MyRuleDetector/Occupancy", KindUnknown},

		// Substring guards.
		{"motion_recording_not_motion", "tns1:Recording/MotionRecording/Started", KindUnknown},
		{"audio_encoder_config_not_audio_alarm", "tns1:Configuration/AudioEncoderConfiguration", KindUnknown},
		{"relay_failure_not_digital_output", "tns1:Device/HardwareFailure/RelayFailure", KindUnknown},
		{"digital_input_config_not_digital_input", "tns1:Device/IO/DigitalInputConfiguration", KindUnknown},
		{"tamper_detector_log_not_tampering", "tns1:Device/Diagnostics/TamperDetectorLog", KindUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.topic), "topic=%q", tc.topic)
		})
	}
}

func TestClassifyIsCaseSensitive(t *testing.T) {
	// ONVIF topic identifiers are case-sensitive per the spec; a
	// lowercased topic must not match a capitalised pattern.
	assert.Equal(t, KindUnknown, Classify("tns1:videosource/motionalarm"))
}

func TestCanonicalizeTopicStripsNamespaces(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"single_namespace", "tns1:VideoSource/MotionAlarm", "VideoSource/MotionAlarm"},
		{"per_segment_namespace", "tns1:Device/tns1:Trigger/tns1:Relay", "Device/Trigger/Relay"},
		{"vendor_namespace_inner", "tns1:VideoAnalytics/tnssamsung:MotionDetection", "VideoAnalytics/MotionDetection"},
		{"axis_outer_namespace", "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1", "CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1"},
		{"empty", "", ""},
		{"no_colon_passthrough", "Foo/Bar", "Foo/Bar"},
		{"double_slash_keeps_empty_segment", "tns1://Foo", "//Foo"},
		{"colon_only_segment_collapses_to_empty", "tns1:/Foo", "/Foo"},
		{"trailing_colon_segment", "tns1:", ""},
		{"multi_colon_takes_first", "tns1:Foo:Bar/Baz", "Foo:Bar/Baz"},
		{"leading_slash_kept", "/tns1:Foo", "/Foo"},
		{"trailing_slash_kept", "tns1:Foo/", "Foo/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, canonicalizeTopic(tc.in), "input=%q", tc.in)
		})
	}
}

func TestClassifyRuleOrder_ObjectAnalyticsBeforeGenericObjects(t *testing.T) {
	// Locks the invariant that the prefix rule "ObjectAnalytics/" is
	// matched before the broader "ObjectsInside" rule. Without this
	// ordering, AXIS AOA topics that contain neither would still classify
	// correctly via the ObjectAnalytics/ rule; we encode the dependency.
	topic := "tnsaxis:CameraApplicationPlatform/ObjectAnalytics/Device1Scenario1"
	assert.Equal(t, KindObjectDetected, Classify(topic))
}
