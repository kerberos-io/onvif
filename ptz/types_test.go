package ptz

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/kerberos-io/onvif/xsd/onvif"
)

func TestContinuousMoveIncludesZeroPanTiltCoordinates(t *testing.T) {
	request := ContinuousMove{
		Velocity: onvif.PTZSpeedPanTilt{
			PanTilt: onvif.Vector2D{X: 0.5, Y: 0},
		},
	}

	encoded, err := xml.Marshal(request)
	if err != nil {
		t.Fatalf("xml.Marshal() error = %v", err)
	}
	if !strings.Contains(string(encoded), `x="0.5" y="0"`) {
		t.Fatalf("ContinuousMove PanTilt = %s, want explicit x and y attributes", encoded)
	}
}
