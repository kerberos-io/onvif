// Package stream is a typed, channel-based consumer for ONVIF device
// events. It hides the SOAP/XML, pull-point subscription lifecycle,
// subscription renewal and vendor-specific topic conventions behind a
// single Event stream.
//
// # Usage
//
//	dev, _ := onvif.NewDevice(onvif.DeviceParams{Xaddr: "...", Username: "...", Password: "..."})
//	s, err := stream.NewStream(ctx, dev, stream.Options{DeviceID: "front-door"})
//	if err != nil { /* construction failed: auth, network, or no event support */ }
//	defer s.Close()
//
//	for ev := range s.Events() {
//	    switch ev.Kind {
//	    case stream.KindMotion:
//	        if ev.State == stream.StateActive { /* start recording */ }
//	    }
//	}
//
// NewStream performs network I/O so auth and reachability failures
// surface synchronously. Events and Errors close when the Stream stops;
// Errors sends are non-blocking so a stalled consumer drops older
// errors rather than blocking the pull loop. After a silent reconnect,
// the next batch's events carry Event.AfterReconnect=true.
//
// See topics.go for the verified topic→Kind mapping across AXIS,
// Hikvision, Avigilon, Hanwha, Bosch and Dahua.
package stream
