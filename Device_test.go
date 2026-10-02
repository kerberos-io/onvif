package onvif

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kerberos-io/onvif/deviceio"
	"github.com/kerberos-io/onvif/gosoap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevice_SetDeviceInfoFromScopes(t *testing.T) {
	const (
		name     = "DeviceName"
		hardware = "M9000"
	)
	scopes := []string{
		"onvif://www.onvif.org/Profile/Streaming",
		"onvif://www.onvif.org/SomethingElse/value",
		"onvif://www.onvif.org/name/" + name,
		"onvif://www.onvif.org/hardware/" + hardware,
	}
	device := Device{}
	device.SetDeviceInfoFromScopes(scopes)
	assert.Equal(t, device.info.Name, name)
	assert.Equal(t, device.info.Model, hardware)
}

// TestDevice_SendSoapWithHeader_InjectsHeaderXML verifies that the
// supplied header XML lands inside the SOAP <Header> element of the
// outgoing request. AXIS-style WS-Addressing reference parameter
// echoing depends on this — without it the camera returns
// ter:InvalidArgs on every PullMessages.
func TestDevice_SendSoapWithHeader_InjectsHeaderXML(t *testing.T) {
	const headerXML = `<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event" wsa:IsReferenceParameter="true">297</dom0:SubscriptionId>`
	const bodyXML = `<tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl"><tev:Timeout>PT5S</tev:Timeout><tev:MessageLimit>32</tev:MessageLimit></tev:PullMessages>`

	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{
		params: DeviceParams{
			Xaddr:      strings.TrimPrefix(srv.URL, "http://"),
			HttpClient: srv.Client(),
		},
	}
	resp, err := dev.SendSoapWithHeader(srv.URL, bodyXML, headerXML)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	headerStart := strings.Index(captured, "Header>")
	bodyStart := strings.Index(captured, "Body>")
	require.NotEqual(t, -1, headerStart, "envelope must contain <Header>; got: %s", captured)
	require.Greater(t, bodyStart, headerStart, "Body must follow Header in the envelope")

	headerSlice := captured[headerStart:bodyStart]
	assert.Contains(t, headerSlice, "SubscriptionId",
		"injected header element must land inside SOAP <Header>")
	assert.Contains(t, headerSlice, "297")

	bodySlice := captured[bodyStart:]
	assert.Contains(t, bodySlice, "PullMessages",
		"body content must land inside SOAP <Body>")
}

// Per WS-Addressing 1.0 SOAP Binding §3.4 every reference parameter is a separate
// SOAP Header block. Vendors that declare two ref params would silently
// produce a header-less request if the implementation only accepts a
// single top-level element.
func TestDevice_SendSoapWithHeader_AcceptsMultipleTopLevelChildren(t *testing.T) {
	const headerXML = `<a:Foo xmlns:a="ns/a">1</a:Foo><b:Bar xmlns:b="ns/b">2</b:Bar>`
	const bodyXML = `<tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl"/>`

	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{
		Xaddr:      strings.TrimPrefix(srv.URL, "http://"),
		HttpClient: srv.Client(),
	}}
	resp, err := dev.SendSoapWithHeader(srv.URL, bodyXML, headerXML)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	headerSlice := captured[strings.Index(captured, "Header>"):strings.Index(captured, "Body>")]
	assert.Contains(t, headerSlice, "Foo")
	assert.Contains(t, headerSlice, "Bar")
}

func TestDevice_SendSoapWithHeader_RejectsElementFreeContent(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	dev := Device{params: DeviceParams{HttpClient: srv.Client()}}

	// Well-formed XML but contains no child elements — would otherwise
	// parse, yield zero ChildElements, and send a header-less request.
	_, err := dev.SendSoapWithHeader(srv.URL, "<body/>", "just text content")
	require.Error(t, err)
	assert.Equal(t, 0, hits,
		"non-empty header content with no element children must fail fast")
}

// SendSoapWithOptions is the variadic shape that future per-call
// options (timeout, context, ...) will hang off. SendSoap and
// SendSoapWithHeader stay as thin convenience wrappers so existing
// callers are not forced to migrate.
func TestDevice_SendSoapWithOptions_WithHeaderMatchesSendSoapWithHeader(t *testing.T) {
	const headerXML = `<dom0:SubscriptionId xmlns:dom0="urn:test">42</dom0:SubscriptionId>`
	const bodyXML = `<tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl"/>`

	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{HttpClient: srv.Client()}}
	resp, err := dev.SendSoapWithOptions(srv.URL, bodyXML, WithSOAPHeader(headerXML))
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	assert.Contains(t, captured, "SubscriptionId")
	assert.Contains(t, captured, "42")
}

func TestDevice_SendSoapWithOptions_NoOptsMatchesSendSoap(t *testing.T) {
	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{HttpClient: srv.Client()}}
	resp, err := dev.SendSoapWithOptions(srv.URL, `<tev:Body xmlns:tev="x"/>`)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	assert.NotContains(t, captured, "IsReferenceParameter",
		"no opts should produce a header-less envelope")
}

// Digest auth fallback path: the camera 401s the first POST and the
// retry computes a digest. The ref-params header must survive the
// retry — losing it would silently re-introduce the AXIS regression
// on every authenticated camera.
func TestDevice_SendSoapWithHeader_PreservesHeaderAcrossDigestRetry(t *testing.T) {
	const headerXML = `<dom0:SubscriptionId xmlns:dom0="urn:vendor:axis" wsa:IsReferenceParameter="true">297</dom0:SubscriptionId>`
	var capturedSecondBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="onvif", nonce="abc", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		capturedSecondBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{
		Xaddr:      strings.TrimPrefix(srv.URL, "http://"),
		HttpClient: srv.Client(),
		Username:   "admin",
		Password:   "secret",
	}}
	resp, err := dev.SendSoapWithHeader(srv.URL, `<tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl"/>`, headerXML)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	assert.Contains(t, capturedSecondBody, "SubscriptionId",
		"digest retry must carry the same ref-params header as the first attempt")
	assert.Contains(t, capturedSecondBody, "297")
}

func TestDevice_SendSoapWithHeader_PropagatesMalformedHeaderError(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	dev := Device{params: DeviceParams{HttpClient: srv.Client()}}

	_, err := dev.SendSoapWithHeader(srv.URL, "<body/>", "<not-closed")
	require.Error(t, err)
	assert.Equal(t, 0, hits,
		"malformed header XML must fail fast — no request should reach the camera with a missing header block")
}

// Digest retry: networking.SendSoapWithDigest strips the wsse:Security
// element from the envelope before re-POSTing so credentials don't go
// on the wire twice (once via WS-Security, once via the digest header).
// Pin the behaviour from the Device layer.
func TestDevice_SendSoapWithOptions_DigestRetryStripsWSSE(t *testing.T) {
	var authedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="onvif", nonce="abc", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		authedBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{
		HttpClient: srv.Client(),
		Username:   "admin",
		Password:   "secret",
	}}
	resp, err := dev.SendSoapWithOptions(srv.URL, "<tev:X xmlns:tev='x'/>")
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.NotEmpty(t, authedBody, "expected an authenticated POST after the 401 challenge")
	assert.NotContains(t, authedBody, "UsernameToken",
		"digest retry must strip wsse:Security/UsernameToken; otherwise credentials go on the wire twice")
}

// Last-write-wins on duplicate SendSoapOption — pin the behaviour so
// the next maintainer adding an option doesn't accidentally introduce
// a merge or first-wins semantic.
func TestDevice_SendSoapWithOptions_DuplicateWithSOAPHeaderLastWins(t *testing.T) {
	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{HttpClient: srv.Client()}}
	_, err := dev.SendSoapWithOptions(srv.URL, "<body/>",
		WithSOAPHeader(`<a:First xmlns:a="x"/>`),
		WithSOAPHeader(`<b:Second xmlns:b="y"/>`),
	)
	require.NoError(t, err)
	assert.NotContains(t, captured, "First", "first WithSOAPHeader must be overwritten")
	assert.Contains(t, captured, "Second")
}

func newBothAuthTestSOAP() gosoap.SoapMessage {
	soap := gosoap.NewEmptySOAP()
	soap.AddStringBodyContent(`<tds:GetDeviceInformation xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>`)
	soap.AddRootNamespaces(Xlmns)
	return soap
}

// Regression: in "both" mode a WS-Security-only camera answers an
// unauthenticated request with a NotAuthorized SOAP fault (HTTP 400), not a
// 401 digest challenge. The first attempt must therefore carry the
// UsernameToken, otherwise the credentials never reach the camera.
func TestDevice_SendSOAP_BothSendsWSSecurityFirst(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), "UsernameToken") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`<s:Envelope><s:Body><s:Fault><s:Reason><s:Text>Sender not authorized</s:Text></s:Reason></s:Fault></s:Body></s:Envelope>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{
		HttpClient: srv.Client(),
		Username:   "admin",
		Password:   "secret",
		AuthMode:   Both,
	}}
	resp, err := dev.sendSOAP(srv.URL, newBothAuthTestSOAP())
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, hits, "WS-Security-only camera should succeed on the first request")
}

// "both" mode must still fall back to HTTP digest (without the WS-Security
// header) for cameras that only accept digest.
func TestDevice_SendSOAP_BothFallsBackToDigest(t *testing.T) {
	var authedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="onvif", nonce="abc", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		authedBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{params: DeviceParams{
		HttpClient: srv.Client(),
		Username:   "admin",
		Password:   "secret",
		AuthMode:   Both,
	}}
	resp, err := dev.sendSOAP(srv.URL, newBothAuthTestSOAP())
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, authedBody, "expected an authenticated digest POST")
	assert.NotContains(t, authedBody, "UsernameToken")
}

func TestEndpointKeyFromNamespace(t *testing.T) {
	cases := map[string]string{
		"http://www.onvif.org/ver10/device/wsdl":    "device",
		"http://www.onvif.org/ver10/deviceIO/wsdl":  "deviceio",
		"http://www.onvif.org/ver10/deviceio/wsdl":  "deviceio",
		"http://www.onvif.org/ver10/events/wsdl":    "events",
		"http://www.onvif.org/ver10/media/wsdl":     "media",
		"http://www.onvif.org/ver20/media/wsdl":     "media2",
		"http://www.onvif.org/ver20/ptz/wsdl":       "ptz",
		"http://www.onvif.org/ver20/analytics/wsdl": "analytics",
		"http://www.onvif.org/ver10/schema":         "",
		"":                                          "",
	}
	for namespace, want := range cases {
		assert.Equal(t, want, endpointKeyFromNamespace(namespace), namespace)
	}
}

// Some devices list DeviceIO only through GetServices. Those endpoints must be
// registered (with the host rewritten to Xaddr) without overriding endpoints
// already learned from GetCapabilities.
func TestDevice_AddServicesFromResponse(t *testing.T) {
	dev := Device{
		params:    DeviceParams{Xaddr: "127.0.0.1:8080"},
		endpoints: map[string]string{"events": "http://127.0.0.1:8080/onvif/events_from_capabilities"},
	}
	response := `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
<SOAP-ENV:Body><tds:GetServicesResponse>
<tds:Service><tds:Namespace>http://www.onvif.org/ver10/events/wsdl</tds:Namespace><tds:XAddr>http://10.0.0.5:80/onvif/event_service</tds:XAddr></tds:Service>
<tds:Service><tds:Namespace>http://www.onvif.org/ver10/deviceio/wsdl</tds:Namespace><tds:XAddr>http://10.0.0.5:80/onvif/deviceio_service</tds:XAddr></tds:Service>
</tds:GetServicesResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

	dev.addServicesFromResponse([]byte(response))

	endpoint, err := dev.getEndpoint("deviceio")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:8080/onvif/deviceio_service", endpoint)
	assert.Equal(t, "http://127.0.0.1:8080/onvif/events_from_capabilities", dev.endpoints["events"],
		"GetCapabilities endpoints must not be overridden")
}

// DeviceIO requests use the tmd prefix, which must be declared on the
// envelope or the request is not well-formed XML.
func TestDevice_CallMethod_DeclaresDeviceIONamespace(t *testing.T) {
	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		captured = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dev := Device{
		params:    DeviceParams{HttpClient: srv.Client()},
		endpoints: map[string]string{"deviceio": srv.URL + "/onvif/deviceio_service"},
	}
	resp, err := dev.CallMethod(deviceio.GetDigitalInputs{})
	require.NoError(t, err)
	resp.Body.Close()
	assert.Contains(t, captured, "<tmd:GetDigitalInputs")
	assert.Contains(t, captured, `xmlns:tmd="http://www.onvif.org/ver10/deviceIO/wsdl"`)
}
