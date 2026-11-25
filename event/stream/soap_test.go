package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/kerberos-io/onvif"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- SOAP fault detection ---------------------------------------------

func TestExtractSOAPFault_SOAP11(t *testing.T) {
	body := `<?xml version="1.0"?>
<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <env:Fault>
      <faultcode>env:Client</faultcode>
      <faultstring>The action requested requires authorization and the sender is not authorized</faultstring>
    </env:Fault>
  </env:Body>
</env:Envelope>`
	got := extractSOAPFault(body)
	assert.Contains(t, got, "not authorized")
}

func TestExtractSOAPFault_SOAP12(t *testing.T) {
	body := `<?xml version="1.0"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body>
    <env:Fault>
      <env:Code><env:Value>env:Sender</env:Value></env:Code>
      <env:Reason><env:Text xml:lang="en">Subscription has expired</env:Text></env:Reason>
    </env:Fault>
  </env:Body>
</env:Envelope>`
	got := extractSOAPFault(body)
	assert.Contains(t, got, "Subscription has expired")
}

func TestExtractSOAPFault_NotAFault(t *testing.T) {
	assert.Empty(t, extractSOAPFault(createPullPointResp))
}

func TestExtractSOAPFault_EmptyBody(t *testing.T) {
	assert.Empty(t, extractSOAPFault(""))
}

func TestUnmarshalNode_ReturnsFaultReasonInsteadOfMissingElement(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body><env:Fault><faultstring>not authorized</faultstring></env:Fault></env:Body>
</env:Envelope>`
	var out struct{}
	err := unmarshalNode(body, "PullMessagesResponse", &out)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")
	assert.NotContains(t, err.Error(), "missing PullMessagesResponse")
}

// --- Bounded body read -----------------------------------------------

func TestReadClose_LimitsBodySize(t *testing.T) {
	if maxResponseBytes < 1024 {
		t.Skip("limit too small for this test")
	}
	big := strings.Repeat("A", maxResponseBytes+1024)
	body := "<env:Envelope><env:Body>" + big + "</env:Body></env:Envelope>"
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	// Construction will fail because the truncated body has no
	// CreatePullPointSubscriptionResponse — that's fine; what matters is
	// the read completes without OOM.
	_, err := newStream(testContext(t), fc, Options{})
	assert.Error(t, err)
}

// testContext returns a Background context already wired to cancel via
// t.Cleanup so the test does not need to manage the cancellation
// goroutine inline.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

// --- Error enrichment from SOAP response bodies ----------------------

func fakeResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestEnrichSOAPErr_NilErrReturnsNil(t *testing.T) {
	assert.NoError(t, enrichSOAPErr(fakeResponse("anything"), nil))
}

func TestEnrichSOAPErr_NilRespPreservesOriginal(t *testing.T) {
	orig := errors.New("transport boom")
	got := enrichSOAPErr(nil, orig)
	assert.ErrorIs(t, got, orig)
	assert.Equal(t, orig.Error(), got.Error(), "no body, no extra context to add")
}

func TestEnrichSOAPErr_SOAP11FaultStringAppearsInError(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body><env:Fault><faultstring>not authorized</faultstring></env:Fault></env:Body>
</env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "400 Bad Request")
	assert.Contains(t, got.Error(), "not authorized")
}

func TestEnrichSOAPErr_SOAP12ReasonAppearsInError(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code><env:Value>env:Sender</env:Value></env:Code>
    <env:Reason><env:Text xml:lang="en">Subscription has expired</env:Text></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "Subscription has expired")
}

// Pins the AXIS case: a Fault with populated Subcode but an empty
// <Reason><Text/></Reason>. Without subcode fallback, the only signal
// the operator sees is "400 Bad Request".
func TestEnrichSOAPErr_EmptyReasonFallsBackToSubcode(t *testing.T) {
	body := `<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:ter="http://www.onvif.org/ver10/error">
  <SOAP-ENV:Body><SOAP-ENV:Fault>
    <SOAP-ENV:Code>
      <SOAP-ENV:Value>SOAP-ENV:Sender</SOAP-ENV:Value>
      <SOAP-ENV:Subcode><SOAP-ENV:Value>ter:InvalidArgs</SOAP-ENV:Value></SOAP-ENV:Subcode>
    </SOAP-ENV:Code>
    <SOAP-ENV:Reason><SOAP-ENV:Text xml:lang="en"/></SOAP-ENV:Reason>
  </SOAP-ENV:Fault></SOAP-ENV:Body>
</SOAP-ENV:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("Post with digest error: 400: 400 Bad Request"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "ter:InvalidArgs",
		"AXIS-style empty-Reason Faults must surface their Subcode")
}

func TestEnrichSOAPErr_NonFaultBodyIncludesExcerpt(t *testing.T) {
	body := `<html><body>404 Not Found — /onvif/services missing</body></html>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("404 Not Found"))
	require.Error(t, got)
	assert.Contains(t, got.Error(), "/onvif/services missing")
}

func TestEnrichSOAPErr_LargeNonFaultBodyTruncated(t *testing.T) {
	// A misbehaving camera could stream a multi-megabyte body. The
	// helper must cap the excerpt so a wedged camera does not flood
	// logs.
	body := strings.Repeat("X", 8192)
	got := enrichSOAPErr(fakeResponse(body), errors.New("500"))
	require.Error(t, got)
	assert.Less(t, len(got.Error()), 2048,
		"enriched error must stay log-line sized even on huge bodies")
}

func TestEnrichSOAPErr_PreservesOriginalForErrorsIs(t *testing.T) {
	// Callers wrap pull/renew/recreate errors with errors.As in
	// logStreamError; enrichment must keep the original wrappable.
	orig := errors.New("sentinel")
	got := enrichSOAPErr(fakeResponse(`<env:Fault><faultstring>x</faultstring></env:Fault>`), orig)
	assert.ErrorIs(t, got, orig)
}

// --- Subcode extraction ----------------------------------------------

func TestExtractSOAPSubcode_Present(t *testing.T) {
	body := `<SOAP-ENV:Code>
  <SOAP-ENV:Value>SOAP-ENV:Sender</SOAP-ENV:Value>
  <SOAP-ENV:Subcode><SOAP-ENV:Value>ter:InvalidArgs</SOAP-ENV:Value></SOAP-ENV:Subcode>
</SOAP-ENV:Code>`
	assert.Equal(t, "ter:InvalidArgs", extractSOAPSubcode(body))
}

func TestExtractSOAPSubcode_Absent(t *testing.T) {
	assert.Empty(t, extractSOAPSubcode(`<env:Code><env:Value>env:Sender</env:Value></env:Code>`))
}

// --- Wiring: each SOAP call site routes errors through enrichSOAPErr -

const faultBody = `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code><env:Value>env:Sender</env:Value></env:Code>
    <env:Reason><env:Text xml:lang="en">camera-specific complaint</env:Text></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`

func TestCreatePullPoint_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(faultBody, errors.New("400 Bad Request"))
	_, err := createPullPoint(fc, defaultOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"createPullPoint must enrich transport errors with the camera's SOAP fault")
}

func TestPullMessages_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap(faultBody, errors.New("400 Bad Request"))
	_, err := pullMessages(fc, subscriptionRef{Address: "http://camera/sub"}, defaultOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"pullMessages must enrich transport errors with the camera's SOAP fault")
}

func TestUnsubscribePullPoint_EnrichesTransportErrWithFaultReason(t *testing.T) {
	fc := newFakeCaller()
	fc.queueSendSoap(faultBody, errors.New("400 Bad Request"))
	err := unsubscribePullPoint(fc, subscriptionRef{Address: "http://camera/sub"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "camera-specific complaint",
		"unsubscribePullPoint must enrich transport errors with the camera's SOAP fault")
}

// --- ReferenceParameters extraction (WS-Addressing 1.0 SOAP Binding §3.4) ---------
//
// AXIS encodes the subscription identity in <wsa:ReferenceParameters>
// inside CreatePullPointSubscriptionResponse rather than in the URL
// itself. Subsequent PullMessages/Renew/Unsubscribe MUST echo those
// elements verbatim into the SOAP Header, or the camera responds with
// ter:InvalidArgs. The auto-generated event.ReferenceParametersType is
// an empty struct (drops children), so we extract the raw inner XML.

func TestExtractReferenceParameters_AXISShape(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa5="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa5:Address>http://192.168.1.10/onvif/services</wsa5:Address>
      <wsa5:ReferenceParameters>
        <dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>
      </wsa5:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	got := extractReferenceParameters(body)
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.Contains(t, got, `xmlns:dom0="http://www.axis.com/2009/event"`,
		"namespace declaration on the SubscriptionId child must survive extraction")
}

func TestExtractReferenceParameters_AbsentReturnsEmpty(t *testing.T) {
	// Geovision/Hikvision-style: Address only, no ReferenceParameters.
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing">
  <env:Body><tev:CreatePullPointSubscriptionResponse xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
    <tev:SubscriptionReference>
      <wsa:Address>http://camera/onvif/Events/Sub_1</wsa:Address>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	assert.Empty(t, extractReferenceParameters(body))
}

func TestExtractReferenceParameters_EmptyBodyReturnsEmpty(t *testing.T) {
	assert.Empty(t, extractReferenceParameters(""))
}

// --- createPullPoint returns both address and ref params -------------

func TestCreatePullPoint_ReturnsRefParamsAlongsideAddress(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa5="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa5:Address>http://192.168.1.10/onvif/services</wsa5:Address>
      <wsa5:ReferenceParameters>
        <dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>
      </wsa5:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	assert.Equal(t, "http://192.168.1.10/onvif/services", ref.Address)
	assert.Contains(t, ref.RefParamsXML, "SubscriptionId")
	assert.Contains(t, ref.RefParamsXML, "297")
}

func TestCreatePullPoint_VendorWithoutRefParams_RefParamsEmpty(t *testing.T) {
	fc := newFakeCaller()
	fc.queueCallMethod(createPullPointResp, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	assert.NotEmpty(t, ref.Address)
	assert.Empty(t, ref.RefParamsXML)
}

// --- Reference-parameter echoing in subscription-scoped calls --------
//
// WS-Addressing 1.0 SOAP Binding §3.4 requires each <wsa:ReferenceParameters> child
// to be echoed as a SOAP Header block carrying wsa:IsReferenceParameter
// ="true". AXIS rejects PullMessages with ter:InvalidArgs when this is
// absent.

func TestPullMessages_EchoesRefParamsWithIsReferenceParameterAttribute(t *testing.T) {
	ref := subscriptionRef{
		Address:      "http://192.168.1.10/onvif/services",
		RefParamsXML: `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing"><dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId></wsa:ReferenceParameters>`,
	}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Contains(t, hdr, "SubscriptionId", "ref param element must be echoed")
	assert.Contains(t, hdr, "297", "ref param value must be echoed")
	assert.Contains(t, hdr, `IsReferenceParameter="true"`,
		"WS-Addressing 1.0 SOAP Binding §3.4 requires the attribute on each echoed element")
}

func TestPullMessages_NoRefParams_NoReferenceParameterEchoed(t *testing.T) {
	ref := subscriptionRef{Address: "http://camera/sub", RefParamsXML: ""}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	for _, el := range headerElements(t, fc.sendSoapHeaders[0]) {
		assert.Equal(t, "http://www.w3.org/2005/08/addressing", el.NamespaceURI(),
			"only wsa:Action and wsa:To expected without ref params, got <%s>", el.FullTag())
	}
}

func TestPullMessages_PostsToAddressFromRef(t *testing.T) {
	ref := subscriptionRef{Address: "http://camera/specific-sub-endpoint", RefParamsXML: ""}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.NotEmpty(t, fc.sendSoapCalls)
	assert.Equal(t, "http://camera/specific-sub-endpoint", fc.sendSoapCalls[0][0])
}

// --- Building the header XML from raw ref params ----------------------

func TestBuildRefParamsHeader_AddsIsReferenceParameter(t *testing.T) {
	raw := `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing">` +
		`<dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>` +
		`</wsa:ReferenceParameters>`
	got, err := buildRefParamsHeader(raw)
	require.NoError(t, err)
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.Contains(t, got, `xmlns:dom0="http://www.axis.com/2009/event"`,
		"original namespace declaration must survive")
	assert.Contains(t, got, `IsReferenceParameter="true"`)
}

func TestBuildRefParamsHeader_MultipleTopLevelChildren(t *testing.T) {
	raw := `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing">` +
		`<a:Foo xmlns:a="ns/a">1</a:Foo><b:Bar xmlns:b="ns/b">2</b:Bar>` +
		`</wsa:ReferenceParameters>`
	got, err := buildRefParamsHeader(raw)
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(got, `IsReferenceParameter="true"`),
		"attribute must be added to every top-level child, not just the first")
	assert.Contains(t, got, "Foo")
	assert.Contains(t, got, "Bar")
}

func TestBuildRefParamsHeader_EmptyInputReturnsEmpty(t *testing.T) {
	got, err := buildRefParamsHeader("")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestUnsubscribePullPoint_EchoesRefParamsWithIsReferenceParameter(t *testing.T) {
	ref := subscriptionRef{
		Address:      "http://192.168.1.10/onvif/services",
		RefParamsXML: `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing"><dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId></wsa:ReferenceParameters>`,
	}
	fc := newFakeCaller()
	require.NoError(t, unsubscribePullPoint(fc, ref))
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Contains(t, hdr, "SubscriptionId")
	assert.Contains(t, hdr, `IsReferenceParameter="true"`)
}

func TestUnsubscribePullPoint_EmptyAddressIsNoOp(t *testing.T) {
	fc := newFakeCaller()
	require.NoError(t, unsubscribePullPoint(fc, subscriptionRef{}))
	assert.Empty(t, fc.sendSoapCalls, "no SOAP call should happen when there is no subscription endpoint")
}

// End-to-end multi-child wiring through the production caller, not
// just the unit-tested builder. Without the fix to addHeaderChildren
// in Device.SendSoapWithHeader, the second child would silently
// vanish from the wire envelope.
func TestPullMessages_TwoRefParamsEachLandsOnTheWire(t *testing.T) {
	ref := subscriptionRef{
		Address: "http://camera/sub",
		RefParamsXML: `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing">` +
			`<a:Foo xmlns:a="ns/a">1</a:Foo>` +
			`<b:Bar xmlns:b="ns/b">2</b:Bar>` +
			`</wsa:ReferenceParameters>`,
	}
	fc := newFakeCaller()
	_, err := pullMessages(fc, ref, defaultOptions())
	require.NoError(t, err)
	require.Len(t, fc.sendSoapHeaders, 1)
	hdr := fc.sendSoapHeaders[0]
	assert.Equal(t, 2, strings.Count(hdr, `IsReferenceParameter="true"`))
	assert.Contains(t, hdr, "Foo")
	assert.Contains(t, hdr, "Bar")
}

// A camera echoing our request in a fault response (some debug-mode
// firmwares do) or a fault that includes the Security header verbatim
// would otherwise leak the WS-Security Username/Password into operator
// logs. The body excerpt must scrub the Security block before the
// fault extractor and the excerpt fallback see it.
func TestEnrichSOAPErr_RedactsWSSESecurityBlock(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Header><wsse:Security xmlns:wsse="x"><wsse:UsernameToken>
    <wsse:Username>admin</wsse:Username>
    <wsse:Password>hunter2</wsse:Password>
  </wsse:UsernameToken></wsse:Security></env:Header>
  <env:Body>plain text excerpt</env:Body>
</env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400"))
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "hunter2", "Password must never reach logs")
	assert.NotContains(t, got.Error(), "admin", "Username must never reach logs")
	assert.Contains(t, got.Error(), "REDACTED", "redaction marker must remain visible")
}

// Same vendor pattern as the enrichSOAPErr case but reached via
// unmarshalNode → extractSOAPFault on a 200 OK response carrying a
// Fault. Diverging from enrichSOAPErr's fallback chain would mean
// PullMessages reports "missing PullMessagesResponse element" instead
// of the actionable ter:InvalidArgs.
func TestExtractSOAPFault_FallsBackToSubcodeWhenReasonEmpty(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body><env:Fault>
    <env:Code>
      <env:Value>env:Sender</env:Value>
      <env:Subcode><env:Value>ter:InvalidArgs</env:Value></env:Subcode>
    </env:Code>
    <env:Reason><env:Text xml:lang="en"/></env:Reason>
  </env:Fault></env:Body>
</env:Envelope>`
	assert.Equal(t, "ter:InvalidArgs", extractSOAPFault(body))
}

// WS-Addressing 1.0 Core §2.1 allows ReferenceParameters in any endpoint
// reference (wsa:From, wsa:ReplyTo, wsa:FaultTo, ...). An unanchored
// search would silently pick up the wrong one.
func TestExtractReferenceParameters_AnchoredToSubscriptionReference(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Header>
    <wsa:ReplyTo>
      <wsa:Address>http://anon</wsa:Address>
      <wsa:ReferenceParameters>
        <decoy:NotTheRealOne xmlns:decoy="urn:decoy">DO-NOT-PICK</decoy:NotTheRealOne>
      </wsa:ReferenceParameters>
    </wsa:ReplyTo>
  </env:Header>
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa:Address>http://camera/sub</wsa:Address>
      <wsa:ReferenceParameters>
        <dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId>
      </wsa:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	got := extractReferenceParameters(body)
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.NotContains(t, got, "DO-NOT-PICK",
		"ref params from wsa:ReplyTo must not leak through — only SubscriptionReference's children belong on PullMessages")
}

// When a vendor declares the namespace prefix on the parent
// <ReferenceParameters> element rather than the child (legal XML, just
// different from AXIS's shape), naïve inner-only extraction strips the
// declaration and produces children with orphaned prefixes that fail
// to round-trip. Inheritance must propagate ancestor xmlns onto each
// child before serialisation.
func TestBuildRefParamsHeader_InheritsParentXmlns(t *testing.T) {
	parentScopedXmlns := `<wsa:ReferenceParameters xmlns:dom0="urn:vendor:axis">` +
		`<dom0:SubscriptionId>297</dom0:SubscriptionId>` +
		`</wsa:ReferenceParameters>`
	got, err := buildRefParamsHeader(parentScopedXmlns)
	require.NoError(t, err)
	assert.NotContains(t, got, "ReferenceParameters",
		"the wrapping element must not appear in output — each param child is its own header block")
	assert.Contains(t, got, "SubscriptionId")
	assert.Contains(t, got, "297")
	assert.Contains(t, got, `xmlns:dom0="urn:vendor:axis"`,
		"the dom0 prefix is undeclared on the child itself — it must be inherited from the parent so the standalone child stays valid XML")
	assert.Contains(t, got, `IsReferenceParameter="true"`)
}

// Pins the contract change: extractReferenceParameters returns the
// full <ReferenceParameters> element (including its own attributes),
// not just the inner content, so parent-scoped xmlns survives into
// buildRefParamsHeader.
func TestExtractReferenceParameters_IncludesParentElementForXmlnsPreservation(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference>
      <wsa:Address>http://camera</wsa:Address>
      <wsa:ReferenceParameters xmlns:dom0="urn:vendor:axis">
        <dom0:SubscriptionId>297</dom0:SubscriptionId>
      </wsa:ReferenceParameters>
    </tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	got := extractReferenceParameters(body)
	assert.Contains(t, got, "ReferenceParameters",
		"extractor must include the wrapping element so parent-scoped xmlns survives")
	assert.Contains(t, got, `xmlns:dom0="urn:vendor:axis"`)
	assert.Contains(t, got, "SubscriptionId")
}

// --- Camera-granted TerminationTime -----------------------------------
//
// Cameras may grant a shorter subscription than we ask for. Scheduling
// the next renew from opts.InitialTermination instead of what the
// camera actually granted leads to expired subscriptions and the
// recreate-recovery path firing unnecessarily.

func TestCreatePullPoint_CapturesGrantedTermination(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing"
                      xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference><wsa:Address>http://camera/sub</wsa:Address></tev:SubscriptionReference>
    <wsnt:CurrentTime>2026-05-27T13:19:11Z</wsnt:CurrentTime>
    <wsnt:TerminationTime>2026-05-27T13:21:11Z</wsnt:TerminationTime>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	expected, _ := time.Parse(time.RFC3339, "2026-05-27T13:21:11Z")
	assert.Equal(t, expected, ref.GrantedTermination)
}

func TestCreatePullPoint_NoTerminationTimeYieldsZeroTime(t *testing.T) {
	body := `<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"
                      xmlns:wsa="http://www.w3.org/2005/08/addressing"
                      xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference><wsa:Address>http://camera/sub</wsa:Address></tev:SubscriptionReference>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	fc := newFakeCaller()
	fc.queueCallMethod(body, nil)
	ref, err := createPullPoint(fc, defaultOptions())
	require.NoError(t, err)
	assert.True(t, ref.GrantedTermination.IsZero(),
		"absent TerminationTime must yield zero so renew falls back to opts")
}

func TestNextRenewInterval_UsesGrantedTerminationMinusMargin(t *testing.T) {
	now := time.Date(2026, 5, 27, 13, 0, 0, 0, time.UTC)
	granted := now.Add(60 * time.Second)
	opts := Options{InitialTermination: 60 * time.Second, RenewMargin: 10 * time.Second}
	assert.Equal(t, 50*time.Second, nextRenewInterval(granted, opts, now))
}

func TestNextRenewInterval_FallsBackToInitialTerminationWhenGrantedZero(t *testing.T) {
	now := time.Date(2026, 5, 27, 13, 0, 0, 0, time.UTC)
	opts := Options{InitialTermination: 60 * time.Second, RenewMargin: 10 * time.Second}
	assert.Equal(t, 50*time.Second, nextRenewInterval(time.Time{}, opts, now))
}

func TestNextRenewInterval_FloorsAtOneSecondIfAlreadyExpired(t *testing.T) {
	now := time.Date(2026, 5, 27, 13, 0, 0, 0, time.UTC)
	granted := now.Add(-1 * time.Second) // camera says we're already expired
	opts := Options{InitialTermination: 60 * time.Second, RenewMargin: 10 * time.Second}
	assert.Equal(t, time.Second, nextRenewInterval(granted, opts, now),
		"never sleep zero or negative — recreate-recovery handles the truly-dead case")
}

func TestBuildRefParamsHeader_MalformedXMLReturnsError(t *testing.T) {
	_, err := buildRefParamsHeader("<not-closed")
	require.Error(t, err)
}

func TestBuildRefParamsHeader_WhitespaceOnlyReturnsEmpty(t *testing.T) {
	got, err := buildRefParamsHeader("   \n\t ")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// Worst case: the response body's <Security> block starts within the
// 64 KiB error cap but its </Security> is past it. The non-greedy
// regex needs a close tag — without one the redaction misses and
// raw Username/Password reaches the excerpt. Verify the helper
// strips from <Security to EOF when no close tag is present.
func TestEnrichSOAPErr_RedactsSecurityBlockMissingCloseTag(t *testing.T) {
	body := `<env:Envelope xmlns:env="x"><env:Header>` +
		`<wsse:Security xmlns:wsse="y">` +
		`<wsse:Username>admin</wsse:Username>` +
		`<wsse:Password>hunter2</wsse:Password>` +
		// no </wsse:Security> — simulates a Security block truncated
		// at the 64 KiB read cap.
		strings.Repeat("padding ", 1000)
	got := enrichSOAPErr(fakeResponse(body), errors.New("500"))
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "hunter2",
		"truncated Security block must not leak Password to the excerpt")
	assert.NotContains(t, got.Error(), "admin",
		"truncated Security block must not leak Username to the excerpt")
}

// The wrapper-only contract means an element whose local name merely
// ends in "ReferenceParameters" cannot be mistaken for the wrapper —
// the root must be exactly <*:ReferenceParameters>. Anything else is
// a contract violation by the caller and surfaces as an error.
func TestBuildRefParamsHeader_RejectsNonReferenceParametersRoot(t *testing.T) {
	raw := `<my:MyReferenceParameters xmlns:my="urn:vendor:my">X</my:MyReferenceParameters>`
	_, err := buildRefParamsHeader(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReferenceParameters")
}

// A non-conformant camera echoing UsernameToken/Password outside a
// <Security> wrapper would still leak credentials through the body
// excerpt. Belt-and-braces: redact Password elements directly too.
func TestEnrichSOAPErr_RedactsBarePasswordElement(t *testing.T) {
	body := `<env:Envelope xmlns:env="x"><env:Body>` +
		`<wsse:UsernameToken xmlns:wsse="y">` +
		`<wsse:Username>admin</wsse:Username>` +
		`<wsse:Password>hunter2</wsse:Password>` +
		`</wsse:UsernameToken>` +
		`</env:Body></env:Envelope>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("400"))
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "hunter2",
		"Password must be redacted regardless of whether it's wrapped in Security")
}

// --- Edge-case coverage flagged in review -----------------------------

func TestExtractTerminationTime_MalformedDateYieldsZero(t *testing.T) {
	body := `<env:Body><wsnt:TerminationTime>not-a-date</wsnt:TerminationTime></env:Body>`
	assert.True(t, extractTerminationTime(body).IsZero(),
		"unparseable datetime must not panic and must not return a garbage time — fall back to opts")
}

func TestNextRenewInterval_MarginEqualsBaseFallsToHalf(t *testing.T) {
	now := time.Date(2026, 5, 27, 13, 0, 0, 0, time.UTC)
	opts := Options{InitialTermination: 30 * time.Second, RenewMargin: 30 * time.Second}
	got := nextRenewInterval(time.Time{}, opts, now)
	assert.Equal(t, 15*time.Second, got,
		"when margin == base, the helper must fall through to base/2 rather than the 1s floor")
}

func TestExtractReferenceParameters_EmptySubscriptionReferenceReturnsEmpty(t *testing.T) {
	body := `<env:Envelope xmlns:env="x" xmlns:tev="y">
  <env:Body><tev:CreatePullPointSubscriptionResponse>
    <tev:SubscriptionReference/>
  </tev:CreatePullPointSubscriptionResponse></env:Body>
</env:Envelope>`
	assert.Empty(t, extractReferenceParameters(body))
}

func TestEnrichSOAPErr_RedactsTruncatedPasswordOutsideSecurity(t *testing.T) {
	body := `<env:Envelope xmlns:env="x"><env:Body>` +
		`<wsse:UsernameToken xmlns:wsse="y">` +
		`<wsse:Username>admin</wsse:Username>` +
		`<wsse:Password>hunter2` // truncated — no </Password>, no </UsernameToken>, no </Envelope>
	got := enrichSOAPErr(fakeResponse(body), errors.New("500"))
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "hunter2",
		"Password without a closing tag (truncated at cap) must still be redacted")
}

func TestEnrichSOAPErr_RedactsMultipleSecurityBlocks(t *testing.T) {
	body := `<E>` +
		`<wsse:Security xmlns:wsse="y"><wsse:Password>secret1</wsse:Password></wsse:Security>` +
		`<wsse:Security xmlns:wsse="y"><wsse:Password>secret2</wsse:Password></wsse:Security>` +
		`</E>`
	got := enrichSOAPErr(fakeResponse(body), errors.New("500"))
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "secret1")
	assert.NotContains(t, got.Error(), "secret2",
		"ReplaceAllString must catch every Security block, not just the first")
}

func TestBuildRefParamsHeader_RejectsNonReferenceParametersRoot_Table(t *testing.T) {
	cases := []struct {
		name, raw string
	}{
		{"vendor suffix", `<my:MyReferenceParameters xmlns:my="urn:x">X</my:MyReferenceParameters>`},
		{"multi-root", `<a:Foo xmlns:a="ns/a"/><b:Bar xmlns:b="ns/b"/>`},
		{"unrelated element", `<not-the-wrapper>content</not-the-wrapper>`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := buildRefParamsHeader(c.raw)
			require.Error(t, err)
		})
	}
}

// --- WS-Addressing Action and To --------------------------------------

// Some devices identify the subscription from wsa:To rather than the
// request URL, and reject PullMessages and Unsubscribe without it as
// NotAuthorized. Echoing ref params alone does not satisfy them.

// Pinned as literals rather than reusing the production constants, so a
// typo in an action URI fails here instead of passing on both sides.
const (
	wantPullMessagesAction = "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/PullMessagesRequest"
	wantRenewAction        = "http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/RenewRequest"
	wantUnsubscribeAction  = "http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/UnsubscribeRequest"
)

type subscriptionRequest struct {
	name   string
	action string
	send   func(c caller, ref subscriptionRef) error
}

func subscriptionRequests() []subscriptionRequest {
	return []subscriptionRequest{
		{"PullMessages", wantPullMessagesAction, func(c caller, ref subscriptionRef) error {
			_, err := pullMessages(c, ref, defaultOptions())
			return err
		}},
		{"Renew", wantRenewAction, func(c caller, ref subscriptionRef) error {
			_, err := renewPullPoint(c, ref, defaultOptions())
			return err
		}},
		{"Unsubscribe", wantUnsubscribeAction, func(c caller, ref subscriptionRef) error {
			return unsubscribePullPoint(c, ref)
		}},
	}
}

// headerElements parses captured header XML into its top-level
// elements, failing the test if it is not well-formed.
func headerElements(t *testing.T, headerXML string) []*etree.Element {
	t.Helper()
	doc := etree.NewDocument()
	require.NoError(t, doc.ReadFromString("<wrap>"+headerXML+"</wrap>"), "header must be well-formed XML: %s", headerXML)
	return doc.SelectElement("wrap").ChildElements()
}

func wsaHeaders(t *testing.T, headerXML, local string) []*etree.Element {
	t.Helper()
	var out []*etree.Element
	for _, el := range headerElements(t, headerXML) {
		if el.Tag == local && el.NamespaceURI() == "http://www.w3.org/2005/08/addressing" {
			out = append(out, el)
		}
	}
	return out
}

func TestSubscriptionRequests_SendWSAddressingActionAndTo(t *testing.T) {
	for _, req := range subscriptionRequests() {
		t.Run(req.name, func(t *testing.T) {
			ref := subscriptionRef{Address: "http://camera/onvif/Subscription?Idx=7"}
			fc := newFakeCaller()
			require.NoError(t, req.send(fc, ref))
			require.Len(t, fc.sendSoapHeaders, 1)
			hdr := fc.sendSoapHeaders[0]

			action := wsaHeaders(t, hdr, "Action")
			require.Len(t, action, 1, "exactly one wsa:Action; got: %s", hdr)
			assert.Equal(t, req.action, action[0].Text())

			to := wsaHeaders(t, hdr, "To")
			require.Len(t, to, 1, "exactly one wsa:To; got: %s", hdr)
			assert.Equal(t, ref.Address, to[0].Text(), "wsa:To must name the subscription the request is posted to")
		})
	}
}

// Subscription addresses routinely carry query strings; an unescaped
// '&' makes the whole envelope malformed and the request is never sent.
func TestSubscriptionRequests_EscapeAddressInTo(t *testing.T) {
	for _, req := range subscriptionRequests() {
		t.Run(req.name, func(t *testing.T) {
			ref := subscriptionRef{Address: "http://camera/onvif/Subscription?Idx=7&Ch=<1>"}
			fc := newFakeCaller()
			require.NoError(t, req.send(fc, ref))
			require.Len(t, fc.sendSoapHeaders, 1)

			to := wsaHeaders(t, fc.sendSoapHeaders[0], "To")
			require.Len(t, to, 1)
			assert.Equal(t, ref.Address, to[0].Text(), "address must round-trip through escaping unchanged")
		})
	}
}

func TestSubscriptionRequests_KeepRefParamsAlongsideAddressing(t *testing.T) {
	for _, req := range subscriptionRequests() {
		t.Run(req.name, func(t *testing.T) {
			ref := subscriptionRef{
				Address:      "http://192.168.1.10/onvif/services",
				RefParamsXML: `<wsa:ReferenceParameters xmlns:wsa="http://www.w3.org/2005/08/addressing"><dom0:SubscriptionId xmlns:dom0="http://www.axis.com/2009/event">297</dom0:SubscriptionId></wsa:ReferenceParameters>`,
			}
			fc := newFakeCaller()
			require.NoError(t, req.send(fc, ref))
			require.Len(t, fc.sendSoapHeaders, 1)
			hdr := fc.sendSoapHeaders[0]

			var refParam *etree.Element
			for _, el := range headerElements(t, hdr) {
				if el.Tag == "SubscriptionId" {
					refParam = el
				}
			}
			require.NotNil(t, refParam, "ref param must still be echoed; got: %s", hdr)
			assert.Equal(t, "297", refParam.Text())
			assert.Equal(t, "true", refParam.SelectAttrValue("wsa:IsReferenceParameter", ""))
		})
	}
}

// End-to-end through Device.SendSoapWithHeader and gosoap, which the
// fakeCaller tests bypass: the envelope builder must not drop or
// duplicate the headers, and the digest retry must resend them.
func TestSubscriptionRequests_OnTheWireCarryOneActionAndTo(t *testing.T) {
	for _, challenge := range []bool{false, true} {
		for _, req := range subscriptionRequests() {
			name := req.name
			if challenge {
				name += "/after_digest_challenge"
			}
			t.Run(name, func(t *testing.T) {
				body := sendThroughDevice(t, req, challenge)

				doc := etree.NewDocument()
				require.NoError(t, doc.ReadFromString(body), "envelope must be well-formed: %s", body)
				header := doc.Root().SelectElement("Header")
				require.NotNil(t, header, "envelope must carry a Header: %s", body)

				var actions, tos []*etree.Element
				for _, el := range header.ChildElements() {
					if el.NamespaceURI() != "http://www.w3.org/2005/08/addressing" {
						continue
					}
					switch el.Tag {
					case "Action":
						actions = append(actions, el)
					case "To":
						tos = append(tos, el)
					}
				}
				require.Len(t, actions, 1, "exactly one wsa:Action in the sent envelope: %s", body)
				assert.Equal(t, req.action, actions[0].Text())
				require.Len(t, tos, 1, "exactly one wsa:To in the sent envelope: %s", body)
				assert.True(t, strings.HasSuffix(tos[0].Text(), "/sub?a=1&b=2"), "wsa:To = %q", tos[0].Text())
			})
		}
	}
}

// sendThroughDevice sends req via a real Device to an httptest camera
// and returns the envelope of the request that reached the
// subscription. With challenge set, the camera rejects every request
// without an Authorization header, so only the digest retry is captured.
func sendThroughDevice(t *testing.T, req subscriptionRequest, challenge bool) string {
	t.Helper()
	var mu sync.Mutex
	var captured string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sub" {
			w.WriteHeader(http.StatusOK) // GetCapabilities / GetServices at construction
			return
		}
		if challenge && r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="onvif", nonce="abc", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		captured = string(b)
		mu.Unlock()
		_, _ = io.WriteString(w, pullMessagesResp())
	}))
	t.Cleanup(srv.Close)

	dev, err := onvif.NewDevice(onvif.DeviceParams{
		Xaddr:      strings.TrimPrefix(srv.URL, "http://"),
		Username:   "admin",
		Password:   "secret",
		HttpClient: srv.Client(),
	})
	require.NoError(t, err)

	require.NoError(t, req.send(deviceCaller{dev: dev}, subscriptionRef{Address: srv.URL + "/sub?a=1&b=2"}))
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, captured, "request never reached the subscription endpoint")
	return captured
}
