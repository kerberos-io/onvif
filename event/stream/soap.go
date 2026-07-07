package stream

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/beevik/etree"
	"github.com/kerberos-io/onvif/event"
	"github.com/kerberos-io/onvif/xsd"
)

// maxResponseBytes caps SOAP response buffering on success paths.
// ONVIF PullMessages bodies are normally <100KB even with dense
// analytics payloads; 10 MiB is well above legitimate traffic while
// keeping a hostile or buggy camera from OOMing the process.
const maxResponseBytes = 10 << 20

// maxErrorBodyBytes caps the body read by enrichSOAPErr. The pull
// retry loop runs every RetryBackoff (~1s) so an unbounded read on
// the error path would churn 10 MiB/s per wedged camera. Fault bodies
// are always small.
const maxErrorBodyBytes = 64 << 10

func createPullPoint(c caller, opts Options) (subscriptionRef, error) {
	term := xsd.String(durationToXSD(opts.InitialTermination))
	req := event.CreatePullPointSubscription{InitialTerminationTime: &term}
	if opts.RawTopicFilter != "" {
		req.Filter = &event.FilterType{
			TopicExpression: &event.TopicExpressionType{
				Dialect:    xsd.String("http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet"),
				TopicKinds: xsd.String(opts.RawTopicFilter),
			},
		}
	}
	resp, err := c.CallMethod(req)
	if err != nil {
		return subscriptionRef{}, enrichSOAPErr(resp, err)
	}
	body, err := readClose(resp)
	if err != nil {
		return subscriptionRef{}, err
	}
	var decoded event.CreatePullPointSubscriptionResponse
	if err := unmarshalNode(body, "CreatePullPointSubscriptionResponse", &decoded); err != nil {
		return subscriptionRef{}, err
	}
	addr := string(decoded.SubscriptionReference.Address)
	if addr == "" {
		return subscriptionRef{}, errors.New("CreatePullPointSubscription response has empty SubscriptionReference Address")
	}
	return subscriptionRef{
		Address:            addr,
		RefParamsXML:       extractReferenceParameters(body),
		GrantedTermination: extractTerminationTime(body),
	}, nil
}

// extractTerminationTime parses the absolute UTC instant the camera
// granted as the subscription expiry. Returns zero on absence or parse
// failure — callers fall back to opts.InitialTermination.
func extractTerminationTime(body string) time.Time {
	m := terminationTimeRE.FindStringSubmatch(body)
	if len(m) < 2 {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(m[1]))
	if err != nil {
		return time.Time{}
	}
	return t
}

// terminationTimeRE matches the first <*:TerminationTime> in the body.
// Only safe on responses that contain exactly one — currently
// CreatePullPointSubscriptionResponse and RenewResponse via
// extractTerminationTime. PullMessagesResponse also has a
// TerminationTime element; do not call extractTerminationTime on pull
// bodies.
var terminationTimeRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?TerminationTime\b[^>]*>(.*?)</(?:[^:>\s]+:)?TerminationTime>`)

// pullMessages returns an empty slice (no error) when the camera had
// nothing within PullTimeout.
func pullMessages(c caller, ref subscriptionRef, opts Options) ([]event.NotificationMessage, error) {
	req := event.PullMessages{
		Timeout:      xsd.Duration(durationToXSD(opts.PullTimeout)),
		MessageLimit: xsd.Int(opts.MessageLimit),
	}
	body, err := xml.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal PullMessages: %w", err)
	}
	headerXML, err := buildRefParamsHeader(ref.RefParamsXML)
	if err != nil {
		return nil, fmt.Errorf("build ref params header: %w", err)
	}
	resp, err := c.SendSoapWithHeader(ref.Address, string(body), headerXML)
	if err != nil {
		return nil, enrichSOAPErr(resp, err)
	}
	respBody, err := readClose(resp)
	if err != nil {
		return nil, err
	}
	var decoded event.PullMessagesResponse
	if err := unmarshalNode(respBody, "PullMessagesResponse", &decoded); err != nil {
		return nil, err
	}
	return decoded.NotificationMessage, nil
}

// unsubscribePullPoint is best-effort. Empty Address is a no-op
// (construction failed before installing a subscription).
func unsubscribePullPoint(c caller, ref subscriptionRef) error {
	if ref.Address == "" {
		return nil
	}
	body, err := xml.Marshal(event.Unsubscribe{})
	if err != nil {
		return fmt.Errorf("marshal Unsubscribe: %w", err)
	}
	headerXML, err := buildRefParamsHeader(ref.RefParamsXML)
	if err != nil {
		return fmt.Errorf("build ref params header: %w", err)
	}
	resp, err := c.SendSoapWithHeader(ref.Address, string(body), headerXML)
	if err != nil {
		return enrichSOAPErr(resp, err)
	}
	_, err = readClose(resp)
	return err
}

func readClose(resp *http.Response) (string, error) {
	if resp == nil || resp.Body == nil {
		return "", errors.New("nil HTTP response")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}
	return string(b), nil
}

// unmarshalNode finds the first XML start element with the given local
// name and decodes it into out. ONVIF SOAP responses are wrapped in an
// envelope with many namespace prefixes; keying on local name only
// sidesteps namespace matching.
//
// When the camera returns a SOAP Fault, the fault reason is returned
// as the error so callers can distinguish auth / expired-subscription
// from "unparseable response".
func unmarshalNode(body, localName string, out any) error {
	if reason := extractSOAPFault(body); reason != "" {
		return fmt.Errorf("ONVIF SOAP fault: %s", reason)
	}
	dec := xml.NewDecoder(bytes.NewBufferString(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("ONVIF response missing %s element", localName)
			}
			return fmt.Errorf("scan ONVIF response: %w", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name.Local != localName {
			continue
		}
		if err := dec.DecodeElement(out, &start); err != nil {
			return fmt.Errorf("decode %s: %w", localName, err)
		}
		return nil
	}
}

var (
	// SOAP 1.1: <faultstring>reason</faultstring>
	soap11FaultRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?faultstring[^>]*>(.*?)</(?:[^:>\s]+:)?faultstring>`)
	// SOAP 1.2: <Fault>...<Reason><Text>reason</Text></Reason>...</Fault>
	soap12FaultRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Reason\b[^>]*>.*?<(?:[^:>\s]+:)?Text[^>]*>(.*?)</(?:[^:>\s]+:)?Text>`)
	// SOAP 1.2 Subcode: <Code>...<Subcode><Value>ter:InvalidArgs</Value></Subcode>...
	soap12SubcodeRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Subcode\b[^>]*>.*?<(?:[^:>\s]+:)?Value[^>]*>(.*?)</(?:[^:>\s]+:)?Value>`)

	// WS-Security blocks may carry our Username/Password if the camera
	// echoes the request in a fault; scrub before logging. The
	// alternation handles the truncated case where the read cap fell
	// between <Security> and </Security>: in that case nothing past
	// the opening tag is safe to retain — the replacement re-emits a
	// synthetic close tag, dropping the remainder of the body excerpt
	// (max-redact preferred to max-context for log lines).
	// wssePasswordRE is the belt-and-braces fallback for
	// non-conformant cameras emitting Password / UsernameToken outside
	// a Security wrapper; same truncation handling.
	wsseSecurityRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Security\b[^>]*>(?:.*?</(?:[^:>\s]+:)?Security>|.*)`)
	wssePasswordRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?Password\b[^>]*>(?:.*?</(?:[^:>\s]+:)?Password>|.*)`)
)

// extractSOAPFault returns the reason text from a SOAP fault, falling
// back to the Subcode value when Reason/Text is empty (AXIS pattern).
// Returns "" when the body is not a fault.
func extractSOAPFault(body string) string {
	if !strings.Contains(body, "Fault") {
		return ""
	}
	if m := soap11FaultRE.FindStringSubmatch(body); len(m) > 1 {
		if r := strings.TrimSpace(m[1]); r != "" {
			return r
		}
	}
	if m := soap12FaultRE.FindStringSubmatch(body); len(m) > 1 {
		if r := strings.TrimSpace(m[1]); r != "" {
			return r
		}
	}
	return extractSOAPSubcode(body)
}

// Anchored to SubscriptionReference because other WS-Addressing
// endpoint references in the same envelope (wsa:ReplyTo, wsa:FaultTo,
// wsa:From) may also carry ReferenceParameters that are not ours.
var (
	subscriptionRefRE = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?SubscriptionReference\b[^>]*>(.*?)</(?:[^:>\s]+:)?SubscriptionReference>`)
	refParamsRE       = regexp.MustCompile(`(?s)<(?:[^:>\s]+:)?ReferenceParameters\b[^>]*>(.*?)</(?:[^:>\s]+:)?ReferenceParameters>`)
)

// buildRefParamsHeader produces the SOAP <Header> inner XML from the
// full <*:ReferenceParameters> element returned by
// extractReferenceParameters. Each child element is re-emitted with
// wsa:IsReferenceParameter="true" added and any xmlns:* declared on
// the parent inherited onto it (so the standalone child stays valid).
// Empty input yields empty output.
func buildRefParamsHeader(refParamsXML string) (string, error) {
	if strings.TrimSpace(refParamsXML) == "" {
		return "", nil
	}
	doc := etree.NewDocument()
	if err := doc.ReadFromString(refParamsXML); err != nil {
		return "", fmt.Errorf("parse ref params: %w", err)
	}
	wrapper := doc.Root()
	if wrapper == nil {
		return "", errors.New("ref params has no root element")
	}
	if wrapper.Tag != "ReferenceParameters" {
		return "", fmt.Errorf("ref params root must be <*:ReferenceParameters>, got <%s>", wrapper.Tag)
	}
	var out strings.Builder
	for _, child := range wrapper.ChildElements() {
		c := child.Copy()
		inheritXmlns(c, wrapper)
		c.CreateAttr("wsa:IsReferenceParameter", "true")
		d := etree.NewDocument()
		d.SetRoot(c)
		s, err := d.WriteToString()
		if err != nil {
			return "", fmt.Errorf("serialise ref param child: %w", err)
		}
		out.WriteString(strings.TrimRight(s, "\n"))
	}
	return out.String(), nil
}

// inheritXmlns copies xmlns / xmlns:* declarations from src onto dst
// when dst doesn't already declare them, so a child whose namespace
// prefix was declared on an ancestor stays valid in isolation.
func inheritXmlns(dst, src *etree.Element) {
	for _, attr := range src.Attr {
		isDefault := attr.Space == "" && attr.Key == "xmlns"
		isPrefixed := attr.Space == "xmlns"
		if !isDefault && !isPrefixed {
			continue
		}
		key := attr.Key
		if isPrefixed {
			key = "xmlns:" + attr.Key
		}
		if dst.SelectAttr(key) != nil {
			continue
		}
		dst.CreateAttr(key, attr.Value)
	}
}

// extractReferenceParameters returns the verbatim inner XML so callers
// can echo it (with wsa:IsReferenceParameter="true") into the SOAP
// Header of subscription-scoped requests per WS-Addressing 1.0 SOAP Binding §3.4.
// Without that echo, AXIS rejects PullMessages with ter:InvalidArgs.
func extractReferenceParameters(body string) string {
	sub := subscriptionRefRE.FindStringSubmatch(body)
	if len(sub) < 2 {
		return ""
	}
	return strings.TrimSpace(refParamsRE.FindString(sub[1]))
}

// extractSOAPSubcode is the fallback when Reason/Text is empty — AXIS
// routinely sends an empty <Text/> alongside a populated Subcode
// (e.g. "ter:InvalidArgs"), and that subcode is the only actionable
// signal the operator gets.
func extractSOAPSubcode(body string) string {
	m := soap12SubcodeRE.FindStringSubmatch(body)
	if len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// maxErrExcerpt caps the body excerpt appended to an enriched error so
// a wedged camera streaming a multi-megabyte HTML error page can not
// flood logs with every retry.
const maxErrExcerpt = 512

// enrichSOAPErr appends the camera's actual complaint (SOAP Fault
// reason, then Subcode, then raw body excerpt) to a transport error so
// operators see *why* the camera said 400 instead of just "400 Bad
// Request". The original err is preserved via %w for errors.Is/As.
func enrichSOAPErr(resp *http.Response, err error) error {
	if err == nil {
		return nil
	}
	if resp == nil || resp.Body == nil {
		return err
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if readErr != nil || len(b) == 0 {
		return err
	}
	body := wsseSecurityRE.ReplaceAllString(string(b), "<Security>[REDACTED]</Security>")
	body = wssePasswordRE.ReplaceAllString(body, "<Password>[REDACTED]</Password>")
	if reason := extractSOAPFault(body); reason != "" {
		return fmt.Errorf("SOAP fault: %s: %w", reason, err)
	}
	excerpt := strings.TrimSpace(body)
	if len(excerpt) > maxErrExcerpt {
		excerpt = excerpt[:maxErrExcerpt] + "...(truncated)"
	}
	return fmt.Errorf("response body: %s: %w", excerpt, err)
}

// durationToXSD formats a duration as xsd:duration PTnS. Second
// precision is sufficient — ONVIF cameras do not honour sub-second
// pull timeouts.
func durationToXSD(d time.Duration) string {
	secs := int(d.Round(time.Second).Seconds())
	if secs <= 0 {
		secs = 1
	}
	return "PT" + strconv.Itoa(secs) + "S"
}
