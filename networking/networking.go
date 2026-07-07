package networking

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/beevik/etree"
	"github.com/juju/errors"
)

const soapContentType = "application/soap+xml; charset=utf-8"

// SendSoap send soap message
func SendSoap(httpClient *http.Client, endpoint, message string) (*http.Response, error) {
	resp, err := httpClient.Post(endpoint, soapContentType, bytes.NewBufferString(message))
	if err != nil {
		return resp, errors.Annotate(err, "Post")
	}

	// if resp.StatusCode is 4xx,5xx, return error
	if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		return resp, errors.Errorf("Server error: %d: %s", resp.StatusCode, resp.Status)
	}

	return resp, nil
}

// SendSoapWithDigest sends a soap message and, when the device answers with an
// HTTP 401 digest challenge, transparently retries the request with the
// computed HTTP digest Authorization header.
//
// Any wsse:Security header present in the message is stripped before sending:
// when a device requires HTTP digest the credentials travel in the
// Authorization header, so keeping the WS-Security UsernameToken in the body
// would put the credentials on the wire twice. Other SOAP header blocks (for
// example WS-Addressing reference parameters) are preserved so vendor-specific
// routing keeps working across the retry.
func SendSoapWithDigest(httpClient *http.Client, endpoint, message, username, password string) (*http.Response, error) {
	if httpClient == nil {
		httpClient = new(http.Client)
	}

	// Avoid sending the credentials twice (WS-Security + digest) on the retry.
	message = stripWSSecurityHeader(message)

	resp, err := httpClient.Post(endpoint, soapContentType, bytes.NewBufferString(message))
	if err != nil {
		return resp, errors.Annotate(err, "Post")
	}

	// Only escalate to HTTP digest when the device explicitly asks for it.
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	challenge := resp.Header.Get("WWW-Authenticate")
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(challenge)), "digest") {
		// Not a digest challenge (e.g. Basic) - nothing more we can do here.
		return resp, nil
	}

	authorization := newDigestAuthorization(challenge, http.MethodPost, endpoint, username, password)
	if authorization == "" {
		return resp, errors.New("unsupported digest challenge")
	}

	// Release the challenge response before issuing the authenticated retry.
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(message))
	if err != nil {
		return nil, errors.Annotate(err, "new digest request")
	}
	req.Header.Set("Content-Type", soapContentType)
	req.Header.Set("Authorization", authorization)

	resp, err = httpClient.Do(req)
	if err != nil {
		return resp, errors.Annotate(err, "Post with digest")
	}

	return resp, nil
}

// stripWSSecurityHeader removes the wsse:Security header block from a SOAP
// envelope, leaving all other header blocks intact. The message is returned
// unchanged if it cannot be parsed as XML or has no such header.
func stripWSSecurityHeader(message string) string {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(message); err != nil {
		return message
	}
	security := doc.FindElement("./Envelope/Header/Security")
	if security == nil {
		return message
	}
	header := doc.Root().SelectElement("Header")
	if header == nil {
		return message
	}
	header.RemoveChild(security)
	data, err := doc.WriteToString()
	if err != nil {
		return message
	}
	return data
}

var digestParamRe = regexp.MustCompile(`(\w+)=(?:"([^"]*)"|([^,]+))`)

// parseDigestChallenge parses the parameters of a WWW-Authenticate: Digest header.
func parseDigestChallenge(challenge string) map[string]string {
	challenge = strings.TrimSpace(challenge)
	if i := strings.IndexAny(challenge, " \t"); i >= 0 && strings.EqualFold(challenge[:i], "Digest") {
		challenge = challenge[i+1:]
	}

	result := make(map[string]string)
	for _, m := range digestParamRe.FindAllStringSubmatch(challenge, -1) {
		value := m[2]
		if value == "" {
			value = m[3]
		}
		result[strings.ToLower(m[1])] = strings.TrimSpace(value)
	}
	return result
}

// newDigestAuthorization builds an RFC 2617 HTTP digest Authorization header
// value. It supports the MD5 and MD5-sess algorithms and the "auth" qop, which
// covers the vast majority of ONVIF devices. It returns an empty string when the
// challenge is missing required parameters or requires an unsupported qop.
func newDigestAuthorization(challenge, method, uri, username, password string) string {
	parts := parseDigestChallenge(challenge)
	realm := parts["realm"]
	nonce := parts["nonce"]
	if realm == "" || nonce == "" {
		return ""
	}
	opaque := parts["opaque"]
	algorithm := parts["algorithm"]

	qop := ""
	if rawQop, ok := parts["qop"]; ok {
		for _, candidate := range strings.Split(rawQop, ",") {
			if strings.TrimSpace(candidate) == "auth" {
				qop = "auth"
				break
			}
		}
		// The server offered qop but none we support (e.g. auth-int only).
		if qop == "" {
			return ""
		}
	}

	digestURI := uri
	if u, err := url.Parse(uri); err == nil {
		digestURI = u.RequestURI()
	}

	cnonce := randomCnonce()
	const nc = "00000001"

	ha1 := md5Hex(username + ":" + realm + ":" + password)
	if strings.EqualFold(algorithm, "MD5-sess") {
		ha1 = md5Hex(ha1 + ":" + nonce + ":" + cnonce)
	}
	ha2 := md5Hex(method + ":" + digestURI)

	var response string
	if qop == "auth" {
		response = md5Hex(strings.Join([]string{ha1, nonce, nc, cnonce, qop, ha2}, ":"))
	} else {
		response = md5Hex(ha1 + ":" + nonce + ":" + ha2)
	}

	var b strings.Builder
	fmt.Fprintf(&b, `Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s"`,
		username, realm, nonce, digestURI, response)
	if qop == "auth" {
		fmt.Fprintf(&b, `, qop=auth, nc=%s, cnonce="%s"`, nc, cnonce)
	}
	if algorithm != "" {
		fmt.Fprintf(&b, `, algorithm=%s`, algorithm)
	}
	if opaque != "" {
		fmt.Fprintf(&b, `, opaque="%s"`, opaque)
	}
	return b.String()
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func randomCnonce() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(b)
}
