package networking

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSendSoapWithDigestReturnsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	response, err := SendSoapWithDigest(server.Client(), server.URL, "<Envelope/>", "user", "password")
	if response == nil {
		t.Fatal("SendSoapWithDigest returned a nil response")
	}
	defer response.Body.Close()
	if err == nil {
		t.Fatal("SendSoapWithDigest returned nil error for HTTP 500")
	}
}

func TestSendSoapWithDigestReturnsServerErrorAfterAuthentication(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		if requestCount == 1 {
			writer.Header().Set("WWW-Authenticate", `Digest realm="AXIS", nonce="nonce", qop="auth", algorithm=MD5`)
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Digest ") {
			t.Error("authenticated retry is missing Digest Authorization header")
		}
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	response, err := SendSoapWithDigest(server.Client(), server.URL, "<Envelope/>", "user", "password")
	if response == nil {
		t.Fatal("SendSoapWithDigest returned a nil response")
	}
	defer response.Body.Close()
	if err == nil {
		t.Fatal("SendSoapWithDigest returned nil error for authenticated HTTP 500")
	}
	if requestCount != 2 {
		t.Fatalf("request count = %d, want 2", requestCount)
	}
}

func TestParseDigestChallenge(t *testing.T) {
	challenge := `Digest realm="testrealm@host.com", qop="auth,auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", opaque="5ccc069c403ebaf9f0171e9517f40e41", algorithm=MD5`
	parts := parseDigestChallenge(challenge)

	cases := map[string]string{
		"realm":     "testrealm@host.com",
		"qop":       "auth,auth-int",
		"nonce":     "dcd98b7102dd2f0e8b11d0f600bfb0c093",
		"opaque":    "5ccc069c403ebaf9f0171e9517f40e41",
		"algorithm": "MD5",
	}
	for key, want := range cases {
		if got := parts[key]; got != want {
			t.Errorf("parseDigestChallenge()[%q] = %q, want %q", key, got, want)
		}
	}
}

// TestMD5DigestVectors validates md5Hex and the response assembly order against
// the canonical RFC 2617 section 3.5 worked example.
func TestMD5DigestVectors(t *testing.T) {
	ha1 := md5Hex("Mufasa:testrealm@host.com:Circle Of Life")
	if want := "939e7578ed9e3c518a452acee763bce9"; ha1 != want {
		t.Fatalf("HA1 = %q, want %q", ha1, want)
	}

	ha2 := md5Hex("GET:/dir/index.html")
	if want := "39aff3a2bab6126f332b942af96d3366"; ha2 != want {
		t.Fatalf("HA2 = %q, want %q", ha2, want)
	}

	response := md5Hex(strings.Join([]string{
		ha1,
		"dcd98b7102dd2f0e8b11d0f600bfb0c093", // nonce
		"00000001",                           // nc
		"0a4f113b",                           // cnonce
		"auth",                               // qop
		ha2,
	}, ":"))
	if want := "6629fae49393a05397450978507c4ef1"; response != want {
		t.Fatalf("response = %q, want %q", response, want)
	}
}

// TestNewDigestAuthorizationConsistency builds an Authorization header and then
// recomputes the response from the emitted cnonce/nc to confirm the header is
// internally consistent (correct field wiring and formula).
func TestNewDigestAuthorizationConsistency(t *testing.T) {
	const (
		username = "admin"
		password = "s3cret"
		realm    = "IP Camera"
		nonce    = "0cc175b9c0f1b6a831c399e269772661"
	)
	challenge := `Digest realm="` + realm + `", nonce="` + nonce + `", qop="auth", algorithm=MD5`

	header := newDigestAuthorization(challenge, "POST", "http://192.168.1.10/onvif/ptz_service", username, password)
	if header == "" {
		t.Fatal("newDigestAuthorization returned empty header")
	}
	if !strings.HasPrefix(header, "Digest ") {
		t.Fatalf("header does not start with Digest scheme: %q", header)
	}

	fields := parseDigestChallenge(header)
	if fields["username"] != username {
		t.Errorf("username = %q, want %q", fields["username"], username)
	}
	if fields["uri"] != "/onvif/ptz_service" {
		t.Errorf("uri = %q, want %q", fields["uri"], "/onvif/ptz_service")
	}
	if fields["qop"] != "auth" {
		t.Errorf("qop = %q, want %q", fields["qop"], "auth")
	}

	ha1 := md5Hex(username + ":" + realm + ":" + password)
	ha2 := md5Hex("POST:/onvif/ptz_service")
	want := md5Hex(strings.Join([]string{ha1, nonce, fields["nc"], fields["cnonce"], "auth", ha2}, ":"))
	if fields["response"] != want {
		t.Errorf("response = %q, want %q", fields["response"], want)
	}
}

// TestNewDigestAuthorizationRejectsUnsupported ensures an empty header is
// returned when required parameters are missing or the qop is unsupported.
func TestNewDigestAuthorizationRejectsUnsupported(t *testing.T) {
	if got := newDigestAuthorization(`Digest realm="r"`, "POST", "http://host/x", "u", "p"); got != "" {
		t.Errorf("expected empty header when nonce missing, got %q", got)
	}
	if got := newDigestAuthorization(`Digest realm="r", nonce="n", qop="auth-int"`, "POST", "http://host/x", "u", "p"); got != "" {
		t.Errorf("expected empty header for unsupported qop, got %q", got)
	}
}
