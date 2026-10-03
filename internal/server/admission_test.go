package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func registerBodyWithQuota(endpoint, version, zone string, weight, ttl, maxConcurrency int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"maxConcurrency":%d}`,
		endpoint, version, zone, weight, ttl, maxConcurrency)
}

func acquire(t *testing.T, h http.Handler, service, body string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPost, "/v1/admission/"+service+"/acquire", body, nil)
}

func acquireOK(t *testing.T, h http.Handler, service, body string) acquireResponse {
	t.Helper()
	rec := acquire(t, h, service, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("acquire = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	requireJSON(t, rec)
	if strings.Contains(rec.Body.String(), "leaseToken") {
		t.Fatalf("acquire leaks lease token: %s", rec.Body.String())
	}
	var resp acquireResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("acquire response is not JSON: %v", err)
	}
	return resp
}

func releasePermit(t *testing.T, h http.Handler, service, token string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodDelete, "/v1/admission/"+service+"/permits/"+token, "", nil)
}

func TestRegisterMaxConcurrencyEchoed(t *testing.T) {
	h := Handler()
	rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i1",
		registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 5), nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", rec.Code)
	}
	var resp registerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("register response is not JSON: %v", err)
	}
	if resp.MaxConcurrency == nil || *resp.MaxConcurrency != 5 {
		t.Fatalf("maxConcurrency = %v, want 5", resp.MaxConcurrency)
	}
	// The value rides along in discovery and resolve views too.
	rec = do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)
	if !strings.Contains(rec.Body.String(), `"maxConcurrency":5`) {
		t.Fatalf("discovery missing maxConcurrency: %s", rec.Body.String())
	}
	rec = do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil)
	if !strings.Contains(rec.Body.String(), `"maxConcurrency":5`) {
		t.Fatalf("resolve missing maxConcurrency: %s", rec.Body.String())
	}

	// Omitted means unlimited, and the field stays out of the public view.
	rec = do(t, h, http.MethodPut, "/v1/services/svc/instances/i2",
		registerBody("http://b:1", "v1", "z1", 10, 60), nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register without quota = %d, want 201", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "maxConcurrency") {
		t.Fatalf("unlimited instance exposes maxConcurrency: %s", rec.Body.String())
	}
}

func TestRegisterMaxConcurrencyValidation(t *testing.T) {
	h := Handler()
	for _, value := range []string{"0", "-1", "10001", "1.5", `"5"`, "true"} {
		body := fmt.Sprintf(`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"maxConcurrency":%s}`, value)
		rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i1", body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("maxConcurrency=%s = %d (%s), want 400 validation_error", value, rec.Code, errorCode(t, rec))
		}
	}
	for _, value := range []int{1, 10000} {
		code, _ := mustRegister(t, h, "svc", fmt.Sprintf("i%d", value),
			registerBodyWithQuota("http://a:1", "v1", "z1", 10, 30, value))
		if code != http.StatusCreated {
			t.Fatalf("maxConcurrency=%d = %d, want 201", value, code)
		}
	}
}

func TestAcquireAndReleaseLifecycle(t *testing.T) {
	h := Handler()
	before := time.Now()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 2))

	first := acquireOK(t, h, "svc", `{"key":"user-1","permitTTLSeconds":30}`)
	if first.Service != "svc" || first.Key != "user-1" {
		t.Fatalf("service/key not echoed: %+v", first)
	}
	if first.Instance.Instance != "i1" || first.Instance.Endpoint != "http://a:1" {
		t.Fatalf("unexpected instance view: %+v", first.Instance)
	}
	if first.Instance.MaxConcurrency == nil || *first.Instance.MaxConcurrency != 2 {
		t.Fatalf("instance view missing maxConcurrency: %+v", first.Instance)
	}
	if first.PermitToken == "" {
		t.Fatal("permitToken is empty")
	}
	expiresAt, err := time.Parse(time.RFC3339, first.ExpiresAt)
	if err != nil {
		t.Fatalf("expiresAt %q is not RFC3339: %v", first.ExpiresAt, err)
	}
	if expiresAt.Before(before.Add(29*time.Second)) || expiresAt.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("expiresAt %s not ~30s in the future", expiresAt)
	}

	second := acquireOK(t, h, "svc", `{"key":"user-1","permitTTLSeconds":30}`)
	if second.PermitToken == first.PermitToken {
		t.Fatal("permit tokens are not unique")
	}

	// Quota of 2 is now exhausted.
	rec := acquire(t, h, "svc", `{"key":"user-1","permitTTLSeconds":30}`)
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("acquire over quota = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
	requireJSON(t, rec)

	// Releasing a valid permit frees its slot.
	if rec := releasePermit(t, h, "svc", first.PermitToken); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rec.Code)
	}
	acquireOK(t, h, "svc", `{"key":"user-1","permitTTLSeconds":30}`)

	// A spent token and an unknown token are both gone.
	for _, token := range []string{first.PermitToken, "no-such-permit"} {
		rec := releasePermit(t, h, "svc", token)
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("release %q = %d (%s), want 404 permit_not_found", token, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}
}

func TestAcquireUnlimitedInstanceNeverLimited(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	for i := 0; i < 50; i++ {
		acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)
	}
}

func TestAcquirePermitExpiryFreesSlot(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 300, 1))

	first := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`)
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("acquire with full quota = %d, want 429", rec.Code)
	}
	now = now.Add(11 * time.Second) // permit expires, instance lease still live
	second := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`)
	if rec := releasePermit(t, h, "svc", first.PermitToken); rec.Code != http.StatusNotFound {
		t.Fatalf("release of expired permit = %d, want 404", rec.Code)
	}
	if rec := releasePermit(t, h, "svc", second.PermitToken); rec.Code != http.StatusNoContent {
		t.Fatalf("release of live permit = %d, want 204", rec.Code)
	}
}

func TestAcquireNoAvailableInstance(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	for _, body := range []string{
		`{"key":"k","permitTTLSeconds":10,"version":"v9"}`,
		`{"key":"k","permitTTLSeconds":10,"zone":"z9"}`,
	} {
		rec := acquire(t, h, "svc", body)
		if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
			t.Fatalf("acquire %s = %d (%s), want 503 no_available_instance", body, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}
	rec := acquire(t, h, "ghost", `{"key":"k","permitTTLSeconds":10}`)
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("unknown service = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
}

func TestAcquireNoFallbackAcrossVersionOrZone(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	mustRegister(t, h, "svc", "i2", registerBody("http://b:1", "v2", "z2", 10, 60))

	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60,"version":"v1","zone":"z1"}`)
	// The only matching instance is full: 429, never a fallback to v2/z2.
	rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":60,"version":"v1","zone":"z1"}`)
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("filtered acquire = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
	// A filter matching nothing routable is 503 even though other instances idle.
	rec = acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":60,"version":"v1","zone":"z2"}`)
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("unmatched filter = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
}

func TestAcquireRendezvousOrderSkipsFullInstances(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "a", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	mustRegister(t, h, "svc", "b", registerBodyWithQuota("http://b:1", "v1", "z1", 10, 60, 1))
	// The rendezvous ranking depends only on key, names and weights, so the
	// plain resolve order predicts the acquire order.
	winner := resolveOK(t, h, "/v1/resolve/svc?key=k").Instance.Instance
	loser := "a"
	if winner == "a" {
		loser = "b"
	}
	if got := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`).Instance.Instance; got != winner {
		t.Fatalf("first acquire = %q, want rendezvous winner %q", got, winner)
	}
	if got := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`).Instance.Instance; got != loser {
		t.Fatalf("second acquire = %q, want next-ranked %q", got, loser)
	}
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third acquire = %d, want 429", rec.Code)
	}
}

func TestAcquireOverwriteInvalidatesPermits(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	perm := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)

	// Overwriting the instance supersedes it; old permits die immediately.
	code, _ := mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:2", "v1", "z1", 10, 60, 1))
	if code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	if rec := releasePermit(t, h, "svc", perm.PermitToken); rec.Code != http.StatusNotFound {
		t.Fatalf("release after overwrite = %d, want 404", rec.Code)
	}
	// The new record starts with a fresh quota.
	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("acquire beyond new quota = %d, want 429", rec.Code)
	}
}

func TestAcquireDeregisterInvalidatesPermits(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	perm := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)
	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deregister = %d, want 204", rec.Code)
	}
	if rec := releasePermit(t, h, "svc", perm.PermitToken); rec.Code != http.StatusNotFound {
		t.Fatalf("release after deregister = %d, want 404", rec.Code)
	}
}

func TestAcquireLeaseExpiryInvalidatesPermits(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 10, 1))
	perm := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":300}`)
	now = now.Add(11 * time.Second) // lease expires before the permit does
	if rec := releasePermit(t, h, "svc", perm.PermitToken); rec.Code != http.StatusNotFound {
		t.Fatalf("release after lease expiry = %d, want 404", rec.Code)
	}
}

func TestAcquireDoesNotExtendLease(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 30, 5))

	now = now.Add(20 * time.Second)
	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":300}`)
	// t=40: past the 30s lease; the permit did not keep the instance alive.
	now = now.Add(20 * time.Second)
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatalf("discovery after lease expiry = %v, want []", names)
	}
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("acquire after lease expiry = %d, want 503", rec.Code)
	}
}

func TestAcquireUnhealthyInstance(t *testing.T) {
	h := Handler()
	body := `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"maxConcurrency":1,"healthPolicy":{"failureThreshold":1,"successThreshold":1}}`
	_, reg := mustRegister(t, h, "svc", "i1", body)
	perm := acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":300}`)

	report := func(seq int, status string) {
		t.Helper()
		rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/health",
			fmt.Sprintf(`{"sequence":%d,"status":%q}`, seq, status),
			map[string]string{leaseHeader: reg.LeaseToken})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("health report = %d, want 204", rec.Code)
		}
	}
	report(1, "fail") // -> unhealthy
	// Unhealthy instances receive no new permits; with no other candidate
	// the service is simply unavailable.
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("acquire while unhealthy = %d, want 503", rec.Code)
	}
	report(2, "pass") // -> healthy again
	// The old permit kept counting while unhealthy, so the quota is still full.
	if rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("acquire after recovery = %d, want 429", rec.Code)
	}
	if rec := releasePermit(t, h, "svc", perm.PermitToken); rec.Code != http.StatusNoContent {
		t.Fatalf("release after recovery = %d, want 204", rec.Code)
	}
	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":10}`)
}

func TestResolveDoesNotConsumeQuota(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 1))
	for i := 0; i < 10; i++ {
		resolveOK(t, h, "/v1/resolve/svc?key=k")
	}
	// Resolving never occupies a slot: the quota is still whole.
	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)
	// And a full quota does not affect plain resolves either.
	resolveOK(t, h, "/v1/resolve/svc?key=k")
}

func TestAcquireValidationErrors(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	cases := []struct {
		name   string
		target string
		body   string
	}{
		{"bad service name", "/v1/admission/bad!/acquire", `{"key":"k","permitTTLSeconds":10}`},
		{"service too long", "/v1/admission/" + strings.Repeat("a", 65) + "/acquire", `{"key":"k","permitTTLSeconds":10}`},
		{"missing key", "/v1/admission/svc/acquire", `{"permitTTLSeconds":10}`},
		{"empty key", "/v1/admission/svc/acquire", `{"key":"","permitTTLSeconds":10}`},
		{"key too long", "/v1/admission/svc/acquire", `{"key":"` + strings.Repeat("a", 257) + `","permitTTLSeconds":10}`},
		{"non-string key", "/v1/admission/svc/acquire", `{"key":1,"permitTTLSeconds":10}`},
		{"missing ttl", "/v1/admission/svc/acquire", `{"key":"k"}`},
		{"ttl zero", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":0}`},
		{"ttl too big", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":301}`},
		{"ttl fractional", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":1.5}`},
		{"ttl wrong type", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":"10"}`},
		{"empty version", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":10,"version":""}`},
		{"empty zone", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":10,"zone":""}`},
		{"unknown field", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":10,"bogus":1}`},
		{"malformed JSON", "/v1/admission/svc/acquire", `{"key":`},
		{"empty body", "/v1/admission/svc/acquire", ``},
		{"trailing data", "/v1/admission/svc/acquire", `{"key":"k","permitTTLSeconds":10} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, tc.target, tc.body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
			}
			requireJSON(t, rec)
			if code := errorCode(t, rec); code != "validation_error" {
				t.Fatalf("error code = %q, want validation_error", code)
			}
		})
	}
	// Boundary: a 256-byte key and the TTL extremes are accepted.
	acquireOK(t, h, "svc", `{"key":"`+strings.Repeat("a", 256)+`","permitTTLSeconds":1}`)
	acquireOK(t, h, "svc", `{"key":"k","permitTTLSeconds":300}`)
}

func TestReleaseValidationErrors(t *testing.T) {
	h := Handler()
	rec := releasePermit(t, h, "bad!", "token")
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
		t.Fatalf("bad service name = %d (%s), want 400 validation_error", rec.Code, errorCode(t, rec))
	}
}

func TestAdmissionMethodNotAllowed(t *testing.T) {
	h := Handler()
	cases := []struct {
		method string
		target string
		allow  string
	}{
		{"GET", "/v1/admission/svc/acquire", "POST"},
		{"PUT", "/v1/admission/svc/acquire", "POST"},
		{"DELETE", "/v1/admission/svc/acquire", "POST"},
		{"GET", "/v1/admission/svc/permits/abc", "DELETE"},
		{"POST", "/v1/admission/svc/permits/abc", "DELETE"},
		{"PUT", "/v1/admission/svc/permits/abc", "DELETE"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := do(t, h, tc.method, tc.target, "", nil)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
			if allow := rec.Header().Get("Allow"); allow != tc.allow {
				t.Fatalf("Allow = %q, want %q", allow, tc.allow)
			}
			requireJSON(t, rec)
			if code := errorCode(t, rec); code != "method_not_allowed" {
				t.Fatalf("error code = %q, want method_not_allowed", code)
			}
		})
	}
}

func TestAcquireConcurrentQuota(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBodyWithQuota("http://a:1", "v1", "z1", 10, 60, 5))
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted, limited := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := acquire(t, h, "svc", `{"key":"k","permitTTLSeconds":60}`)
			mu.Lock()
			defer mu.Unlock()
			switch rec.Code {
			case http.StatusCreated:
				granted++
			case http.StatusTooManyRequests:
				limited++
			default:
				t.Errorf("acquire = %d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if granted != 5 || limited != 15 {
		t.Fatalf("granted=%d limited=%d, want exactly 5 granted and 15 limited", granted, limited)
	}
}
