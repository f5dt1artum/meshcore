package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func registerCapBody(endpoint, version, zone string, weight, ttl, maxConcurrency int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"maxConcurrency":%d}`,
		endpoint, version, zone, weight, ttl, maxConcurrency)
}

func mustRegisterCap(t *testing.T, h http.Handler, service, name string, maxConcurrency int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerCapBody("http://"+name+":1", "v1", "z1", 10, 300, maxConcurrency))
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201 (body %+v)", code, resp)
	}
	return resp
}

func acquireBody(key string, ttl int, extra string) string {
	s := fmt.Sprintf(`{"key":%q,"permitTTLSeconds":%d`, key, ttl)
	if extra != "" {
		s += "," + extra
	}
	return s + "}"
}

func doAcquire(t *testing.T, h http.Handler, service, body string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPost, "/v1/admission/"+service+"/acquire", body, nil)
}

func acquireOK(t *testing.T, h http.Handler, service, body string) acquireResponse {
	t.Helper()
	rec := doAcquire(t, h, service, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("acquire = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	requireJSON(t, rec)
	var resp acquireResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("acquire response is not JSON: %v", err)
	}
	if resp.PermitToken == "" {
		t.Fatal("permitToken is empty")
	}
	return resp
}

func releasePermit(t *testing.T, h http.Handler, service, token string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodDelete, "/v1/admission/"+service+"/permits/"+token, "", nil)
}

func TestAcquireSuccessShape(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 2)

	before := time.Now()
	resp := acquireOK(t, h, "svc", acquireBody("user-42", 30, ""))
	if resp.Service != "svc" || resp.Key != "user-42" {
		t.Fatalf("service/key not echoed: %+v", resp)
	}
	if resp.Instance.Instance != "i1" || resp.Instance.MaxConcurrency != 2 {
		t.Fatalf("unexpected instance view: %+v", resp.Instance)
	}
	expiresAt, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("expiresAt %q is not RFC3339: %v", resp.ExpiresAt, err)
	}
	if expiresAt.Before(before.Add(29*time.Second)) || expiresAt.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("expiresAt %s not ~30s in the future", expiresAt)
	}
	if len(resp.PermitToken) < 16 {
		t.Fatalf("permitToken too short: %q", resp.PermitToken)
	}
}

func TestMaxConcurrencyViewOmitempty(t *testing.T) {
	h := Handler()
	// Without a cap the field is absent from every public representation.
	mustRegister(t, h, "svc", "unbounded", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegisterCap(t, h, "svc", "capped", 7)

	hasRawField := func(body []byte) bool {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("body is not a JSON object: %v", err)
		}
		_, ok := raw["maxConcurrency"]
		return ok
	}

	// Registration view of an uncapped instance.
	rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/unbounded2",
		registerBody("http://a:1", "v1", "z1", 10, 60), nil)
	if hasRawField(rec.Body.Bytes()) {
		t.Fatalf("register view leaks maxConcurrency: %s", rec.Body.String())
	}

	// Discovery: inspect each instance object.
	rec = do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)
	var disc struct {
		Instances []map[string]json.RawMessage `json:"instances"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &disc); err != nil {
		t.Fatalf("discovery not JSON: %v", err)
	}
	for _, inst := range disc.Instances {
		var name string
		_ = json.Unmarshal(inst["instance"], &name)
		_, present := inst["maxConcurrency"]
		if name == "unbounded" && present {
			t.Fatalf("unbounded discovery view exposes maxConcurrency: %s", rec.Body.String())
		}
		if name == "capped" {
			var n int
			_ = json.Unmarshal(inst["maxConcurrency"], &n)
			if n != 7 {
				t.Fatalf("capped maxConcurrency = %d, want 7", n)
			}
		}
	}

	// Resolve and acquire echo the same view; the chosen instance only
	// carries the field when it is capped.
	checkView := func(view map[string]json.RawMessage) {
		var name string
		_ = json.Unmarshal(view["instance"], &name)
		_, present := view["maxConcurrency"]
		if name == "unbounded" && present {
			t.Fatalf("unbounded view of %s exposes maxConcurrency", name)
		}
		if name == "capped" {
			var n int
			_ = json.Unmarshal(view["maxConcurrency"], &n)
			if n != 7 {
				t.Fatalf("capped maxConcurrency = %d, want 7", n)
			}
		}
	}
	rec = do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil)
	var res struct {
		Instance map[string]json.RawMessage `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("resolve not JSON: %v", err)
	}
	checkView(res.Instance)

	rec = doAcquire(t, h, "svc", acquireBody("k", 60, ""))
	var acq struct {
		Instance map[string]json.RawMessage `json:"instance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &acq); err != nil {
		t.Fatalf("acquire not JSON: %v", err)
	}
	checkView(acq.Instance)
}

func TestAcquireMatchesResolveRanking(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 100)
	mustRegisterCap(t, h, "svc", "i2", 100)
	mustRegisterCap(t, h, "svc", "i3", 100)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("route-%d", i)
		want := resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
		got := acquireOK(t, h, "svc", acquireBody(key, 60, "")).Instance.Instance
		if got != want {
			t.Fatalf("key %q: acquire picked %q, resolve picks %q", key, got, want)
		}
	}
}

func TestAcquireQuotaFillsThenLimits(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "only", 1)

	first := acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	if first.Instance.Instance != "only" {
		t.Fatalf("first acquire picked %q", first.Instance.Instance)
	}
	rec := doAcquire(t, h, "svc", acquireBody("k", 60, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("second acquire = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
	// Releasing frees the slot.
	rel := releasePermit(t, h, "svc", first.PermitToken)
	if rel.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rel.Code)
	}
	if rel.Body.Len() != 0 {
		t.Fatalf("release returned body: %q", rel.Body.String())
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestAcquireFallsBackToNextRanked(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 1)
	mustRegisterCap(t, h, "svc", "i2", 1)

	first := acquireOK(t, h, "svc", acquireBody("hot", 60, ""))
	second := acquireOK(t, h, "svc", acquireBody("hot", 60, ""))
	if first.Instance.Instance == second.Instance.Instance {
		t.Fatalf("both permits went to %q; second acquire must use the next-ranked instance", first.Instance.Instance)
	}
	// Every matching instance is now at capacity: 429, never a fallback
	// across version/zone.
	rec := doAcquire(t, h, "svc", acquireBody("hot", 60, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("third acquire = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
}

func TestAcquireUnboundedAlwaysAdmits(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "open", registerBody("http://a:1", "v1", "z1", 10, 300))
	for i := 0; i < 20; i++ {
		if rec := doAcquire(t, h, "svc", acquireBody("k", 60, "")); rec.Code != http.StatusCreated {
			t.Fatalf("acquire %d = %d, want 201", i, rec.Code)
		}
	}
}

func TestAcquireNoFallbackAcrossVersionOrZone(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "v1z1", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "v2z1", registerBody("http://b:1", "v2", "z1", 10, 60))

	for _, extra := range []string{`"version":"v9"`, `"zone":"z9"`, `"version":"v1","zone":"z9"`} {
		rec := doAcquire(t, h, "svc", acquireBody("k", 60, extra))
		if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
			t.Fatalf("filter %s: %d (%s), want 503 no_available_instance", extra, rec.Code, errorCode(t, rec))
		}
	}
	// Unknown service behaves the same way.
	rec := doAcquire(t, h, "ghost", acquireBody("k", 60, ""))
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("unknown service = %d (%s), want 503", rec.Code, errorCode(t, rec))
	}
}

func TestAcquireFiltersVersionZone(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "v1z1", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "v2z2", registerBody("http://b:1", "v2", "z2", 10, 60))
	resp := acquireOK(t, h, "svc", acquireBody("k", 60, `"version":"v2","zone":"z2"`))
	if resp.Instance.Instance != "v2z2" {
		t.Fatalf("filtered acquire picked %q, want v2z2", resp.Instance.Instance)
	}
	if resp.Instance.Version != "v2" || resp.Instance.Zone != "z2" {
		t.Fatalf("view filters not reflected: %+v", resp.Instance)
	}
}

func TestReleaseUnknownToken(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	for _, token := range []string{"deadbeef", "0123456789abcdef0123456789abcdef"} {
		rec := releasePermit(t, h, "svc", token)
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("release %q = %d (%s), want 404 permit_not_found", token, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}
}

func TestReleaseIdempotentNotFound(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 1)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := releasePermit(t, h, "svc", tok); rec.Code != http.StatusNoContent {
		t.Fatalf("first release = %d, want 204", rec.Code)
	}
	rec := releasePermit(t, h, "svc", tok)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("second release = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
}

func TestPermitExpiryFreesSlot(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterCap(t, h, "svc", "i1", 1)

	tok := acquireOK(t, h, "svc", acquireBody("k", 5, "")).PermitToken
	now = now.Add(5 * time.Second) // permit reaches its expiry

	rec := releasePermit(t, h, "svc", tok)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("expired release = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
	// The expired permit no longer occupies the quota.
	acquireOK(t, h, "svc", acquireBody("k", 5, ""))
}

func TestOverwriteInvalidatesPermitsAndQuota(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 1)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken

	// Overwrite as a brand-new instance.
	if code, _ := mustRegister(t, h, "svc", "i1", registerCapBody("http://i1:2", "v1", "z1", 10, 300, 1)); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	rec := releasePermit(t, h, "svc", tok)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("release after overwrite = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
	// Old quota was released with the invalidated permits.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestDeregisterInvalidatesPermits(t *testing.T) {
	h := Handler()
	reg := mustRegisterCap(t, h, "svc", "i1", 1)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken

	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deregister = %d, want 204", rec.Code)
	}
	rec = releasePermit(t, h, "svc", tok)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("release after deregister = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
}

func TestLeaseExpiryInvalidatesPermitsAndFreesQuota(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1", registerCapBody("http://a:1", "v1", "z1", 10, 10, 1))

	tok := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	now = now.Add(11 * time.Second) // instance lease outlasted by the permit TTL

	rec := releasePermit(t, h, "svc", tok)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("release after lease expiry = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
	// Re-register under the same name; the old permit's slot must be free.
	mustRegister(t, h, "svc", "i1", registerCapBody("http://a:1", "v1", "z1", 10, 300, 1))
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestUnhealthyGetsNoPermitsButKeepsCounting(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()

	// a: cap 1, health policy flips on a single report; b exists in another
	// version so v1-only filters prove there is no fallback.
	regA := mustRegisterWithPolicyBody2(t, h, "svc", "a", "v1", 1, 1, 1)
	mustRegister(t, h, "svc", "b", registerBody("http://b:1", "v2", "z1", 10, 300))

	// Find a key routed to a.
	key := ""
	for i := 0; ; i++ {
		cand := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/svc?key="+cand+"&version=v1").Instance.Instance == "a" {
			key = cand
			break
		}
	}
	held := acquireOK(t, h, "svc", acquireBody(key, 300, `"version":"v1"`))
	if held.Instance.Instance != "a" {
		t.Fatalf("expected permit on a, got %q", held.Instance.Instance)
	}

	// a turns unhealthy: no new permits, and v2 must not receive the fallback.
	if rec := reportHealth(t, h, "svc", "a", regA, `{"sequence":1,"status":"fail"}`); rec.Code != http.StatusNoContent {
		t.Fatalf("fail report = %d, want 204", rec.Code)
	}
	rec := doAcquire(t, h, "svc", acquireBody(key, 60, `"version":"v1"`))
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("acquire while unhealthy = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}

	// a recovers but the held permit still occupies its only slot: 429.
	if rec := reportHealth(t, h, "svc", "a", regA, `{"sequence":2,"status":"pass"}`); rec.Code != http.StatusNoContent {
		t.Fatalf("pass report = %d, want 204", rec.Code)
	}
	rec = doAcquire(t, h, "svc", acquireBody(key, 60, `"version":"v1"`))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("acquire after recovery = %d (%s), want 429 concurrency_limited (held permit still counts)", rec.Code, errorCode(t, rec))
	}
	// Release the held permit: admission works again.
	if rec := releasePermit(t, h, "svc", held.PermitToken); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody(key, 60, `"version":"v1"`))
}

// mustRegisterWithPolicyBody2 registers an instance with a health policy and
// an explicit version/cap, returning its lease token.
func mustRegisterWithPolicyBody2(t *testing.T, h http.Handler, service, name, version string, weight, ttl, cap int) string {
	t.Helper()
	body := fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":"z1","weight":%d,"ttlSeconds":%d,"maxConcurrency":%d,"healthPolicy":{"failureThreshold":1,"successThreshold":1}}`,
		"http://"+name+":1", version, weight, ttl, cap)
	code, resp := mustRegister(t, h, service, name, body)
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}
	return resp.LeaseToken
}

func TestAcquireDoesNotExtendLeaseOrChangeHealth(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1", registerCapBody("http://a:1", "v1", "z1", 10, 10, 100))

	now = now.Add(8 * time.Second)
	for i := 0; i < 5; i++ {
		acquireOK(t, h, "svc", acquireBody(fmt.Sprintf("k-%d", i), 100, ""))
	}
	now = now.Add(3 * time.Second) // t=11: past the original 10s lease
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatalf("acquire extended the lease: discovery = %v, want []", names)
	}
}

func TestResolveDoesNotConsumeQuota(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 1)
	for i := 0; i < 10; i++ {
		if rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil); rec.Code != http.StatusOK {
			t.Fatalf("resolve = %d, want 200", rec.Code)
		}
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	rec := doAcquire(t, h, "svc", acquireBody("k", 60, ""))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second acquire = %d, want 429", rec.Code)
	}
}

func TestAcquireValidationErrors(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 10)
	cases := []struct {
		name    string
		service string
		body    string
	}{
		{"bad service name", "bad!", acquireBody("k", 10, "")},
		{"empty body", "svc", ``},
		{"not json", "svc", `nope`},
		{"missing key", "svc", `{"permitTTLSeconds":10}`},
		{"empty key", "svc", `{"key":"","permitTTLSeconds":10}`},
		{"key too long", "svc", `{"key":"` + stringsRepeat(257) + `","permitTTLSeconds":10}`},
		{"numeric key", "svc", `{"key":7,"permitTTLSeconds":10}`},
		{"missing ttl", "svc", `{"key":"k"}`},
		{"ttl zero", "svc", acquireBody("k", 0, "")},
		{"ttl too big", "svc", acquireBody("k", 301, "")},
		{"ttl negative", "svc", acquireBody("k", -1, "")},
		{"ttl fractional", "svc", `{"key":"k","permitTTLSeconds":1.5}`},
		{"ttl string", "svc", `{"key":"k","permitTTLSeconds":"10"}`},
		{"empty version", "svc", `{"key":"k","permitTTLSeconds":10,"version":""}`},
		{"empty zone", "svc", `{"key":"k","permitTTLSeconds":10,"zone":""}`},
		{"unknown field", "svc", `{"key":"k","permitTTLSeconds":10,"bogus":1}`},
		{"trailing data", "svc", acquireBody("k", 10, "") + ` {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doAcquire(t, h, tc.service, tc.body)
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
				t.Fatalf("status = %d code = %q, want 400 validation_error (body %s)", rec.Code, errorCode(t, rec), rec.Body.String())
			}
		})
	}
}

func stringsRepeat(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}

func TestRegisterMaxConcurrencyValidation(t *testing.T) {
	h := Handler()
	for _, n := range []int{0, -1, 10001} {
		body := fmt.Sprintf(`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"maxConcurrency":%d}`, n)
		rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i"+fmt.Sprint(n), body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("maxConcurrency %d: status = %d code = %q, want 400 validation_error", n, rec.Code, errorCode(t, rec))
		}
	}
	for _, body := range []string{
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"maxConcurrency":1.5}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"maxConcurrency":"5"}`,
	} {
		rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/x", body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("bad maxConcurrency type: %d %q, want 400", rec.Code, errorCode(t, rec))
		}
	}
	// Boundaries 1 and 10000 are accepted.
	for i, n := range []int{1, 10000} {
		body := fmt.Sprintf(`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"maxConcurrency":%d}`, n)
		if code, _ := mustRegister(t, h, "svc", fmt.Sprintf("ok-%d", i), body); code != http.StatusCreated {
			t.Fatalf("maxConcurrency %d: status = %d, want 201", n, code)
		}
	}
}

func TestAdmissionMethodNotAllowed(t *testing.T) {
	h := Handler()
	cases := []struct {
		method string
		target string
		allow  string
	}{
		{http.MethodGet, "/v1/admission/svc/acquire", http.MethodPost},
		{http.MethodPut, "/v1/admission/svc/acquire", http.MethodPost},
		{http.MethodDelete, "/v1/admission/svc/acquire", http.MethodPost},
		{http.MethodGet, "/v1/admission/svc/permits/tok", http.MethodDelete},
		{http.MethodPost, "/v1/admission/svc/permits/tok", http.MethodDelete},
		{http.MethodPut, "/v1/admission/svc/permits/tok", http.MethodDelete},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := do(t, h, tc.method, tc.target, "", nil)
			if rec.Code != http.StatusMethodNotAllowed || errorCode(t, rec) != "method_not_allowed" {
				t.Fatalf("%s %s = %d %q, want 405 method_not_allowed", tc.method, tc.target, rec.Code, errorCode(t, rec))
			}
			if allow := rec.Header().Get("Allow"); allow != tc.allow {
				t.Fatalf("Allow = %q, want %q", allow, tc.allow)
			}
		})
	}
}

func TestAcquireAtomicUnderConcurrency(t *testing.T) {
	h := Handler()
	const cap = 50
	mustRegisterCap(t, h, "svc", "only", cap)

	var mu sync.Mutex
	created, limited := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				rec := doAcquire(t, h, "svc", acquireBody(fmt.Sprintf("g-%d-k-%d", i, j), 300, ""))
				mu.Lock()
				switch rec.Code {
				case http.StatusCreated:
					created++
				case http.StatusTooManyRequests:
					limited++
				default:
					t.Errorf("acquire = %d, want 201 or 429", rec.Code)
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if created != cap {
		t.Fatalf("created = %d, want exactly %d (limited=%d)", created, cap, limited)
	}
	if created+limited != 320 {
		t.Fatalf("created+limited = %d, want 320", created+limited)
	}
}

func TestAcquireTokenUniqueAndOpaque(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "open", registerBody("http://a:1", "v1", "z1", 10, 300))
	seen := map[string]bool{}
	for i := 0; i < 25; i++ {
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if seen[tok] {
			t.Fatalf("duplicate permit token %q", tok)
		}
		seen[tok] = true
	}
}
