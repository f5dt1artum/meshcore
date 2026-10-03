package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func registerCircuitBody(endpoint, version, zone string, weight, ttl, failThreshold, openSeconds int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"circuitBreaker":{"failureThreshold":%d,"openSeconds":%d}}`,
		endpoint, version, zone, weight, ttl, failThreshold, openSeconds)
}

func registerCircuitCapBody(endpoint, version, zone string, weight, ttl, maxConcurrency, failThreshold, openSeconds int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"maxConcurrency":%d,"circuitBreaker":{"failureThreshold":%d,"openSeconds":%d}}`,
		endpoint, version, zone, weight, ttl, maxConcurrency, failThreshold, openSeconds)
}

func mustRegisterCircuit(t *testing.T, h http.Handler, service, name string, failThreshold, openSeconds int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerCircuitBody("http://"+name+":1", "v1", "z1", 10, 300, failThreshold, openSeconds))
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201 (body %+v)", code, resp)
	}
	return resp
}

func completePermit(t *testing.T, h http.Handler, service, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPost, "/v1/admission/"+service+"/permits/"+token+"/complete", body, nil)
}

func completeOK(t *testing.T, h http.Handler, service, token, outcome string) {
	t.Helper()
	rec := completePermit(t, h, service, token, fmt.Sprintf(`{"outcome":%q}`, outcome))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("complete %q = %d, want 204 (body %s)", outcome, rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("complete returned body: %q", rec.Body.String())
	}
}

// acquireFail acquires one permit and completes it with the given outcome.
func acquireAndComplete(t *testing.T, h http.Handler, service, key, outcome string) {
	t.Helper()
	tok := acquireOK(t, h, service, acquireBody(key, 60, "")).PermitToken
	completeOK(t, h, service, tok, outcome)
}

func wantAcquireStatus(t *testing.T, h http.Handler, service, body string, code int, errCode string) {
	t.Helper()
	rec := doAcquire(t, h, service, body)
	if rec.Code != code || errorCode(t, rec) != errCode {
		t.Fatalf("acquire = %d (%s), want %d %s", rec.Code, errorCode(t, rec), code, errCode)
	}
}

func TestRegisterCircuitBreakerValidation(t *testing.T) {
	h := Handler()
	base := `"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30`
	bodies := []string{
		`{"circuitBreaker":{}}`,
		`{"circuitBreaker":{"failureThreshold":2}}`,
		`{"circuitBreaker":{"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":0,"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":21,"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":-1,"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":2,"openSeconds":0}}`,
		`{"circuitBreaker":{"failureThreshold":2,"openSeconds":301}}`,
		`{"circuitBreaker":{"failureThreshold":2,"openSeconds":-5}}`,
		`{"circuitBreaker":{"failureThreshold":1.5,"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":"2","openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":2,"openSeconds":"10"}}`,
		`{"circuitBreaker":{"failureThreshold":null,"openSeconds":10}}`,
		`{"circuitBreaker":{"failureThreshold":2,"openSeconds":10,"bogus":1}}`,
		`{"circuitBreaker":"off"}`,
	}
	for i, cb := range bodies {
		rec := do(t, h, http.MethodPut, fmt.Sprintf("/v1/services/svc/instances/bad-%d", i),
			"{"+base+","+cb[1:], nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("body %s: status = %d code = %q, want 400 validation_error", cb, rec.Code, errorCode(t, rec))
		}
	}
	// Boundaries are accepted.
	for i, cfg := range [][2]int{{1, 1}, {20, 300}} {
		body := registerCircuitBody("http://a:1", "v1", "z1", 10, 30, cfg[0], cfg[1])
		if code, _ := mustRegister(t, h, "svc", fmt.Sprintf("ok-%d", i), body); code != http.StatusCreated {
			t.Fatalf("circuitBreaker %v: status = %d, want 201", cfg, code)
		}
	}
}

func TestCompleteSuccessReleasesAndInvalidates(t *testing.T) {
	h := Handler()
	mustRegisterCapCircuit(t, h, "svc", "i1", 1, 2, 60)

	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	completeOK(t, h, "svc", tok, "success")
	// The token is invalidated: repeated complete and DELETE both 404.
	rec := completePermit(t, h, "svc", tok, `{"outcome":"success"}`)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("second complete = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
	requireJSON(t, rec)
	if rec := releasePermit(t, h, "svc", tok); rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE after complete = %d, want 404", rec.Code)
	}
	// The slot was freed immediately.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestCompleteValidationErrors(t *testing.T) {
	h := Handler()
	mustRegisterCircuit(t, h, "svc", "i1", 2, 60)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken

	bodies := []string{
		``, `{}`, `nope`, `{"outcome":""}`, `{"outcome":"ok"}`, `{"outcome":1}`,
		`{"outcome":null}`, `{"outcome":"success","bogus":1}`, `{"outcome":"success"} {}`,
		`{"outcome":"SUCCESS"}`, `{"outcome":["success"]}`,
	}
	for _, body := range bodies {
		rec := completePermit(t, h, "svc", tok, body)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("body %q: = %d (%s), want 400 validation_error", body, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}
	// Failed validation does not consume the permit.
	completeOK(t, h, "svc", tok, "success")
}

func TestCompleteUnknownExpiredAndCrossService(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterCircuit(t, h, "svc", "i1", 2, 60)
	mustRegisterCircuit(t, h, "other", "i2", 2, 60)

	// Unknown tokens, even with a valid body, are 404 (existence precedes
	// body validation).
	for _, token := range []string{"deadbeef", "0123456789abcdef0123456789abcdef"} {
		rec := completePermit(t, h, "svc", token, `{"outcome":"success"}`)
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("unknown token = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
		}
		rec = completePermit(t, h, "svc", token, `not json`)
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("unknown token + bad body = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
		}
	}

	// A permit of another service does not match.
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	rec := completePermit(t, h, "other", tok, `{"outcome":"success"}`)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("cross-service complete = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}

	// An expired permit is gone.
	expiring := acquireOK(t, h, "svc", acquireBody("k", 5, "")).PermitToken
	now = now.Add(5 * time.Second)
	rec = completePermit(t, h, "svc", expiring, `{"outcome":"failure"}`)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("expired complete = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
	// The live permit from before still completes.
	completeOK(t, h, "svc", tok, "success")
}

func TestCompleteMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := do(t, h, method, "/v1/admission/svc/permits/tok/complete", "", nil)
		if rec.Code != http.StatusMethodNotAllowed || errorCode(t, rec) != "method_not_allowed" {
			t.Fatalf("%s = %d (%s), want 405 method_not_allowed", method, rec.Code, errorCode(t, rec))
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
			t.Fatalf("%s: Allow = %q, want POST", method, allow)
		}
	}
}

func TestCircuitOpensAfterThresholdAndSuccessResets(t *testing.T) {
	h := Handler()
	mustRegisterCircuit(t, h, "svc", "i1", 3, 60)

	acquireAndComplete(t, h, "svc", "k", "failure")
	acquireAndComplete(t, h, "svc", "k", "failure")
	acquireAndComplete(t, h, "svc", "k", "success") // resets the streak
	acquireAndComplete(t, h, "svc", "k", "failure")
	acquireAndComplete(t, h, "svc", "k", "failure")
	// Two failures after the reset: still closed.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	// The third consecutive failure opens the circuit.
	acquireAndComplete(t, h, "svc", "k", "failure")
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
}

func TestCircuitOpenSkipsToNextRanked(t *testing.T) {
	h := Handler()
	mustRegisterCircuit(t, h, "svc", "a", 1, 60)
	mustRegister(t, h, "svc", "b", registerBody("http://b:1", "v1", "z1", 10, 300))

	// Find a key whose first-ranked instance is a.
	key := ""
	for i := 0; ; i++ {
		cand := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/svc?key="+cand).Instance.Instance == "a" {
			key = cand
			break
		}
	}
	tok := acquireOK(t, h, "svc", acquireBody(key, 60, "")).PermitToken
	completeOK(t, h, "svc", tok, "failure") // a opens

	// Acquire falls through to b in rank order.
	resp := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if resp.Instance.Instance != "b" {
		t.Fatalf("acquire with a open picked %q, want b", resp.Instance.Instance)
	}
	// Resolve and discovery are unaffected by the circuit.
	if got := resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance; got != "a" {
		t.Fatalf("resolve picked %q, want a (circuit must not affect resolve)", got)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 2 {
		t.Fatalf("discovery = %v, want both instances", names)
	}
}

func TestCircuitOpenAllBlockedVsConcurrencyLimited(t *testing.T) {
	h := Handler()
	// a has a circuit (threshold 1), b has none and room for one permit.
	mustRegisterCapCircuit(t, h, "svc", "a", 1, 1, 60)
	mustRegisterCap(t, h, "svc", "b", 1)

	// Find a key ranked a-first.
	key := ""
	for i := 0; ; i++ {
		cand := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/svc?key="+cand).Instance.Instance == "a" {
			key = cand
			break
		}
	}
	// Open a's circuit.
	tok := acquireOK(t, h, "svc", acquireBody(key, 60, "")).PermitToken
	completeOK(t, h, "svc", tok, "failure")
	// Fill b's only slot via the fallback.
	if got := acquireOK(t, h, "svc", acquireBody(key, 60, "")).Instance.Instance; got != "b" {
		t.Fatalf("fallback acquire picked %q, want b", got)
	}
	// a is circuit-blocked, b passes the circuit but is full: 429.
	wantAcquireStatus(t, h, "svc", acquireBody(key, 60, ""), http.StatusTooManyRequests, "concurrency_limited")
}

func TestCircuitOpenSingleInstance(t *testing.T) {
	h := Handler()
	mustRegisterCapCircuit(t, h, "svc", "only", 2, 1, 60)

	// Open the circuit while a second permit is still outstanding.
	p1 := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	p2 := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	completeOK(t, h, "svc", p1, "failure")
	// Free slots remain, but the circuit blocks every candidate.
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
	// Completing the outstanding permit while open only frees its slot.
	completeOK(t, h, "svc", p2, "success")
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
}

func TestCircuitHalfOpenProbeFlow(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterCircuit(t, h, "svc", "i1", 1, 30)

	acquireAndComplete(t, h, "svc", "k", "failure") // opens
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")

	// After openSeconds the next acquire gets the single probe permit.
	now = now.Add(30 * time.Second)
	probe := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	// No second permit while the probe is outstanding.
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")

	// A failed probe restarts the open period.
	completeOK(t, h, "svc", probe, "failure")
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")

	// The next window's probe succeeds: closed again, streak cleared.
	now = now.Add(30 * time.Second)
	probe2 := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	completeOK(t, h, "svc", probe2, "success")
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestProbeReleaseReopensCircuit(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterCircuit(t, h, "svc", "i1", 1, 10)

	acquireAndComplete(t, h, "svc", "k", "failure") // opens for 10s
	now = now.Add(10 * time.Second)
	probe := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	// Releasing the probe via DELETE re-opens the circuit.
	if rec := releasePermit(t, h, "svc", probe); rec.Code != http.StatusNoContent {
		t.Fatalf("release probe = %d, want 204", rec.Code)
	}
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
	// The fresh open period runs from the release.
	now = now.Add(10 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestProbeExpiryReopensCircuit(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterCircuit(t, h, "svc", "i1", 1, 10)

	acquireAndComplete(t, h, "svc", "k", "failure") // opens for 10s
	now = now.Add(10 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 5, "")) // probe, expires after 5s
	now = now.Add(5 * time.Second)                  // probe expires unused
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
	// The re-opened period ends 10s after the expiry.
	now = now.Add(10 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestCircuitClosedConcurrencyStillLimited(t *testing.T) {
	h := Handler()
	mustRegisterCapCircuit(t, h, "svc", "i1", 1, 5, 60)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "concurrency_limited")
}

func TestCompleteOnUnconfiguredInstanceKeepsBehavior(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "i1", 1)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	completeOK(t, h, "svc", tok, "failure")
	completeOK(t, h, "svc", acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken, "failure")
	// Failures never block an instance without a circuitBreaker policy.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestCircuitDoesNotChangeHealthOrDiscovery(t *testing.T) {
	h := Handler()
	body := `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,` +
		`"healthPolicy":{"failureThreshold":1,"successThreshold":1},` +
		`"circuitBreaker":{"failureThreshold":1,"openSeconds":60}}`
	if code, _ := mustRegister(t, h, "svc", "i1", body); code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}
	acquireAndComplete(t, h, "svc", "k", "failure") // opens the circuit
	// Health is untouched: the instance stays visible and resolvable.
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 || names[0] != "i1" {
		t.Fatalf("discovery = %v, want [i1]", names)
	}
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k").Instance.HealthStatus; got != healthHealthy {
		t.Fatalf("healthStatus = %q, want healthy", got)
	}
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")
}

func TestOverwriteResetsCircuit(t *testing.T) {
	h := Handler()
	mustRegisterCircuit(t, h, "svc", "i1", 1, 60)
	acquireAndComplete(t, h, "svc", "k", "failure") // opens
	wantAcquireStatus(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")

	// Overwriting the instance resets its circuit to closed.
	if code, _ := mustRegister(t, h, "svc", "i1",
		registerCircuitBody("http://i1:2", "v1", "z1", 10, 300, 1, 60)); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestDeregisterDropsCircuitState(t *testing.T) {
	h := Handler()
	reg := mustRegisterCircuit(t, h, "svc", "i1", 1, 60)
	acquireAndComplete(t, h, "svc", "k", "failure") // opens
	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deregister = %d, want 204", rec.Code)
	}
	// Re-registered under the same name the circuit starts closed.
	mustRegisterCircuit(t, h, "svc", "i1", 1, 60)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

// mustRegisterCapCircuit registers an instance with both a concurrency cap
// and a circuit breaker policy.
func mustRegisterCapCircuit(t *testing.T, h http.Handler, service, name string, maxConcurrency, failThreshold, openSeconds int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerCircuitCapBody("http://"+name+":1", "v1", "z1", 10, 300, maxConcurrency, failThreshold, openSeconds))
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201 (body %+v)", code, resp)
	}
	return resp
}
