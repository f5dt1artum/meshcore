package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// breakerBody builds a registration body with a concurrency cap and a
// circuit breaker.
func breakerBody(name string, ttl, cap, threshold, openSeconds int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":"v1","zone":"z1","weight":10,"ttlSeconds":%d,"maxConcurrency":%d,"circuitBreaker":{"failureThreshold":%d,"openSeconds":%d}}`,
		"http://"+name+":1", ttl, cap, threshold, openSeconds)
}

func mustRegisterBreaker(t *testing.T, h http.Handler, service, name string, cap, threshold, openSeconds int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name, breakerBody(name, 300, cap, threshold, openSeconds))
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201 (body %+v)", code, resp)
	}
	return resp
}

// clockHarness returns a handler backed by a controllable clock; advancing
// *now mutates the time every handler call observes.
func clockHarness() (http.Handler, *time.Time) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	return s.handler(), &now
}

func advance(now *time.Time, d time.Duration) { *now = now.Add(d) }

func completePermit(t *testing.T, h http.Handler, service, token, outcome string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPost, "/v1/admission/"+service+"/permits/"+token+"/complete",
		fmt.Sprintf(`{"outcome":%q}`, outcome), nil)
}

// keyRoutingTo searches for a key whose resolve (and therefore acquire
// ranking) selects inst; extra is "" or "&version=...".
func keyRoutingTo(t *testing.T, h http.Handler, service, inst, extra string) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/"+service+"?key="+key+extra).Instance.Instance == inst {
			return key
		}
	}
	t.Fatalf("no key routes to %s", inst)
	return ""
}

func expectCircuitOpen(t *testing.T, h http.Handler, body string) {
	t.Helper()
	rec := doAcquire(t, h, "svc", body)
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "circuit_open" {
		t.Fatalf("acquire = %d (%s), want 503 circuit_open", rec.Code, errorCode(t, rec))
	}
}

func TestCircuitBreakerRegistrationValidation(t *testing.T) {
	h := Handler()
	invalid := []string{
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":0,"openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":21,"openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2,"openSeconds":0}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2,"openSeconds":301}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":-1,"openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2.5,"openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":"2","openSeconds":5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2,"openSeconds":5.5}}`,
		`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":2,"openSeconds":5,"extra":1}}`,
	}
	for i, body := range invalid {
		rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/bad"+fmt.Sprint(i), body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("case %d: %d %q, want 400 validation_error", i, rec.Code, errorCode(t, rec))
		}
	}
	// Boundaries are accepted: failureThreshold 1 and 20, openSeconds 1 and 300.
	for i, tc := range []struct{ threshold, open int }{
		{1, 1}, {20, 300},
	} {
		body := fmt.Sprintf(`{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"circuitBreaker":{"failureThreshold":%d,"openSeconds":%d}}`, tc.threshold, tc.open)
		if code, _ := mustRegister(t, h, "svc", "ok"+fmt.Sprint(i), body); code != http.StatusCreated {
			t.Fatalf("boundary %+v: status = %d, want 201", tc, code)
		}
	}
	// Omitting circuitBreaker leaves the baseline behavior intact.
	if code, _ := mustRegister(t, h, "svc", "plain", registerCapBody("http://a:1", "v1", "z1", 10, 300, 5)); code != http.StatusCreated {
		t.Fatalf("register without breaker = %d, want 201", code)
	}
}

func TestClosedOpensAtThresholdAndSuccessResets(t *testing.T) {
	h := Handler()
	mustRegisterBreaker(t, h, "svc", "a", 10, 3, 60)

	fail := func() {
		t.Helper()
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("failure complete = %d, want 204", rec.Code)
		}
	}
	succeed := func() {
		t.Helper()
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "success"); rec.Code != http.StatusNoContent {
			t.Fatalf("success complete = %d, want 204", rec.Code)
		}
	}

	fail()
	fail()
	acquireOK(t, h, "svc", acquireBody("k", 60, "")) // two fails are below three
	succeed()                                        // resets the streak
	fail()
	fail()
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	fail() // third consecutive failure opens the breaker

	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
}

func TestAcquireSkipsOpenInstanceAndFallsBack(t *testing.T) {
	h := Handler()
	mustRegisterBreaker(t, h, "svc", "i1", 10, 1, 60)
	mustRegisterBreaker(t, h, "svc", "i2", 10, 1, 60)
	key := keyRoutingTo(t, h, "svc", "i1", "")

	first := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if first.Instance.Instance != "i1" {
		t.Fatalf("first acquire picked %q, want i1", first.Instance.Instance)
	}
	if rec := completePermit(t, h, "svc", first.PermitToken, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	// i1 is open: acquire moves to the next-ranked instance.
	second := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if second.Instance.Instance != "i2" {
		t.Fatalf("second acquire picked %q, want i2", second.Instance.Instance)
	}
	// Resolve and discovery ignore the breaker entirely.
	if got := resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance; got != "i1" {
		t.Fatalf("resolve = %q, still want i1 while breaker open", got)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 2 {
		t.Fatalf("discovery = %v, want both instances", names)
	}
	if rec := completePermit(t, h, "svc", second.PermitToken, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	expectCircuitOpen(t, h, acquireBody(key, 60, ""))
}

func TestCircuitOpenVsConcurrencyPrecedence(t *testing.T) {
	h := Handler()
	// a opens its breaker (threshold 1); b is closed but capped at one slot.
	mustRegisterBreaker(t, h, "svc", "a", 10, 1, 60)
	mustRegisterCap(t, h, "svc", "b", 1)
	keyA := keyRoutingTo(t, h, "svc", "a", "")

	tok := acquireOK(t, h, "svc", acquireBody(keyA, 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	// Fill b's only slot; resolve still ranks a first, unaffected.
	bTok := acquireOK(t, h, "svc", acquireBody(keyA, 60, "")).PermitToken
	if got := resolveOK(t, h, "/v1/resolve/svc?key="+keyA).Instance.Instance; got != "a" {
		t.Fatalf("setup: resolve picked %q, want a", got)
	}
	// a passes no breaker check (open), b passes the breaker check but is at
	// capacity: 429, not 503 circuit_open.
	rec := doAcquire(t, h, "svc", acquireBody(keyA, 60, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("acquire = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
	if rec := releasePermit(t, h, "svc", bTok); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d", rec.Code)
	}
	got := acquireOK(t, h, "svc", acquireBody(keyA, 60, ""))
	if got.Instance.Instance != "b" {
		t.Fatalf("acquire picked %q, want b", got.Instance.Instance)
	}
}

func TestNoAvailableInstancePrecedence(t *testing.T) {
	h := Handler()
	// The only v1 instance is breaker-open; a healthy instance exists only in
	// v2 and must not be fallen back to when a v1 filter is used.
	mustRegisterBreaker(t, h, "svc", "a", 10, 1, 60)
	mustRegister(t, h, "svc", "b", registerBody("http://b:1", "v2", "z1", 10, 300))

	tok := acquireOK(t, h, "svc", acquireBody("k", 60, `"version":"v1"`)).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	// Without the version filter the next-ranked healthy v2 instance admits.
	if got := acquireOK(t, h, "svc", acquireBody("k", 60, "")).Instance.Instance; got != "b" {
		t.Fatalf("unfiltered acquire picked %q, want fallback b", got)
	}
	// With the v1 filter the open a is the only match: circuit_open, never a
	// fallback to v2.
	expectCircuitOpen(t, h, acquireBody("k", 60, `"version":"v1"`))
	// A version with no instances at all is the plain no-match case.
	rec := doAcquire(t, h, "svc", acquireBody("k", 60, `"version":"v9"`))
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("v9 = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
}

func TestOpenTimingAndHalfOpenSingleProbe(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 3, 2, 10)

	// Three permits; the first two fail and open the breaker while p3 stays
	// outstanding. Opening never cancels a granted permit.
	p1 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	p2 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	p3 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	for _, tok := range []string{p1, p2} {
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
	}
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	// The outstanding ordinary permit can still be completed during open;
	// this only frees its slot.
	if rec := completePermit(t, h, "svc", p3, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete during open = %d, want 204", rec.Code)
	}

	advance(now, 9*time.Second)
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	advance(now, 1*time.Second) // openSeconds elapsed -> half_open

	probe := acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	expectCircuitOpen(t, h, acquireBody("k", 60, "")) // trial pending: no more permits

	// Trial success closes the breaker and clears the failure streak.
	if rec := completePermit(t, h, "svc", probe.PermitToken, "success"); rec.Code != http.StatusNoContent {
		t.Fatalf("probe complete = %d", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	// Counter was reset: one failure below the threshold of two stays closed.
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestHalfOpenProbeFailsReopens(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 5, 1, 10)

	trip := func() {
		t.Helper()
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
	}
	trip() // open at t=0
	advance(now, 10*time.Second)
	probe := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", probe, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("probe complete = %d", rec.Code)
	}
	// A fresh open period starts at the probe failure (t=10).
	advance(now, 9*time.Second)
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	advance(now, 1*time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 60, "")) // trial available again
}

func TestProbeDeletedOrExpiredReopens(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		h, now := clockHarness()
		mustRegisterBreaker(t, h, "svc", "a", 5, 1, 10)
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
		advance(now, 10*time.Second)
		probe := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := releasePermit(t, h, "svc", probe); rec.Code != http.StatusNoContent {
			t.Fatalf("delete probe = %d", rec.Code)
		}
		expectCircuitOpen(t, h, acquireBody("k", 60, ""))
		advance(now, 10*time.Second)
		acquireOK(t, h, "svc", acquireBody("k", 60, "")) // fresh period elapsed
	})

	t.Run("expire", func(t *testing.T) {
		h, now := clockHarness()
		mustRegisterBreaker(t, h, "svc", "a", 5, 1, 10)
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
		advance(now, 10*time.Second)
		acquireOK(t, h, "svc", acquireBody("k", 5, "")) // probe with 5s TTL
		advance(now, 5*time.Second)                     // probe expires: reopen
		expectCircuitOpen(t, h, acquireBody("k", 60, ""))
		advance(now, 10*time.Second)
		acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	})
}

func TestCompletionsDuringOpenDoNotMoveBreaker(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 5, 1, 10)

	p1 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	p2 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	// p1 failure opens at t=0; p2 is an ordinary permit granted before open.
	if rec := completePermit(t, h, "svc", p1, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	advance(now, 5*time.Second)
	// A failure of an in-flight ordinary permit neither restarts the open
	// period nor changes any state; the original t=0 deadline still holds.
	if rec := completePermit(t, h, "svc", p2, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete during open = %d", rec.Code)
	}
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	advance(now, 5*time.Second) // t=10: the original period has elapsed
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestProbeBlocksOtherCandidateWhilePending(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "i1", 5, 1, 10)
	mustRegisterBreaker(t, h, "svc", "i2", 5, 1, 10)
	key := keyRoutingTo(t, h, "svc", "i1", "")
	tok := acquireOK(t, h, "svc", acquireBody(key, 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	advance(now, 10*time.Second)
	probe := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if probe.Instance.Instance != "i1" {
		t.Fatalf("probe went to %q, want i1", probe.Instance.Instance)
	}
	// While i1's trial is pending it is skipped and i2 admits.
	next := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if next.Instance.Instance != "i2" {
		t.Fatalf("acquire picked %q, want i2 while i1 probe pending", next.Instance.Instance)
	}
}

func TestBreakerIndependentOfHealthAndInvisibleInViews(t *testing.T) {
	h := Handler()
	// Health policy (1/1) plus a breaker (threshold 1) on the same instance.
	body := `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":60,"maxConcurrency":5,` +
		`"healthPolicy":{"failureThreshold":1,"successThreshold":1},` +
		`"circuitBreaker":{"failureThreshold":1,"openSeconds":60}}`
	code, reg := mustRegister(t, h, "svc", "a", body)
	if code != http.StatusCreated {
		t.Fatalf("register = %d", code)
	}
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	// Breaker open: admission blocked, but health is untouched.
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 || names[0] != "a" {
		t.Fatalf("discovery = %v, breaker must not affect health/discovery", names)
	}
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k").Instance.HealthStatus; got != healthHealthy {
		t.Fatalf("healthStatus = %q, want healthy after permit failures", got)
	}
	// A failing health report then removes the instance from routing even
	// though permit outcomes would otherwise be the only gating mechanism.
	if rec := reportHealth(t, h, "svc", "a", reg.LeaseToken, `{"sequence":1,"status":"fail"}`); rec.Code != http.StatusNoContent {
		t.Fatalf("health report = %d", rec.Code)
	}
	rec := doAcquire(t, h, "svc", acquireBody("k", 60, ""))
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("unhealthy = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
	// No breaker detail leaks into any public instance view.
	for _, target := range []string{"/v1/discovery/svc", "/v1/resolve/svc?key=k"} {
		raw := do(t, h, http.MethodGet, target, "", nil).Body.String()
		for _, leak := range []string{"circuit", "breaker", "openSeconds", "failureThreshold"} {
			if strings.Contains(raw, leak) {
				t.Fatalf("GET %s leaks %q: %s", target, leak, raw)
			}
		}
	}
}

func TestCompleteValidationAndPermitLifecycle(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "a", 5)
	valid := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken

	badBodies := []string{
		``,
		`not json`,
		`{}`,
		`{"outcome":""}`,
		`{"outcome":"ok"}`,
		`{"outcome":"SUCCESS"}`,
		`{"outcome":1}`,
		`{"outcome":null}`,
		`{"outcome":"success","extra":1}`,
		`{"outcome":"success"} {}`,
	}
	for i, body := range badBodies {
		rec := do(t, h, http.MethodPost, "/v1/admission/svc/permits/"+valid+"/complete", body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("case %d body %q: %d %q, want 400 validation_error", i, body, rec.Code, errorCode(t, rec))
		}
	}
	// A rejected body must not consume the permit: it still completes.
	if rec := completePermit(t, h, "svc", valid, "success"); rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("complete = %d body %q, want 204 empty", rec.Code, rec.Body.String())
	}
	// Repeat completion and a subsequent DELETE both report permit_not_found.
	for i := 0; i < 2; i++ {
		rec := completePermit(t, h, "svc", valid, "success")
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("repeat complete %d = %d (%s), want 404", i, rec.Code, errorCode(t, rec))
		}
	}
	if rec := releasePermit(t, h, "svc", valid); rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("DELETE after complete = %d (%s), want 404", rec.Code, errorCode(t, rec))
	}
	// Unknown token and a token presented against another service 404.
	other := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	for _, tc := range []struct{ service, token string }{
		{"svc", "0123456789abcdef0123456789abcdef"},
		{"ghost", other},
	} {
		rec := completePermit(t, h, tc.service, tc.token, "success")
		if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
			t.Fatalf("%+v: %d (%s), want 404 permit_not_found", tc, rec.Code, errorCode(t, rec))
		}
	}
	// A bad service name is a validation error, not a permit lookup.
	if rec := do(t, h, http.MethodPost, "/v1/admission/bad%21name/permits/x/complete", `{"outcome":"success"}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad service = %d, want 400", rec.Code)
	}
}

func TestCompleteExpiredPermitNotFound(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 5, 3, 60)
	tok := acquireOK(t, h, "svc", acquireBody("k", 5, "")).PermitToken
	advance(now, 5*time.Second)
	rec := completePermit(t, h, "svc", tok, "failure")
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("complete expired = %d (%s), want 404 permit_not_found", rec.Code, errorCode(t, rec))
	}
}

func TestCompleteFreesQuotaImmediately(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "a", 1)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := doAcquire(t, h, "svc", acquireBody("k", 60, "")); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("acquire at cap = %d, want 429", rec.Code)
	}
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, "")) // slot freed
}

func TestCompleteMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := do(t, h, method, "/v1/admission/svc/permits/tok/complete", "", nil)
		if rec.Code != http.StatusMethodNotAllowed || errorCode(t, rec) != "method_not_allowed" {
			t.Fatalf("%s = %d %q, want 405", method, rec.Code, errorCode(t, rec))
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", allow)
		}
	}
}

func TestOverwriteResetsBreakerAndInvalidatesPermit(t *testing.T) {
	h := Handler()
	mustRegisterBreaker(t, h, "svc", "a", 5, 1, 60)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	// Overwrite with a breaker: fresh closed state, old permit invalid.
	if code, _ := mustRegister(t, h, "svc", "a", breakerBody("a", 300, 5, 1, 60)); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNotFound || errorCode(t, rec) != "permit_not_found" {
		t.Fatalf("complete old permit after overwrite = %d (%s), want 404", rec.Code, errorCode(t, rec))
	}

	// Overwriting without a breaker drops the breaker entirely: repeated
	// failure outcomes on the new record never block admission.
	other := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if code, _ := mustRegister(t, h, "svc", "a", registerCapBody("http://a:1", "v1", "z1", 10, 300, 5)); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	if rec := completePermit(t, h, "svc", other, "failure"); rec.Code != http.StatusNotFound {
		t.Fatalf("old permit complete = %d, want 404", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestDeregisterAndLeaseExpiryDiscardBreaker(t *testing.T) {
	t.Run("deregister", func(t *testing.T) {
		h := Handler()
		reg := mustRegisterBreaker(t, h, "svc", "a", 5, 1, 60)
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
		if rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/a", "", map[string]string{leaseHeader: reg.LeaseToken}); rec.Code != http.StatusNoContent {
			t.Fatalf("deregister = %d", rec.Code)
		}
		// A fresh registration starts closed.
		mustRegisterBreaker(t, h, "svc", "a", 5, 1, 60)
		acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	})

	t.Run("lease expiry", func(t *testing.T) {
		h, now := clockHarness()
		mustRegister(t, h, "svc", "a", breakerBody("a", 10, 5, 1, 10))
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete = %d", rec.Code)
		}
		advance(now, 11*time.Second) // lease expires; breaker state is gone
		rec := doAcquire(t, h, "svc", acquireBody("k", 60, ""))
		if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
			t.Fatalf("after lease expiry = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
		}
		mustRegister(t, h, "svc", "a", breakerBody("a", 300, 5, 1, 10))
		acquireOK(t, h, "svc", acquireBody("k", 60, "")) // closed from scratch
	})
}

func TestUnconfiguredInstanceIgnoresOutcomes(t *testing.T) {
	h := Handler()
	mustRegisterCap(t, h, "svc", "a", 1)
	for i := 0; i < 5; i++ {
		tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
		if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
			t.Fatalf("complete %d = %d", i, rec.Code)
		}
	}
	// Still admitted: no breaker exists and each complete frees the slot.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestHalfOpenGrantsExactlyOneProbeUnderConcurrency(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 100, 1, 10)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	if rec := completePermit(t, h, "svc", tok, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	advance(now, 10*time.Second) // half_open before the hammer starts

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	created, blocked := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := doAcquire(t, h, "svc", acquireBody(fmt.Sprintf("k-%d", i), 60, ""))
			mu.Lock()
			switch rec.Code {
			case http.StatusCreated:
				created++
			case http.StatusServiceUnavailable:
				if errorCode(t, rec) != "circuit_open" {
					t.Errorf("blocked with %q", errorCode(t, rec))
				}
				blocked++
			default:
				t.Errorf("acquire = %d, want 201 or 503", rec.Code)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if created != 1 || blocked != n-1 {
		t.Fatalf("created = %d blocked = %d, want exactly 1 probe and %d blocked", created, blocked, n-1)
	}
}

func TestInFlightCompletionsDuringHalfOpenAreIgnored(t *testing.T) {
	h, now := clockHarness()
	mustRegisterBreaker(t, h, "svc", "a", 10, 1, 10)

	// Two ordinary permits granted while closed; the first failure opens the
	// breaker, the second stays in flight through the open period.
	old1 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	old2 := acquireOK(t, h, "svc", acquireBody("k", 300, "")).PermitToken
	if rec := completePermit(t, h, "svc", old1, "failure"); rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d", rec.Code)
	}
	advance(now, 10*time.Second) // half_open
	probe := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken

	// The stale in-flight permit finishes (both outcomes are irrelevant to a
	// half_open breaker) and only releases its slot.
	if rec := completePermit(t, h, "svc", old2, "success"); rec.Code != http.StatusNoContent {
		t.Fatalf("in-flight complete = %d, want 204", rec.Code)
	}
	// The trial is still the only permit allowed.
	expectCircuitOpen(t, h, acquireBody("k", 60, ""))
	if rec := completePermit(t, h, "svc", probe, "success"); rec.Code != http.StatusNoContent {
		t.Fatalf("probe complete = %d", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}
