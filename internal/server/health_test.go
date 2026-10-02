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

// policyBody builds a registration body carrying a health policy.
func policyBody(endpoint, version, zone string, weight, ttl, failThr, succThr int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,`+
		`"healthPolicy":{"failureThreshold":%d,"successThreshold":%d}}`,
		endpoint, version, zone, weight, ttl, failThr, succThr)
}

func healthPost(t *testing.T, h http.Handler, service, name, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	headers := map[string]string{}
	if token != "" {
		headers[leaseHeader] = token
	}
	return do(t, h, http.MethodPost,
		"/v1/services/"+service+"/instances/"+name+"/health", body, headers)
}

func contains(s, substr string) bool { return strings.Contains(s, substr) }

func TestRegisterHealthPolicyStatus(t *testing.T) {
	h := Handler()

	// Without a policy the instance starts disabled and still routes.
	_, none := mustRegister(t, h, "svc", "plain", registerBody("http://a:1", "v1", "z1", 10, 60))
	if none.HealthStatus != healthStatusDisabled {
		t.Fatalf("no-policy status = %q, want disabled", none.HealthStatus)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 || names[0] != "plain" {
		t.Fatalf("disabled instance missing from discovery: %v", names)
	}

	_, pol := mustRegister(t, h, "svc", "checked", policyBody("http://b:1", "v1", "z1", 10, 60, 3, 2))
	if pol.HealthStatus != healthStatusHealthy {
		t.Fatalf("policy status = %q, want healthy", pol.HealthStatus)
	}
	if got := discoverNames(t, h, "svc", ""); len(got) != 2 {
		t.Fatalf("discovery = %v, want both instances", got)
	}

	// healthStatus appears on the public view (discovery) but thresholds do not.
	rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)
	for _, want := range []string{`"healthStatus":"disabled"`, `"healthStatus":"healthy"`} {
		if !contains(rec.Body.String(), want) {
			t.Fatalf("discovery body missing %s: %s", want, rec.Body.String())
		}
	}
	if contains(rec.Body.String(), "failureThreshold") || contains(rec.Body.String(), "leaseToken") {
		t.Fatalf("discovery leaks policy/token fields: %s", rec.Body.String())
	}
}

func TestRegisterHealthPolicyValidation(t *testing.T) {
	h := Handler()
	base := func(policy string) string {
		return `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30` + policy + `}`
	}
	cases := []struct {
		name string
		body string
	}{
		{"empty policy object", base(`,"healthPolicy":{}`)},
		{"missing successThreshold", base(`,"healthPolicy":{"failureThreshold":3}`)},
		{"missing failureThreshold", base(`,"healthPolicy":{"successThreshold":3}`)},
		{"zero failure", base(`,"healthPolicy":{"failureThreshold":0,"successThreshold":3}`)},
		{"zero success", base(`,"healthPolicy":{"failureThreshold":3,"successThreshold":0}`)},
		{"failure too big", base(`,"healthPolicy":{"failureThreshold":11,"successThreshold":3}`)},
		{"success too big", base(`,"healthPolicy":{"failureThreshold":3,"successThreshold":11}`)},
		{"negative failure", base(`,"healthPolicy":{"failureThreshold":-1,"successThreshold":3}`)},
		{"fractional threshold", base(`,"healthPolicy":{"failureThreshold":2.5,"successThreshold":3}`)},
		{"string threshold", base(`,"healthPolicy":{"failureThreshold":"2","successThreshold":3}`)},
		{"unknown policy field", base(`,"healthPolicy":{"failureThreshold":3,"successThreshold":3,"bogus":1}`)},
		{"policy not object", base(`,"healthPolicy":[]`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i1", tc.body, nil)
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
				t.Fatalf("status = %d (%s), want 400 validation_error", rec.Code, errorCode(t, rec))
			}
		})
	}
	// Boundary values 1 and 10 are accepted; an explicit null means "no policy".
	for i, body := range []string{
		policyBody("http://a:1", "v1", "z1", 10, 30, 1, 1),
		policyBody("http://a:1", "v1", "z1", 10, 30, 10, 10),
		base(`,"healthPolicy":null`),
	} {
		if code, _ := mustRegister(t, h, fmt.Sprintf("svc%d", i), "i1", body); code != http.StatusCreated {
			t.Fatalf("boundary policy case %d = %d, want 201", i, code)
		}
	}
}

func TestHealthReportTransitions(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 60, 2, 3))
	post := func(seq int, status string) {
		t.Helper()
		rec := healthPost(t, h, "svc", "i1", fmt.Sprintf(`{"sequence":%d,"status":%q}`, seq, status), reg.LeaseToken)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("report seq=%d %s = %d (%s), want 204", seq, status, rec.Code, errorCode(t, rec))
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("report returned body: %q", rec.Body.String())
		}
	}
	routable := func(want bool) {
		t.Helper()
		found := len(discoverNames(t, h, "svc", "")) == 1
		if found != want {
			t.Fatalf("routable = %v, want %v", found, want)
		}
	}

	// One failure is below failureThreshold: still healthy.
	post(1, "fail")
	routable(true)
	// A pass clears the failure streak, so another single fail stays healthy.
	post(2, "pass")
	post(3, "fail")
	routable(true)
	// Second consecutive fail trips the threshold.
	post(4, "fail")
	routable(false)

	// Two consecutive passes are below successThreshold: still excluded.
	post(5, "pass")
	post(6, "pass")
	routable(false)
	// A fail while unhealthy resets the recovery streak.
	post(7, "fail")
	routable(false)
	// Three consecutive passes restore health.
	post(8, "pass")
	post(9, "pass")
	post(10, "pass")
	routable(true)
}

func TestHealthReportSequenceSemantics(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 60, 2, 2))
	send := func(seq int, status string) *httptest.ResponseRecorder {
		return healthPost(t, h, "svc", "i1", fmt.Sprintf(`{"sequence":%d,"status":%q}`, seq, status), reg.LeaseToken)
	}

	// Same sequence with the same status is idempotent and does not double count.
	if rec := send(1, "fail"); rec.Code != http.StatusNoContent {
		t.Fatalf("first fail = %d, want 204", rec.Code)
	}
	if rec := send(1, "fail"); rec.Code != http.StatusNoContent {
		t.Fatalf("idempotent re-delivery = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 {
		t.Fatal("idempotent re-delivery advanced counters")
	}
	// Same sequence with a different status conflicts and changes nothing.
	if rec := send(1, "pass"); rec.Code != http.StatusConflict || errorCode(t, rec) != "health_report_conflict" {
		t.Fatalf("same-seq conflict = %d (%s), want 409 health_report_conflict", rec.Code, errorCode(t, rec))
	}
	// A larger sequence still needs two consecutive fails to trip (counter stayed at 1).
	if rec := send(2, "fail"); rec.Code != http.StatusNoContent {
		t.Fatalf("second distinct fail = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatal("instance should be unhealthy after two distinct fails")
	}
	// Smaller sequences are stale once a greater one was accepted.
	for _, seq := range []int{0, 1} {
		if rec := send(seq, "pass"); rec.Code != http.StatusConflict || errorCode(t, rec) != "stale_health_report" {
			t.Fatalf("stale seq=%d = %d (%s), want 409 stale_health_report", seq, rec.Code, errorCode(t, rec))
		}
	}
	// Sequence 0 is valid as the first report on a freshly registered instance.
	_, fresh := mustRegister(t, h, "svc", "i2", policyBody("http://b:1", "v1", "z1", 10, 60, 1, 1))
	if rec := healthPost(t, h, "svc", "i2", `{"sequence":0,"status":"pass"}`, fresh.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("first report with sequence 0 = %d, want 204", rec.Code)
	}
}

func TestHealthReportDoesNotRenewLease(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 10, 2, 2))

	now = now.Add(5 * time.Second)
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"pass"}`, reg.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("report = %d, want 204", rec.Code)
	}
	// Past the original 10s expiry: the accepted report must not have extended it.
	now = now.Add(6 * time.Second)
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":2,"status":"pass"}`, reg.LeaseToken); rec.Code != http.StatusNotFound {
		t.Fatalf("report after original expiry = %d, want 404 (no renewal)", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatalf("discovery = %v, want [] after expiry", names)
	}
}

func TestHealthReportErrors(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	_, disabled := mustRegister(t, h, "svc", "plain", registerBody("http://a:1", "v1", "z1", 10, 60))
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://b:1", "v1", "z1", 10, 10, 2, 2))

	// Unknown and expired instances are 404 regardless of token/body.
	if rec := healthPost(t, h, "svc", "ghost", `{"sequence":1,"status":"pass"}`, reg.LeaseToken); rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("unknown instance = %d (%s), want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
	now = now.Add(11 * time.Second)
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"pass"}`, reg.LeaseToken); rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("expired instance = %d (%s), want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}

	// Token problems take precedence over the disabled policy.
	if rec := healthPost(t, h, "svc", "plain", `{"sequence":1,"status":"pass"}`, ""); rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("missing token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	if rec := healthPost(t, h, "svc", "plain", `{"sequence":1,"status":"pass"}`, "wrong"); rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("wrong token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// Valid token against an instance without a policy.
	if rec := healthPost(t, h, "svc", "plain", `{"sequence":1,"status":"pass"}`, disabled.LeaseToken); rec.Code != http.StatusConflict || errorCode(t, rec) != "health_check_disabled" {
		t.Fatalf("no policy = %d (%s), want 409 health_check_disabled", rec.Code, errorCode(t, rec))
	}

	// Error precedence: an invalid body still loses to 404, the token check
	// and the disabled-policy check in that order.
	if rec := healthPost(t, h, "svc", "ghost", `not-json`, reg.LeaseToken); rec.Code != http.StatusNotFound {
		t.Fatalf("invalid body on missing instance = %d, want 404", rec.Code)
	}
	if rec := healthPost(t, h, "svc", "plain", `not-json`, ""); rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("invalid body with missing token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	if rec := healthPost(t, h, "svc", "plain", `not-json`, disabled.LeaseToken); rec.Code != http.StatusConflict || errorCode(t, rec) != "health_check_disabled" {
		t.Fatalf("invalid body on disabled policy = %d (%s), want 409 health_check_disabled", rec.Code, errorCode(t, rec))
	}
}

func TestHealthReportBodyValidation(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 60, 2, 2))
	cases := []struct {
		name string
		body string
	}{
		{"empty body", ``},
		{"malformed json", `{`},
		{"missing sequence", `{"status":"pass"}`},
		{"missing status", `{"sequence":1}`},
		{"negative sequence", `{"sequence":-1,"status":"pass"}`},
		{"bad status", `{"sequence":1,"status":"ok"}`},
		{"unknown field", `{"sequence":1,"status":"pass","extra":1}`},
		{"fractional sequence", `{"sequence":1.5,"status":"pass"}`},
		{"non-string status", `{"sequence":1,"status":true}`},
		{"trailing data", `{"sequence":1,"status":"pass"} {}`},
		{"null body", `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := healthPost(t, h, "svc", "i1", tc.body, reg.LeaseToken)
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
				t.Fatalf("status = %d (%s), want 400 validation_error", rec.Code, errorCode(t, rec))
			}
			requireJSON(t, rec)
		})
	}
	// Bad path names are also validation errors.
	rec := healthPost(t, h, "bad!", "i1", `{"sequence":1,"status":"pass"}`, reg.LeaseToken)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad service name = %d, want 400", rec.Code)
	}
}

func TestHealthMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := do(t, h, method, "/v1/services/svc/instances/i1/health", "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s = %d, want 405", method, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
			t.Fatalf("%s Allow = %q, want POST", method, allow)
		}
		if code := errorCode(t, rec); code != "method_not_allowed" {
			t.Fatalf("%s error code = %q, want method_not_allowed", method, code)
		}
	}
}

func TestHeartbeatDoesNotChangeHealth(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 300, 1, 1))
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"fail"}`, reg.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("fail = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatal("threshold 1 fail should remove the instance immediately")
	}
	// Heartbeats still succeed on an unhealthy instance but never restore it.
	for i := 0; i < 2; i++ {
		rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
			map[string]string{leaseHeader: reg.LeaseToken})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("heartbeat on unhealthy = %d, want 204", rec.Code)
		}
		if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
			t.Fatal("heartbeat restored an unhealthy instance")
		}
	}
	// Only a successful health report (threshold 1) brings it back.
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":2,"status":"pass"}`, reg.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("recovery pass = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 {
		t.Fatal("pass did not restore the instance")
	}
}

func TestReregisterResetsHealth(t *testing.T) {
	h := Handler()
	body := policyBody("http://a:1", "v1", "z1", 10, 300, 1, 1)
	_, first := mustRegister(t, h, "svc", "i1", body)
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":42,"status":"fail"}`, first.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("fail = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatal("instance should be unhealthy")
	}

	// Re-registering under the same name resets status, counters and sequence.
	code, second := mustRegister(t, h, "svc", "i1", body)
	if code != http.StatusOK || second.HealthStatus != healthStatusHealthy {
		t.Fatalf("reregister = %d status=%q, want 200 healthy", code, second.HealthStatus)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 {
		t.Fatal("reregister did not restore routing")
	}
	// The old token is dead on the health endpoint too.
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"pass"}`, first.LeaseToken); rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("old token report = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// Sequence restarted from scratch: a low sequence is accepted, not stale;
	// a single fail trips threshold 1 again proving counters were zeroed.
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"fail"}`, second.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("post-reset fail = %d, want 204", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatal("reset counters did not take effect")
	}
}

func TestResolveExcludesUnhealthyWithoutFallback(t *testing.T) {
	h := Handler()
	// i1 is health-checked on v1; i2 is a healthy v2 instance that must NOT
	// be used as a fallback once i1 goes unhealthy.
	_, r1 := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 300, 1, 1))
	mustRegister(t, h, "svc", "i2", policyBody("http://b:1", "v2", "z1", 10, 300, 1, 1))
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k").Instance.Instance; got != "i1" && got != "i2" {
		t.Fatalf("unexpected choice %q", got)
	}
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":1,"status":"fail"}`, r1.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("fail = %d, want 204", rec.Code)
	}
	// Unfiltered snapshot now only contains the v2 instance.
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k").Instance.Instance; got != "i2" {
		t.Fatalf("after i1 unhealthy choice = %q, want i2", got)
	}
	// The version=v1 filter yields nothing routable: 503, never the v2 fallback.
	if rec := resolve(t, h, "/v1/resolve/svc?key=k&version=v1"); rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("version-filtered resolve = %d (%s), want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
	// Discovery under the same filter is a 200 empty list.
	if names := discoverNames(t, h, "svc", "?version=v1"); len(names) != 0 {
		t.Fatalf("filtered discovery = %v, want []", names)
	}
	// Recovering i1 makes it resolvable again under the v1 filter.
	if rec := healthPost(t, h, "svc", "i1", `{"sequence":2,"status":"pass"}`, r1.LeaseToken); rec.Code != http.StatusNoContent {
		t.Fatalf("recovery = %d, want 204", rec.Code)
	}
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k&version=v1").Instance.Instance; got != "i1" {
		t.Fatalf("after recovery choice = %q, want i1", got)
	}
}

func TestHealthConcurrentReports(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", policyBody("http://a:1", "v1", "z1", 10, 300, 5, 5))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				seq := i*50 + j + 1
				status := "pass"
				if (i+j)%2 == 0 {
					status = "fail"
				}
				rec := healthPost(t, h, "svc", "i1", fmt.Sprintf(`{"sequence":%d,"status":%q}`, seq, status), reg.LeaseToken)
				if rec.Code != http.StatusNoContent &&
					errorCode(t, rec) != "stale_health_report" &&
					errorCode(t, rec) != "health_report_conflict" {
					t.Errorf("report = %d (%s)", rec.Code, errorCode(t, rec))
					return
				}
				if rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil); rec.Code != http.StatusOK {
					t.Errorf("discovery = %d", rec.Code)
					return
				}
				if rec := resolve(t, h, "/v1/resolve/svc?key=k"); rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable {
					t.Errorf("resolve = %d", rec.Code)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
