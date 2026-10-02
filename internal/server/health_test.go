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

func registerWithPolicyBody(endpoint, version, zone string, weight, ttl, failThreshold, successThreshold int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"healthPolicy":{"failureThreshold":%d,"successThreshold":%d}}`,
		endpoint, version, zone, weight, ttl, failThreshold, successThreshold)
}

func mustRegisterWithPolicy(t *testing.T, h http.Handler, service, name string, failThreshold, successThreshold int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerWithPolicyBody("http://10.0.0.1:9000", "v1", "z1", 50, 60, failThreshold, successThreshold))
	if code != http.StatusCreated {
		t.Fatalf("register with policy: status = %d, want %d", code, http.StatusCreated)
	}
	return resp
}

func reportHealth(t *testing.T, h http.Handler, service, name, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	headers := map[string]string{}
	if token != "" {
		headers[leaseHeader] = token
	}
	return do(t, h, http.MethodPost, "/v1/services/"+service+"/instances/"+name+"/health", body, headers)
}

func reportSeq(t *testing.T, h http.Handler, service, name, token string, seq int, status string) int {
	t.Helper()
	rec := reportHealth(t, h, service, name, token, fmt.Sprintf(`{"sequence":%d,"status":%q}`, seq, status))
	return rec.Code
}

func discoveryInstances(t *testing.T, h http.Handler, service string) []instanceView {
	t.Helper()
	rec := do(t, h, http.MethodGet, "/v1/discovery/"+service, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery status = %d, want %d", rec.Code, http.StatusOK)
	}
	var resp discoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("discovery response is not JSON: %v", err)
	}
	return resp.Instances
}

func TestRegisterHealthPolicyStatus(t *testing.T) {
	h := Handler()

	code, resp := mustRegister(t, h, "svc", "a", registerBody("http://10.0.0.1:9000", "v1", "z1", 50, 30))
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", code, http.StatusCreated)
	}
	if resp.HealthStatus != healthDisabled {
		t.Fatalf("healthStatus = %q, want %q", resp.HealthStatus, healthDisabled)
	}

	resp = mustRegisterWithPolicy(t, h, "svc", "b", 2, 3)
	if resp.HealthStatus != healthHealthy {
		t.Fatalf("healthStatus = %q, want %q", resp.HealthStatus, healthHealthy)
	}
}

func TestRegisterHealthPolicyValidation(t *testing.T) {
	h := Handler()
	cases := []string{
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":2}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"successThreshold":2}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":0,"successThreshold":1}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":11,"successThreshold":1}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":1,"successThreshold":11}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":1.5,"successThreshold":1}}`,
		`{"endpoint":"e","version":"v1","zone":"z1","weight":1,"ttlSeconds":10,"healthPolicy":{"failureThreshold":1,"successThreshold":1,"extra":1}}`,
	}
	for i, body := range cases {
		rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i"+fmt.Sprint(i), body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("case %d: status = %d code = %q, want 400 validation_error", i, rec.Code, errorCode(t, rec))
		}
	}
}

func TestHealthReportTransitionsAndFiltering(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 2, 2)
	mustRegisterWithPolicy(t, h, "svc", "b", 2, 2)

	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 1, "fail"); got != http.StatusNoContent {
		t.Fatalf("fail report: status = %d, want 204", got)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 2 {
		t.Fatalf("after 1 fail: %d instances discovered, want 2", n)
	}
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 2, "fail"); got != http.StatusNoContent {
		t.Fatalf("fail report: status = %d, want 204", got)
	}
	views := discoveryInstances(t, h, "svc")
	if len(views) != 1 || views[0].Instance != "b" {
		t.Fatalf("after threshold fails: discovered %+v, want only b", views)
	}

	// Resolve must not select the unhealthy instance.
	rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("resolve status = %d, want 200", rec.Code)
	}
	var resolved resolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resolved); err != nil {
		t.Fatalf("resolve response is not JSON: %v", err)
	}
	if resolved.Instance.Instance != "b" {
		t.Fatalf("resolved %q, want b", resolved.Instance.Instance)
	}

	// One pass is not enough to recover (successThreshold = 2).
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 3, "pass"); got != http.StatusNoContent {
		t.Fatalf("pass report: status = %d, want 204", got)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 1 {
		t.Fatalf("after 1 pass: %d instances discovered, want 1", n)
	}
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 4, "pass"); got != http.StatusNoContent {
		t.Fatalf("pass report: status = %d, want 204", got)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 2 {
		t.Fatalf("after recovery: %d instances discovered, want 2", n)
	}
}

func TestHealthReportConsecutiveReset(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 2, 1)

	// fail, pass, fail: the pass resets the consecutive-fail counter, so the
	// second fail must not trip the threshold of 2.
	reportSeq(t, h, "svc", "a", reg.LeaseToken, 1, "fail")
	reportSeq(t, h, "svc", "a", reg.LeaseToken, 2, "pass")
	reportSeq(t, h, "svc", "a", reg.LeaseToken, 3, "fail")
	if n := len(discoveryInstances(t, h, "svc")); n != 1 {
		t.Fatalf("non-consecutive fails tripped threshold: %d instances, want 1", n)
	}
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 4, "fail"); got != http.StatusNoContent {
		t.Fatalf("fail report: status = %d, want 204", got)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 0 {
		t.Fatalf("consecutive fails did not trip threshold: %d instances, want 0", n)
	}
}

func TestHealthReportSequenceSemantics(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)

	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 5, "pass"); got != http.StatusNoContent {
		t.Fatalf("first report: status = %d, want 204", got)
	}
	// Same sequence and status: idempotent.
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 5, "pass"); got != http.StatusNoContent {
		t.Fatalf("idempotent replay: status = %d, want 204", got)
	}
	// Same sequence, different status: conflict.
	rec := reportHealth(t, h, "svc", "a", reg.LeaseToken, `{"sequence":5,"status":"fail"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "health_report_conflict" {
		t.Fatalf("conflicting replay: status = %d code = %q, want 409 health_report_conflict", rec.Code, errorCode(t, rec))
	}
	// Smaller sequence: stale.
	rec = reportHealth(t, h, "svc", "a", reg.LeaseToken, `{"sequence":4,"status":"pass"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "stale_health_report" {
		t.Fatalf("stale report: status = %d code = %q, want 409 stale_health_report", rec.Code, errorCode(t, rec))
	}
	// Larger sequence advances again.
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 6, "fail"); got != http.StatusNoContent {
		t.Fatalf("advancing report: status = %d, want 204", got)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 0 {
		t.Fatalf("fail with threshold 1 did not remove instance: %d discovered", n)
	}
}

func TestHealthReportErrors(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)
	_, disabled := mustRegister(t, h, "svc", "b", registerBody("http://10.0.0.2:9000", "v1", "z1", 50, 60))

	// Unknown instance.
	rec := reportHealth(t, h, "svc", "ghost", reg.LeaseToken, `{"sequence":1,"status":"pass"}`)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("missing instance: status = %d code = %q, want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
	// Missing token.
	rec = reportHealth(t, h, "svc", "a", "", `{"sequence":1,"status":"pass"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("missing token: status = %d code = %q, want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// Wrong token.
	rec = reportHealth(t, h, "svc", "a", disabled.LeaseToken, `{"sequence":1,"status":"pass"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("wrong token: status = %d code = %q, want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// No policy configured.
	rec = reportHealth(t, h, "svc", "b", disabled.LeaseToken, `{"sequence":1,"status":"pass"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "health_check_disabled" {
		t.Fatalf("disabled policy: status = %d code = %q, want 409 health_check_disabled", rec.Code, errorCode(t, rec))
	}
	// Invalid bodies.
	bodies := []string{
		``,
		`not json`,
		`{"sequence":1}`,
		`{"status":"pass"}`,
		`{"sequence":-1,"status":"pass"}`,
		`{"sequence":1.5,"status":"pass"}`,
		`{"sequence":1,"status":"ok"}`,
		`{"sequence":1,"status":"pass","extra":1}`,
		`{"sequence":"1","status":"pass"}`,
	}
	for i, body := range bodies {
		rec = reportHealth(t, h, "svc", "a", reg.LeaseToken, body)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("body %d: status = %d code = %q, want 400 validation_error", i, rec.Code, errorCode(t, rec))
		}
	}
}

func TestHealthReportMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := do(t, h, method, "/v1/services/svc/instances/a/health", "", nil)
		if rec.Code != http.StatusMethodNotAllowed || errorCode(t, rec) != "method_not_allowed" {
			t.Fatalf("%s: status = %d code = %q, want 405 method_not_allowed", method, rec.Code, errorCode(t, rec))
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
			t.Fatalf("%s: Allow = %q, want POST", method, allow)
		}
	}
}

func TestHealthReportDoesNotRenewLease(t *testing.T) {
	s := newServer()
	now := time.Now()
	s.now = func() time.Time { return now }
	h := s.handler()

	reg := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)
	if got := reportSeq(t, h, "svc", "a", reg.LeaseToken, 1, "pass"); got != http.StatusNoContent {
		t.Fatalf("report: status = %d, want 204", got)
	}
	// Advance past the TTL: the report must not have extended the lease.
	now = now.Add(61 * time.Second)
	rec := reportHealth(t, h, "svc", "a", reg.LeaseToken, `{"sequence":2,"status":"pass"}`)
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("expired instance: status = %d code = %q, want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
}

func TestHeartbeatDoesNotChangeHealth(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)
	reportSeq(t, h, "svc", "a", reg.LeaseToken, 1, "fail")
	if n := len(discoveryInstances(t, h, "svc")); n != 0 {
		t.Fatalf("instance should be unhealthy: %d discovered", n)
	}
	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/a/heartbeat", "", map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat: status = %d, want 204", rec.Code)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 0 {
		t.Fatalf("heartbeat changed health: %d discovered, want 0", n)
	}
}

func TestReregisterResetsHealth(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)
	reportSeq(t, h, "svc", "a", reg.LeaseToken, 7, "fail")
	if n := len(discoveryInstances(t, h, "svc")); n != 0 {
		t.Fatalf("instance should be unhealthy: %d discovered", n)
	}

	// Re-register: healthy again, old token invalid, sequence state reset.
	code, resp := mustRegister(t, h, "svc", "a",
		registerWithPolicyBody("http://10.0.0.1:9000", "v1", "z1", 50, 60, 1, 1))
	if code != http.StatusOK {
		t.Fatalf("re-register: status = %d, want 200", code)
	}
	if resp.HealthStatus != healthHealthy {
		t.Fatalf("healthStatus = %q, want healthy", resp.HealthStatus)
	}
	if n := len(discoveryInstances(t, h, "svc")); n != 1 {
		t.Fatalf("re-registered instance not healthy: %d discovered", n)
	}
	rec := reportHealth(t, h, "svc", "a", reg.LeaseToken, `{"sequence":8,"status":"pass"}`)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("old token: status = %d code = %q, want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// Sequence 7 was used before re-registration; the reset must accept it.
	if got := reportSeq(t, h, "svc", "a", resp.LeaseToken, 7, "pass"); got != http.StatusNoContent {
		t.Fatalf("sequence reuse after reset: status = %d, want 204", got)
	}
}

func TestResolveNoFallbackWhenAllUnhealthy(t *testing.T) {
	h := Handler()
	a := mustRegisterWithPolicy(t, h, "svc", "a", 1, 1)
	// A second version stays healthy but must not be fallen back to.
	mustRegister(t, h, "svc", "b", registerBody("http://10.0.0.2:9000", "v2", "z1", 50, 60))

	reportSeq(t, h, "svc", "a", a.LeaseToken, 1, "fail")

	rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k&version=v1", "", nil)
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("resolve v1: status = %d code = %q, want 503 no_available_instance", rec.Code, errorCode(t, rec))
	}
	views := discoveryInstances(t, h, "svc")
	if len(views) != 1 || views[0].Instance != "b" {
		t.Fatalf("discovery: %+v, want only b", views)
	}
	rec = do(t, h, http.MethodGet, "/v1/discovery/svc?version=v1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery v1: status = %d, want 200", rec.Code)
	}
	var resp discoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("discovery response is not JSON: %v", err)
	}
	if len(resp.Instances) != 0 {
		t.Fatalf("discovery v1: %+v, want empty list", resp.Instances)
	}
}

func TestHealthViewInDiscoveryAndResolve(t *testing.T) {
	h := Handler()
	mustRegisterWithPolicy(t, h, "svc", "a", 2, 2)
	mustRegister(t, h, "svc", "b", registerBody("http://10.0.0.2:9000", "v1", "z1", 50, 60))

	views := discoveryInstances(t, h, "svc")
	status := map[string]string{}
	for _, v := range views {
		status[v.Instance] = v.HealthStatus
	}
	if status["a"] != healthHealthy || status["b"] != healthDisabled {
		t.Fatalf("discovery healthStatus = %v, want a=healthy b=disabled", status)
	}

	rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("resolve status = %d, want 200", rec.Code)
	}
	var resolved resolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resolved); err != nil {
		t.Fatalf("resolve response is not JSON: %v", err)
	}
	want := status[resolved.Instance.Instance]
	if resolved.Instance.HealthStatus != want {
		t.Fatalf("resolve healthStatus = %q, want %q", resolved.Instance.HealthStatus, want)
	}
}

func TestHealthReportConcurrent(t *testing.T) {
	h := Handler()
	reg := mustRegisterWithPolicy(t, h, "svc", "a", 3, 3)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for seq := 1; seq <= 20; seq++ {
				status := "pass"
				if (seq+i)%2 == 0 {
					status = "fail"
				}
				reportSeq(t, h, "svc", "a", reg.LeaseToken, seq, status)
			}
			discoveryInstances(t, h, "svc")
			do(t, h, http.MethodPost, "/v1/services/svc/instances/a/heartbeat", "", map[string]string{leaseHeader: reg.LeaseToken})
		}(i)
	}
	wg.Wait()
}
