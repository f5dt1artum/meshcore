package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func do(t *testing.T, h http.Handler, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("error response is not JSON: %v (body %q)", err, rec.Body.String())
	}
	return payload.Error.Code
}

func requireJSON(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func registerBody(endpoint, version, zone string, weight, ttl int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d}`,
		endpoint, version, zone, weight, ttl)
}

func mustRegister(t *testing.T, h http.Handler, service, name, body string) (int, registerResponse) {
	t.Helper()
	rec := do(t, h, http.MethodPut, "/v1/services/"+service+"/instances/"+name, body, nil)
	var resp registerResponse
	if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("register response is not JSON: %v", err)
		}
	}
	return rec.Code, resp
}

func TestRegisterReturnsCreated(t *testing.T) {
	h := Handler()
	body := `{"endpoint":"http://10.0.0.1:9000","version":"v1","zone":"z1","weight":50,"ttlSeconds":30,"metadata":{"team":"core"}}`
	before := time.Now()
	code, resp := mustRegister(t, h, "payments", "pay-1", body)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", code, http.StatusCreated)
	}
	if resp.Service != "payments" || resp.Instance != "pay-1" || resp.Endpoint != "http://10.0.0.1:9000" ||
		resp.Version != "v1" || resp.Zone != "z1" || resp.Weight != 50 || resp.TTLSeconds != 30 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Metadata["team"] != "core" {
		t.Fatalf("metadata not echoed: %+v", resp.Metadata)
	}
	if resp.LeaseToken == "" {
		t.Fatal("leaseToken is empty")
	}
	expiresAt, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("expiresAt %q is not RFC3339: %v", resp.ExpiresAt, err)
	}
	if expiresAt.Before(before.Add(29*time.Second)) || expiresAt.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("expiresAt %s not ~30s in the future", expiresAt)
	}
}

func TestRegisterOverwriteLiveInstanceRotatesToken(t *testing.T) {
	h := Handler()
	body := registerBody("http://a:1", "v1", "z1", 10, 60)
	_, first := mustRegister(t, h, "svc", "i1", body)
	code, second := mustRegister(t, h, "svc", "i1", body)
	if code != http.StatusOK {
		t.Fatalf("overwrite status = %d, want %d", code, http.StatusOK)
	}
	if second.LeaseToken == "" || second.LeaseToken == first.LeaseToken {
		t.Fatalf("overwrite did not rotate token: %q -> %q", first.LeaseToken, second.LeaseToken)
	}
	// The old token is dead immediately.
	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: first.LeaseToken})
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("old token heartbeat = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	// The new token works.
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: second.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("new token heartbeat = %d, want 204", rec.Code)
	}
}

func TestRegisterOverwriteExpiredReturnsCreated(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	body := registerBody("http://a:1", "v1", "z1", 10, 10)
	if code, _ := mustRegister(t, h, "svc", "i1", body); code != http.StatusCreated {
		t.Fatalf("first register = %d, want 201", code)
	}
	now = now.Add(11 * time.Second) // lease expired
	if code, _ := mustRegister(t, h, "svc", "i1", body); code != http.StatusCreated {
		t.Fatalf("overwrite of expired instance = %d, want 201", code)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	_, reg := mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 30))

	now = now.Add(20 * time.Second)
	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat = %d, want 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("heartbeat returned a body: %q", rec.Body.String())
	}

	// t=40: past the original 30s expiry, but the renewal keeps it alive.
	now = now.Add(20 * time.Second)
	if names := discoverNames(t, h, "svc", ""); len(names) != 1 || names[0] != "i1" {
		t.Fatalf("after renewal discovery = %v, want [i1]", names)
	}
	// t=51: past the renewed expiry (t=20 + 30s).
	now = now.Add(11 * time.Second)
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatalf("after expiry discovery = %v, want []", names)
	}
	// An expired lease cannot be revived.
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("heartbeat after expiry = %d (%s), want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
}

func TestHeartbeatErrors(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))

	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/ghost/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("unknown instance = %d (%s), want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
	requireJSON(t, rec)

	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "", nil)
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("missing token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: "wrong"})
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("wrong token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}
}

func TestDeregister(t *testing.T) {
	h := Handler()
	_, reg := mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))

	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: "wrong"})
	if rec.Code != http.StatusConflict || errorCode(t, rec) != "lease_conflict" {
		t.Fatalf("delete with wrong token = %d (%s), want 409 lease_conflict", rec.Code, errorCode(t, rec))
	}

	rec = do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rec.Code)
	}
	// The deregistered lease is gone for good.
	rec = do(t, h, http.MethodDelete, "/v1/services/svc/instances/i1", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "instance_not_found" {
		t.Fatalf("second delete = %d (%s), want 404 instance_not_found", rec.Code, errorCode(t, rec))
	}
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("heartbeat after delete = %d, want 404", rec.Code)
	}
	if names := discoverNames(t, h, "svc", ""); len(names) != 0 {
		t.Fatalf("discovery after delete = %v, want []", names)
	}
}

func discoverNames(t *testing.T, h http.Handler, service, query string) []string {
	t.Helper()
	rec := do(t, h, http.MethodGet, "/v1/discovery/"+service+query, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery = %d, want 200", rec.Code)
	}
	var resp discoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("discovery response is not JSON: %v", err)
	}
	names := make([]string, 0, len(resp.Instances))
	for _, inst := range resp.Instances {
		names = append(names, inst.Instance)
	}
	return names
}

func TestDiscoverySortingFiltersAndEmpty(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()

	mustRegister(t, h, "svc", "b-inst", registerBody("http://b:1", "v2", "z1", 10, 60))
	mustRegister(t, h, "svc", "a-inst", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "c-inst", registerBody("http://c:1", "v1", "z2", 10, 60))
	mustRegister(t, h, "other", "x-inst", registerBody("http://x:1", "v1", "z1", 10, 60))
	// One instance that expires before the snapshot.
	mustRegister(t, h, "svc", "d-inst", registerBody("http://d:1", "v1", "z1", 10, 10))
	now = now.Add(11 * time.Second)

	if got, want := discoverNames(t, h, "svc", ""), []string{"a-inst", "b-inst", "c-inst"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("discovery = %v, want %v (sorted, expired excluded)", got, want)
	}
	if got := discoverNames(t, h, "svc", "?version=v1"); strings.Join(got, ",") != "a-inst,c-inst" {
		t.Fatalf("version filter = %v, want [a-inst c-inst]", got)
	}
	if got := discoverNames(t, h, "svc", "?zone=z2"); strings.Join(got, ",") != "c-inst" {
		t.Fatalf("zone filter = %v, want [c-inst]", got)
	}
	if got := discoverNames(t, h, "svc", "?version=v1&zone=z1"); strings.Join(got, ",") != "a-inst" {
		t.Fatalf("combined filter = %v, want [a-inst]", got)
	}
	// Unknown service and no-match filters both return 200 with an empty list.
	for _, target := range []string{"/v1/discovery/unknown", "/v1/discovery/svc?version=v9"} {
		rec := do(t, h, http.MethodGet, target, "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", target, rec.Code)
		}
		requireJSON(t, rec)
		if !strings.Contains(rec.Body.String(), `"instances":[]`) {
			t.Fatalf("GET %s body = %s, want empty instances array", target, rec.Body.String())
		}
	}
	// Discovery never leaks lease tokens.
	rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)
	if strings.Contains(rec.Body.String(), "leaseToken") {
		t.Fatalf("discovery leaks lease tokens: %s", rec.Body.String())
	}
}

func TestValidationErrors(t *testing.T) {
	h := Handler()
	valid := registerBody("http://a:1", "v1", "z1", 10, 30)
	cases := []struct {
		name   string
		method string
		target string
		body   string
	}{
		{"bad service char", "PUT", "/v1/services/bad!/instances/i1", valid},
		{"bad instance char", "PUT", "/v1/services/svc/instances/i!1", valid},
		{"service too long", "PUT", "/v1/services/" + strings.Repeat("a", 65) + "/instances/i1", valid},
		{"empty endpoint", "PUT", "/v1/services/svc/instances/i1", registerBody("", "v1", "z1", 10, 30)},
		{"empty version", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "", "z1", 10, 30)},
		{"empty zone", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "v1", "", 10, 30)},
		{"weight zero", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "v1", "z1", 0, 30)},
		{"weight too big", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "v1", "z1", 101, 30)},
		{"ttl zero", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "v1", "z1", 10, 0)},
		{"ttl too big", "PUT", "/v1/services/svc/instances/i1", registerBody("http://a:1", "v1", "z1", 10, 301)},
		{"malformed JSON", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":`},
		{"empty body", "PUT", "/v1/services/svc/instances/i1", ``},
		{"trailing data", "PUT", "/v1/services/svc/instances/i1", valid + ` {}`},
		{"unknown field", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"bogus":1}`},
		{"fractional weight", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10.5,"ttlSeconds":30}`},
		{"empty metadata key", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"metadata":{"":"x"}}`},
		{"empty metadata value", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"metadata":{"k":""}}`},
		{"non-string metadata", "PUT", "/v1/services/svc/instances/i1", `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":30,"metadata":{"k":1}}`},
		{"heartbeat bad name", "POST", "/v1/services/svc/instances/bad!/heartbeat", ""},
		{"discovery bad name", "GET", "/v1/discovery/bad%20name", ""},
		{"discovery unknown query", "GET", "/v1/discovery/svc?foo=bar", ""},
		{"discovery empty version", "GET", "/v1/discovery/svc?version=", ""},
		{"discovery duplicate query", "GET", "/v1/discovery/svc?zone=z1&zone=z2", ""},
		{"discovery malformed query", "GET", "/v1/discovery/svc?version=%zz", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, tc.method, tc.target, tc.body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
			}
			requireJSON(t, rec)
			if code := errorCode(t, rec); code != "validation_error" {
				t.Fatalf("error code = %q, want validation_error", code)
			}
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := Handler()
	cases := []struct {
		method string
		target string
		allow  string
	}{
		{"GET", "/v1/services/svc/instances/i1", "PUT, DELETE"},
		{"POST", "/v1/services/svc/instances/i1", "PUT, DELETE"},
		{"PUT", "/v1/services/svc/instances/i1/heartbeat", "POST"},
		{"DELETE", "/v1/services/svc/instances/i1/heartbeat", "POST"},
		{"POST", "/v1/discovery/svc", "GET"},
		{"PUT", "/v1/discovery/svc", "GET"},
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

func TestHealthzUnaffectedByRegistrations(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	rec := do(t, h, http.MethodGet, "/healthz", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("health payload is not JSON: %v", err)
	}
	if payload["status"] != "ok" || payload["service"] != "meshcore" || payload["version"] != Version {
		t.Fatalf("unexpected payload: %v", payload)
	}
	// Non-GET handling is unchanged from the baseline.
	rec = do(t, h, http.MethodPost, "/healthz", "", nil)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST /healthz = %d Allow=%q, want 405 Allow=GET", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestConcurrentAccess(t *testing.T) {
	h := Handler()
	body := registerBody("http://a:1", "v1", "z1", 10, 60)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("inst-%d", i%4)
			base := "/v1/services/svc/instances/" + name
			for j := 0; j < 50; j++ {
				rec := do(t, h, http.MethodPut, base, body, nil)
				if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
					t.Errorf("register = %d", rec.Code)
					return
				}
				var resp registerResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Errorf("register response not JSON: %v", err)
					return
				}
				hdr := map[string]string{leaseHeader: resp.LeaseToken}
				if rec := do(t, h, http.MethodPost, base+"/heartbeat", "", hdr); rec.Code != http.StatusNoContent && rec.Code != http.StatusConflict && rec.Code != http.StatusNotFound {
					t.Errorf("heartbeat = %d", rec.Code)
				}
				if rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil); rec.Code != http.StatusOK {
					t.Errorf("discovery = %d", rec.Code)
				}
				if rec := do(t, h, http.MethodDelete, base, "", hdr); rec.Code != http.StatusNoContent && rec.Code != http.StatusNotFound && rec.Code != http.StatusConflict {
					t.Errorf("delete = %d", rec.Code)
				}
			}
		}(i)
	}
	wg.Wait()
}
