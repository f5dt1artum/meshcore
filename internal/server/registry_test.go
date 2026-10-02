package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a concurrency-safe controllable time source.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

type regPayload struct {
	Endpoint   string            `json:"endpoint"`
	Version    string            `json:"version"`
	Zone       string            `json:"zone"`
	Weight     int               `json:"weight"`
	TTLSeconds int               `json:"ttlSeconds"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

func validPayload() regPayload {
	return regPayload{
		Endpoint:   "10.0.0.1:8080",
		Version:    "v1.2.3",
		Zone:       "cn-east-1a",
		Weight:     50,
		TTLSeconds: 60,
		Metadata:   map[string]string{"k": "v"},
	}
}

type regResult struct {
	Endpoint   string            `json:"endpoint"`
	Version    string            `json:"version"`
	Zone       string            `json:"zone"`
	Weight     int               `json:"weight"`
	TTLSeconds int               `json:"ttlSeconds"`
	Metadata   map[string]string `json:"metadata"`
	LeaseToken string            `json:"leaseToken"`
	ExpiresAt  string            `json:"expiresAt"`
}

func instancePath(service, instance string) string {
	return "/v1/services/" + service + "/instances/" + instance
}

func heartbeatPath(service, instance string) string {
	return instancePath(service, instance) + "/heartbeat"
}

func do(t *testing.T, h http.Handler, method, target, token string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, bytes.NewReader(body))
	if token != "" {
		r.Header.Set(LeaseHeader, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func registerReq(t *testing.T, h http.Handler, service, instance string, p regPayload) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPut, instancePath(service, instance), "", mustJSON(t, p))
}

func TestRegisterCreatesAndOverwrites(t *testing.T) {
	clk := newFakeClock()
	reg := newRegistry()
	reg.now = clk.now
	h := newHandler(reg)

	rec := registerReq(t, h, "billing", "node-1", validPayload())
	if rec.Code != http.StatusCreated {
		t.Fatalf("first register status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var created regResult
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.LeaseToken == "" {
		t.Fatal("leaseToken must be non-empty")
	}
	wantExpiry := clk.now().Add(60 * time.Second)
	gotExpiry, err := time.Parse(time.RFC3339, created.ExpiresAt)
	if err != nil {
		t.Fatalf("expiresAt is not RFC3339: %v", err)
	}
	if !gotExpiry.Equal(wantExpiry) {
		t.Fatalf("expiresAt = %v, want %v", gotExpiry, wantExpiry)
	}
	if created.Endpoint != "10.0.0.1:8080" || created.Weight != 50 ||
		created.TTLSeconds != 60 || created.Metadata["k"] != "v" {
		t.Fatalf("unexpected echoed fields: %+v", created)
	}
	firstToken := created.LeaseToken

	// Overwriting a live instance returns 200 and issues a new token.
	p2 := validPayload()
	p2.Endpoint = "10.0.0.2:8080"
	rec = registerReq(t, h, "billing", "node-1", p2)
	if rec.Code != http.StatusOK {
		t.Fatalf("overwrite status = %d, want 200", rec.Code)
	}
	var updated regResult
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if updated.LeaseToken == "" || updated.LeaseToken == firstToken {
		t.Fatal("overwrite must issue a fresh non-empty token")
	}

	// The old token is rejected while the instance is alive.
	rec = do(t, h, http.MethodPost, heartbeatPath("billing", "node-1"), firstToken, nil)
	assertError(t, rec, http.StatusConflict, "lease_conflict")
}

func TestHeartbeatRenewsFromAcceptance(t *testing.T) {
	clk := newFakeClock()
	reg := newRegistry()
	reg.now = clk.now
	h := newHandler(reg)

	rec := registerReq(t, h, "svc", "i-1", validPayload())
	token := decodeToken(t, rec)

	clk.add(59 * time.Second)
	rec = do(t, h, http.MethodPost, heartbeatPath("svc", "i-1"), token, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat = %d, want 204", rec.Code)
	}

	// Renewal pushed the deadline 60s past acceptance; the old deadline would
	// have expired one second later.
	clk.add(1 * time.Second)
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 1 {
		t.Fatal("instance should still be visible after renewed lease")
	}
	clk.add(58 * time.Second)
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 1 {
		t.Fatal("instance should be visible right before its new deadline")
	}
	clk.add(2 * time.Second)
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 0 {
		t.Fatal("expired instance must disappear from discovery")
	}
	// Expired instance is reported not found on heartbeat and delete.
	assertError(t, do(t, h, http.MethodPost, heartbeatPath("svc", "i-1"), token, nil),
		http.StatusNotFound, "instance_not_found")
	assertError(t, do(t, h, http.MethodDelete, instancePath("svc", "i-1"), token, nil),
		http.StatusNotFound, "instance_not_found")
}

func TestExpiredSlotCanBeRegisteredAgain(t *testing.T) {
	clk := newFakeClock()
	reg := newRegistry()
	reg.now = clk.now
	h := newHandler(reg)

	decodeToken(t, registerReq(t, h, "svc", "i", validPayload()))
	clk.add(61 * time.Second)
	// Registering onto an expired slot is a fresh 201, not an overwrite.
	rec := registerReq(t, h, "svc", "i", validPayload())
	if rec.Code != http.StatusCreated {
		t.Fatalf("register after expiry = %d, want 201", rec.Code)
	}
}

func TestLeaseConflictCases(t *testing.T) {
	h := newHandler(newRegistry())
	rec := registerReq(t, h, "svc", "i-1", validPayload())
	token := decodeToken(t, rec)

	// Missing token.
	assertError(t, do(t, h, http.MethodPost, heartbeatPath("svc", "i-1"), "", nil),
		http.StatusConflict, "lease_conflict")
	assertError(t, do(t, h, http.MethodDelete, instancePath("svc", "i-1"), "", nil),
		http.StatusConflict, "lease_conflict")

	// Wrong token does not remove the instance.
	assertError(t, do(t, h, http.MethodDelete, instancePath("svc", "i-1"), token+"x", nil),
		http.StatusConflict, "lease_conflict")
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 1 {
		t.Fatal("wrong-token delete must not remove the instance")
	}

	// Correct token releases it.
	rec = do(t, h, http.MethodDelete, instancePath("svc", "i-1"), token, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rec.Code)
	}
	assertError(t, do(t, h, http.MethodDelete, instancePath("svc", "i-1"), token, nil),
		http.StatusNotFound, "instance_not_found")
}

func TestUnknownInstanceNotFound(t *testing.T) {
	h := newHandler(newRegistry())
	assertError(t, do(t, h, http.MethodPost, heartbeatPath("nope", "ghost"), "t", nil),
		http.StatusNotFound, "instance_not_found")
	assertError(t, do(t, h, http.MethodDelete, instancePath("nope", "ghost"), "t", nil),
		http.StatusNotFound, "instance_not_found")
}

func TestDiscoverySnapshotFiltersAndSort(t *testing.T) {
	h := newHandler(newRegistry())
	mk := func(instance, version, zone string, weight int) {
		p := validPayload()
		p.Version, p.Zone, p.Weight = version, zone, weight
		p.Metadata = nil
		if rec := registerReq(t, h, "shop", instance, p); rec.Code != http.StatusCreated {
			t.Fatalf("register %s = %d: %s", instance, rec.Code, rec.Body.String())
		}
	}
	mk("zeta", "v1", "a", 1)
	mk("alpha", "v1", "b", 2)
	mk("mid", "v2", "a", 3)

	rec := do(t, h, http.MethodGet, "/v1/discovery/shop", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery = %d", rec.Code)
	}
	want := []string{"alpha", "mid", "zeta"}
	if got := instanceNames(rec); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("instances = %v, want lexicographic %v", got, want)
	}

	// Exact version filter.
	rec = do(t, h, http.MethodGet, "/v1/discovery/shop?version=v2", "", nil)
	if got := instanceNames(rec); len(got) != 1 || got[0] != "mid" {
		t.Fatalf("version filter = %v", got)
	}
	// Exact zone filter.
	rec = do(t, h, http.MethodGet, "/v1/discovery/shop?zone=a", "", nil)
	if got := instanceNames(rec); strings.Join(got, ",") != "mid,zeta" {
		t.Fatalf("zone filter = %v", got)
	}
	// Both filters.
	rec = do(t, h, http.MethodGet, "/v1/discovery/shop?version=v1&zone=a", "", nil)
	if got := instanceNames(rec); len(got) != 1 || got[0] != "zeta" {
		t.Fatalf("combined filter = %v", got)
	}
	// No match is still 200 with an empty list.
	rec = do(t, h, http.MethodGet, "/v1/discovery/shop?version=v9", "", nil)
	if rec.Code != http.StatusOK || countInstances(rec) != 0 {
		t.Fatalf("empty filter result = %d", rec.Code)
	}
	// Unknown service is 200 with an empty list.
	rec = do(t, h, http.MethodGet, "/v1/discovery/unknown", "", nil)
	if rec.Code != http.StatusOK || countInstances(rec) != 0 {
		t.Fatalf("unknown service = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestValidationErrors(t *testing.T) {
	h := newHandler(newRegistry())
	// Characters that cannot appear in a single path segment (or at all) are
	// validated directly; ServeMux rejects or normalizes such paths before a
	// handler runs.
	for _, name := range []string{"", "a/b", "has space"} {
		if validName(name) {
			t.Fatalf("name %q must be invalid", name)
		}
	}
	badNames := []string{"中文", "a@b", "exclamation!",
		"a.b:c", strings.Repeat("a", 65)}
	for _, name := range badNames {
		assertError(t, do(t, h, http.MethodPut,
			"/v1/services/"+name+"/instances/i", "", mustJSON(t, validPayload())),
			http.StatusBadRequest, "validation_error")
		assertError(t, do(t, h, http.MethodGet, "/v1/discovery/"+name, "", nil),
			http.StatusBadRequest, "validation_error")
	}
	assertError(t, registerReq(t, h, "svc", strings.Repeat("a", 65), validPayload()),
		http.StatusBadRequest, "validation_error")

	// Empty text fields.
	for _, mutate := range []func(*regPayload){
		func(p *regPayload) { p.Endpoint = "" },
		func(p *regPayload) { p.Version = "" },
		func(p *regPayload) { p.Zone = "" },
	} {
		p := validPayload()
		mutate(&p)
		assertError(t, registerReq(t, h, "svc", "i", p),
			http.StatusBadRequest, "validation_error")
	}
	// Out-of-range numbers.
	for _, mutate := range []func(*regPayload){
		func(p *regPayload) { p.Weight = 0 },
		func(p *regPayload) { p.Weight = 101 },
		func(p *regPayload) { p.TTLSeconds = 0 },
		func(p *regPayload) { p.TTLSeconds = 301 },
	} {
		p := validPayload()
		mutate(&p)
		assertError(t, registerReq(t, h, "svc", "i", p),
			http.StatusBadRequest, "validation_error")
	}
	// Malformed bodies.
	target := instancePath("svc", "i")
	for _, body := range [][]byte{
		nil,
		[]byte("not json"),
		[]byte(`{"endpoint":`),
		[]byte(`[]`),
		[]byte(`{"endpoint":"x","version":"v","zone":"z","weight":1,"ttlSeconds":1,"bogus":1}`),
		[]byte(`{"endpoint":"x","version":"v","zone":"z","weight":"high","ttlSeconds":1}`),
		[]byte(`{"endpoint":"x","version":"v","zone":"z","weight":1,"ttlSeconds":1} trailing`),
	} {
		assertError(t, do(t, h, http.MethodPut, target, "", body),
			http.StatusBadRequest, "validation_error")
	}
	// Bad discovery query parameters.
	for _, q := range []string{"?unknown=1", "?version=", "?zone=&version=v1"} {
		assertError(t, do(t, h, http.MethodGet, "/v1/discovery/svc"+q, "", nil),
			http.StatusBadRequest, "validation_error")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := newHandler(newRegistry())
	cases := []struct {
		method, target, allow string
	}{
		{http.MethodPost, instancePath("s", "i"), "DELETE, PUT"},
		{http.MethodGet, instancePath("s", "i"), "DELETE, PUT"},
		{http.MethodPut, heartbeatPath("s", "i"), http.MethodPost},
		{http.MethodDelete, heartbeatPath("s", "i"), http.MethodPost},
		{http.MethodPost, "/v1/discovery/s", http.MethodGet},
	}
	for _, c := range cases {
		rec := do(t, h, c.method, c.target, "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 405", c.method, c.target, rec.Code)
		}
		if rec.Header().Get("Allow") != c.allow {
			t.Errorf("%s %s Allow = %q, want %q", c.method, c.target,
				rec.Header().Get("Allow"), c.allow)
		}
		if errCode(rec) != "method_not_allowed" {
			t.Errorf("%s %s body = %s", c.method, c.target, rec.Body.String())
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s %s content-type = %q", c.method, c.target, ct)
		}
	}
}

func TestStaleTokenCannotResurrectAfterRelease(t *testing.T) {
	h := newHandler(newRegistry())
	token := decodeToken(t, registerReq(t, h, "svc", "i", validPayload()))

	if rec := do(t, h, http.MethodDelete, instancePath("svc", "i"), token, nil); rec.Code != 204 {
		t.Fatalf("delete = %d", rec.Code)
	}
	// Late requests carrying the now-dead token must report not found and must
	// never bring the lease back.
	for range 10 {
		assertError(t, do(t, h, http.MethodPost, heartbeatPath("svc", "i"), token, nil),
			http.StatusNotFound, "instance_not_found")
	}
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 0 {
		t.Fatal("released instance must stay gone")
	}
}

func TestStaleTokenAfterOverwriteCannotRenew(t *testing.T) {
	clk := newFakeClock()
	reg := newRegistry()
	reg.now = clk.now
	h := newHandler(reg)

	oldToken := decodeToken(t, registerReq(t, h, "svc", "i", validPayload()))
	newToken := decodeToken(t, registerReq(t, h, "svc", "i", validPayload()))

	clk.add(59 * time.Second)
	// Old token cannot extend the lease even though a live lease exists.
	assertError(t, do(t, h, http.MethodPost, heartbeatPath("svc", "i"), oldToken, nil),
		http.StatusConflict, "lease_conflict")
	// New token renews in the same instant; without renewal the lease would
	// expire one second later.
	if rec := do(t, h, http.MethodPost, heartbeatPath("svc", "i"), newToken, nil); rec.Code != 204 {
		t.Fatalf("new token heartbeat = %d", rec.Code)
	}
	clk.add(2 * time.Second)
	if countInstances(do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)) != 1 {
		t.Fatal("renewal by the current token should keep the instance alive")
	}
}

func TestConcurrentAccess(t *testing.T) {
	h := newHandler(newRegistry())
	const n = 40
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "i-" + strconv.Itoa(i)
			token := ""
			for j := 0; j < 40; j++ {
				if j%5 == 0 {
					rec := registerReq(t, h, "svc", name, validPayload())
					if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
						token = decodeToken(t, rec)
					}
				}
				if token != "" {
					do(t, h, http.MethodPost, heartbeatPath("svc", name), token, nil)
				}
				do(t, h, http.MethodGet, "/v1/discovery/svc?zone=z", "", nil)
				if j%11 == 0 && token != "" {
					do(t, h, http.MethodDelete, instancePath("svc", name), token, nil)
					token = ""
				}
			}
		}()
	}
	wg.Wait()
}

func TestEmptyRegistryOnFreshHandler(t *testing.T) {
	// Two independent registries never share state, mirroring a process
	// restart that begins with an empty registry.
	h1 := newHandler(newRegistry())
	registerReq(t, h1, "svc", "i", validPayload())
	h2 := newHandler(newRegistry())
	if got := countInstances(do(t, h2, http.MethodGet, "/v1/discovery/svc", "", nil)); got != 0 {
		t.Fatalf("fresh registry must be empty, got %d", got)
	}
}

func TestErrorBodyShape(t *testing.T) {
	h := newHandler(newRegistry())
	rec := do(t, h, http.MethodPost, heartbeatPath("nope", "x"), "t", nil)
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	var body map[string]map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v", err)
	}
	if body["error"]["code"] != "instance_not_found" {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

// --- helpers ---

func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, status, rec.Body.String())
	}
	if got := errCode(rec); got != code {
		t.Fatalf("error code = %q, want %q; body=%s", got, code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
}

func errCode(rec *httptest.ResponseRecorder) string {
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		return ""
	}
	return body.Error.Code
}

func decodeToken(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var r regResult
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("decode register response: %v (%s)", err, rec.Body.String())
	}
	if r.LeaseToken == "" {
		t.Fatal("empty lease token")
	}
	return r.LeaseToken
}

func countInstances(rec *httptest.ResponseRecorder) int {
	var resp struct {
		Instances []json.RawMessage `json:"instances"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return -1
	}
	return len(resp.Instances)
}

func instanceNames(rec *httptest.ResponseRecorder) []string {
	var resp struct {
		Instances []Instance `json:"instances"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return nil
	}
	names := make([]string, 0, len(resp.Instances))
	for _, in := range resp.Instances {
		names = append(names, in.Instance)
	}
	return names
}

func TestHealthzUnaffectedByRegistry(t *testing.T) {
	h := newHandler(newRegistry())
	registerReq(t, h, "svc", "i", validPayload())
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
	// Non-GET stays frozen: 405, Allow: GET, JSON method_not_allowed body.
	rec = do(t, h, http.MethodPost, "/healthz", "", nil)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("healthz POST = %d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}
	if errCode(rec) != "method_not_allowed" {
		t.Fatalf("healthz POST body = %s", rec.Body.String())
	}
}
