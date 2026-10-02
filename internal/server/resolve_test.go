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

func getResolve(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodGet, target, "", nil)
}

func resolveInstance(t *testing.T, rec *httptest.ResponseRecorder) resolveResponse {
	t.Helper()
	var resp resolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resolve response is not JSON: %v (body %s)", err, rec.Body.String())
	}
	return resp
}

func TestResolveSelectsOneOfManyAndEchoes(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "payments", "pay-2", registerBody("http://2:9000", "v1", "z1", 10, 60))
	mustRegister(t, h, "payments", "pay-1", registerBody("http://1:9000", "v1", "z1", 10, 60))
	mustRegister(t, h, "payments", "pay-3", registerBody("http://3:9000", "v1", "z1", 10, 60))

	rec := getResolve(t, h, "/v1/resolve/payments?key=user-42")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	requireJSON(t, rec)
	resp := resolveInstance(t, rec)
	if resp.Service != "payments" {
		t.Fatalf("service = %q, want payments", resp.Service)
	}
	if resp.Key != "user-42" {
		t.Fatalf("key = %q, want user-42", resp.Key)
	}
	switch resp.Instance.Instance {
	case "pay-1", "pay-2", "pay-3":
	default:
		t.Fatalf("unexpected instance: %+v", resp.Instance)
	}
	if resp.Instance.Service != "payments" || !strings.HasPrefix(resp.Instance.Endpoint, "http://") {
		t.Fatalf("instance view looks wrong: %+v", resp.Instance)
	}
	if strings.Contains(rec.Body.String(), "leaseToken") {
		t.Fatalf("resolve leaks lease token: %s", rec.Body.String())
	}
}

func TestResolveStableAcrossRegistrationOrder(t *testing.T) {
	key := "order-777"
	names := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	build := func(order []string) string {
		s := newServer()
		h := s.handler()
		for _, n := range order {
			mustRegister(t, h, "svc", n, registerBody("http://"+n, "v1", "z1", 10, 60))
		}
		resp := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+key))
		return resp.Instance.Instance
	}

	first := build(names)
	for _, order := range [][]string{
		{"echo", "delta", "charlie", "bravo", "alpha"},
		{"charlie", "alpha", "echo", "delta", "bravo"},
		{"bravo", "echo", "alpha", "charlie", "delta"},
	} {
		if got := build(order); got != first {
			t.Fatalf("selection depends on registration order: %q vs %q", got, first)
		}
	}
	if first == "" {
		t.Fatal("no instance selected")
	}
}

func TestResolveKeyOwnershipAfterAddAndRemove(t *testing.T) {
	s := newServer()
	h := s.handler()
	base := []string{"a", "b", "c", "d", "e"}
	for _, n := range base {
		mustRegister(t, h, "svc", n, registerBody("http://"+n, "v1", "z1", 10, 60))
	}
	keys := make([]string, 200)
	owners := make(map[string]string, len(keys))
	for i := range keys {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		owners[k] = resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
	}

	// Removing one instance: keys owned by survivors keep their owner.
	mustDeregister(t, h, "svc", "c")
	for _, k := range keys {
		got := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
		if owners[k] != "c" && got != owners[k] {
			t.Fatalf("after removing c, key %q moved %q -> %q", k, owners[k], got)
		}
		if got == "c" {
			t.Fatalf("removed instance c still selected for key %q", k)
		}
	}

	// Re-adding c with the same name/weight restores exactly the old owners.
	mustRegister(t, h, "svc", "c", registerBody("http://c-new", "v1", "z1", 10, 60))
	for _, k := range keys {
		got := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
		if got != owners[k] {
			t.Fatalf("after re-adding c, key %q owner %q != original %q", k, got, owners[k])
		}
	}

	// Adding a fresh instance: each key either stays or moves to the newcomer.
	mustRegister(t, h, "svc", "f", registerBody("http://f", "v1", "z1", 10, 60))
	for _, k := range keys {
		got := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
		if got != owners[k] && got != "f" {
			t.Fatalf("after adding f, key %q moved to neither its owner %q nor f: %q", k, owners[k], got)
		}
	}
}

func TestResolveSameNameSameWeightOverwriteKeepsOwnershipButRefreshesView(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	for _, n := range []string{"a", "b", "c", "d"} {
		mustRegister(t, h, "svc", n, registerBody("http://"+n+":1", "v1", "z1", 25, 60))
	}
	keys := make([]string, 100)
	owners := make(map[string]string, len(keys))
	for i := range keys {
		k := fmt.Sprintf("k%d", i)
		keys[i] = k
		owners[k] = resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
	}

	// Overwrite every instance with the same name and weight but new endpoint,
	// metadata and a later expiry.
	now = now.Add(10 * time.Second)
	for _, n := range []string{"a", "b", "c", "d"} {
		body := fmt.Sprintf(`{"endpoint":"http://%s:2","version":"v1","zone":"z1","weight":25,"ttlSeconds":60,"metadata":{"rev":"2"}}`, n)
		if code, _ := mustRegister(t, h, "svc", n, body); code != http.StatusOK {
			t.Fatalf("overwrite %s = %d, want 200", n, code)
		}
	}
	for _, k := range keys {
		rec := getResolve(t, h, "/v1/resolve/svc?key="+k)
		resp := resolveInstance(t, rec)
		if resp.Instance.Instance != owners[k] {
			t.Fatalf("same-name/same-weight overwrite moved key %q: %q -> %q", k, owners[k], resp.Instance.Instance)
		}
		if resp.Instance.Endpoint != "http://"+owners[k]+":2" || resp.Instance.Metadata["rev"] != "2" {
			t.Fatalf("overwritten fields not reflected for key %q: %+v", k, resp.Instance)
		}
		expiresAt, err := time.Parse(time.RFC3339, resp.Instance.ExpiresAt)
		if err != nil || expiresAt.Before(now) {
			t.Fatalf("expiresAt %q stale or invalid (now %s)", resp.Instance.ExpiresAt, now)
		}
	}

	// Changing a weight is a topology change and is allowed to move keys.
	mustRegister(t, h, "svc", "a", registerBody("http://a:2", "v1", "z1", 99, 60))
	moved := 0
	for _, k := range keys {
		got := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
		if got != owners[k] {
			moved++
			if got != "a" {
				t.Fatalf("weight change moved key %q to %q, expected only a to gain", k, got)
			}
		}
	}
	if moved == 0 {
		t.Fatal("raising a's weight to 99 never attracted a key")
	}
}

func TestResolveWeightsGiveHigherStableShare(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "heavy", registerBody("http://heavy", "v1", "z1", 90, 60))
	mustRegister(t, h, "svc", "light", registerBody("http://light", "v1", "z1", 10, 60))
	counts := map[string]int{}
	for i := 0; i < 2000; i++ {
		name := resolveInstance(t, getResolve(t, h, fmt.Sprintf("/v1/resolve/svc?key=route-%d", i))).Instance.Instance
		counts[name]++
	}
	if counts["heavy"] <= counts["light"] {
		t.Fatalf("weight share wrong: heavy=%d light=%d", counts["heavy"], counts["light"])
	}
	// ~9x expected; require a comfortably wide margin.
	if float64(counts["heavy"]) < 5*float64(counts["light"]) {
		t.Fatalf("heavy share not proportional to weight: heavy=%d light=%d", counts["heavy"], counts["light"])
	}
}

func TestResolveFiltersVersionAndZoneExactly(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "a", registerBody("http://a", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "b", registerBody("http://b", "v2", "z1", 10, 60))
	mustRegister(t, h, "svc", "c", registerBody("http://c", "v1", "z2", 10, 60))
	mustRegister(t, h, "other", "a", registerBody("http://x", "v1", "z1", 10, 60))

	for i := 0; i < 50; i++ {
		target := fmt.Sprintf("/v1/resolve/svc?version=v1&zone=z1&key=k%d", i)
		resp := resolveInstance(t, getResolve(t, h, target))
		if resp.Instance.Instance != "a" {
			t.Fatalf("version+zone filter selected %q, want a", resp.Instance.Instance)
		}
		if resp.Instance.Version != "v1" || resp.Instance.Zone != "z1" {
			t.Fatalf("filter returned instance outside filter: %+v", resp.Instance)
		}
	}

	// No fallback to another version or zone: 503 no_available_instance.
	for _, target := range []string{
		"/v1/resolve/svc?key=k1&version=v9",
		"/v1/resolve/svc?key=k1&zone=z9",
		"/v1/resolve/unknown?key=k1",
	} {
		rec := getResolve(t, h, target)
		if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
			t.Fatalf("GET %s = %d (%s), want 503 no_available_instance", target, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}

	// An expired instance can never be returned once its lease lapses.
	now = now.Add(61 * time.Second)
	rec := getResolve(t, h, "/v1/resolve/svc?key=k1")
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
		t.Fatalf("after expiry = %d (%s), want 503", rec.Code, errorCode(t, rec))
	}
}

func TestResolveDeregisteredInstanceNeverReturned(t *testing.T) {
	h := Handler()
	for _, n := range []string{"a", "b", "c"} {
		mustRegister(t, h, "svc", n, registerBody("http://"+n, "v1", "z1", 10, 60))
	}
	owners := map[string]string{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i)
		owners[k] = resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
	}
	doomed := owners["k0"]
	mustDeregister(t, h, "svc", doomed) // same name/weight re-register then delete
	got := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key=k0")).Instance.Instance
	if got == doomed {
		t.Fatalf("deregistered instance %q still returned", doomed)
	}
	// Other keys keep their owner when that owner survived.
	for i := 1; i < 100; i++ {
		k := fmt.Sprintf("k%d", i)
		if owners[k] == doomed {
			continue
		}
		if now := resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance; now != owners[k] {
			t.Fatalf("key %q moved %q -> %q after removing %q", k, owners[k], now, doomed)
		}
	}
}

func TestResolveValidationErrors(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "a", registerBody("http://a", "v1", "z1", 10, 60))
	cases := []struct {
		name   string
		target string
	}{
		{"missing key", "/v1/resolve/svc"},
		{"empty key", "/v1/resolve/svc?key="},
		{"unknown param", "/v1/resolve/svc?key=k&foo=1"},
		{"duplicate key", "/v1/resolve/svc?key=a&key=b"},
		{"duplicate version", "/v1/resolve/svc?key=k&version=v1&version=v2"},
		{"empty version", "/v1/resolve/svc?key=k&version="},
		{"empty zone", "/v1/resolve/svc?key=k&zone="},
		{"bad service name", "/v1/resolve/bad%20name?key=k"},
		{"malformed escape", "/v1/resolve/svc?key=%zz"},
		{"key too long", "/v1/resolve/svc?key=" + strings.Repeat("a", 257)},
		{"invalid utf8", "/v1/resolve/svc?key=%ff%fe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := getResolve(t, h, tc.target)
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
				t.Fatalf("GET %s = %d (%s), want 400 validation_error", tc.target, rec.Code, errorCode(t, rec))
			}
			requireJSON(t, rec)
		})
	}

	// A multibyte key of exactly 256 UTF-8 bytes is accepted; 257 is not.
	long := strings.Repeat("世", 85) // 3 bytes each = 255
	rec := getResolve(t, h, "/v1/resolve/svc?key="+long+"a")
	if rec.Code != http.StatusOK {
		t.Fatalf("256-byte key = %d, want 200", rec.Code)
	}
	rec = getResolve(t, h, "/v1/resolve/svc?key="+long+"世")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("258-byte key = %d, want 400", rec.Code)
	}
}

func TestResolveDecodesKeyAndEchoesDecodedValue(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "a", registerBody("http://a", "v1", "z1", 10, 60))
	// "héllo/world 世界" percent-encoded.
	rec := getResolve(t, h, "/v1/resolve/svc?key=h%C3%A9llo%2Fworld%20%E4%B8%96%E7%95%8C")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if resp := resolveInstance(t, rec); resp.Key != "héllo/world 世界" {
		t.Fatalf("echoed key = %q", resp.Key)
	}
}

func TestResolveMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := do(t, h, method, "/v1/resolve/svc?key=k", "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s = %d, want 405", method, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodGet {
			t.Fatalf("Allow = %q, want GET", allow)
		}
		if code := errorCode(t, rec); code != "method_not_allowed" {
			t.Fatalf("code = %q, want method_not_allowed", code)
		}
	}
}

func TestResolveDoesNotExtendLease(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "a", registerBody("http://a", "v1", "z1", 10, 10))

	now = now.Add(9 * time.Second)
	for i := 0; i < 20; i++ {
		if rec := getResolve(t, h, "/v1/resolve/svc?key=k"); rec.Code != http.StatusOK {
			t.Fatalf("resolve = %d, want 200", rec.Code)
		}
	}
	now = now.Add(2 * time.Second) // t=11: expiry anchored at registration, not resolve
	rec := getResolve(t, h, "/v1/resolve/svc?key=k")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("lease was extended by resolve: %d, want 503", rec.Code)
	}
}

func TestResolveDeterministicUnderConcurrency(t *testing.T) {
	h := Handler()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		mustRegister(t, h, "svc", n, registerBody("http://"+n, "v1", "z1", 10, 300))
	}
	const n = 50
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("concurrent-key-%d", i)
	}
	want := map[string]string{}
	for _, k := range keys {
		want[k] = resolveInstance(t, getResolve(t, h, "/v1/resolve/svc?key="+k)).Instance.Instance
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 50; round++ {
				for _, k := range keys {
					rec := getResolve(t, h, "/v1/resolve/svc?key="+k)
					if rec.Code != http.StatusOK {
						t.Errorf("resolve = %d", rec.Code)
						return
					}
					resp := resolveInstance(t, rec)
					if resp.Instance.Instance != want[k] {
						t.Errorf("key %q selected %q, want %q", k, resp.Instance.Instance, want[k])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

func mustDeregister(t *testing.T, h http.Handler, service, name string) {
	t.Helper()
	// Capture a fresh token by re-registering (same name/weight: ownership
	// semantics are unaffected), then deregister with it.
	rec := do(t, h, http.MethodPut,
		"/v1/services/"+service+"/instances/"+name,
		registerBody("http://"+name, "v1", "z1", 10, 60), nil)
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		t.Fatalf("reregister %s = %d", name, rec.Code)
	}
	var reg registerResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &reg); err != nil {
		t.Fatalf("reregister not JSON: %v", err)
	}
	rec = do(t, h, http.MethodDelete, "/v1/services/"+service+"/instances/"+name, "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deregister %s = %d", name, rec.Code)
	}
}
