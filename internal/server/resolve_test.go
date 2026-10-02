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

func resolve(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodGet, target, "", nil)
}

func resolveOK(t *testing.T, h http.Handler, target string) resolveResponse {
	t.Helper()
	rec := resolve(t, h, target)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200 (body %s)", target, rec.Code, rec.Body.String())
	}
	requireJSON(t, rec)
	var resp resolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resolve response is not JSON: %v", err)
	}
	if strings.Contains(rec.Body.String(), "leaseToken") {
		t.Fatalf("resolve leaks lease token: %s", rec.Body.String())
	}
	return resp
}

func TestResolveReturnsSingleInstance(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "i2", registerBody("http://b:1", "v1", "z1", 10, 60))

	resp := resolveOK(t, h, "/v1/resolve/svc?key=user-42")
	if resp.Service != "svc" || resp.Key != "user-42" {
		t.Fatalf("service/key not echoed: %+v", resp)
	}
	if resp.Instance.Instance != "i1" && resp.Instance.Instance != "i2" {
		t.Fatalf("unexpected instance %q", resp.Instance.Instance)
	}
	if resp.Instance.Endpoint == "" || resp.Instance.ExpiresAt == "" {
		t.Fatalf("instance view incomplete: %+v", resp.Instance)
	}
	// The choice is stable across repeated calls.
	for i := 0; i < 10; i++ {
		if again := resolveOK(t, h, "/v1/resolve/svc?key=user-42"); again.Instance.Instance != resp.Instance.Instance {
			t.Fatalf("unstable choice: %q then %q", resp.Instance.Instance, again.Instance.Instance)
		}
	}
}

func TestResolveEchoesDecodedKey(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	resp := resolveOK(t, h, "/v1/resolve/svc?key=a%20b%2Fc")
	if resp.Key != "a b/c" {
		t.Fatalf("decoded key = %q, want %q", resp.Key, "a b/c")
	}
	resp = resolveOK(t, h, "/v1/resolve/svc?key=%E4%B8%AD%E6%96%87")
	if resp.Key != "中文" {
		t.Fatalf("decoded key = %q, want %q", resp.Key, "中文")
	}
}

func TestResolveIndependentOfRegistrationOrder(t *testing.T) {
	build := func(order []string) http.Handler {
		h := Handler()
		for _, name := range order {
			mustRegister(t, h, "svc", name, registerBody("http://"+name+":1", "v1", "z1", 10, 60))
		}
		return h
	}
	forward := build([]string{"a", "b", "c", "d"})
	reverse := build([]string{"d", "c", "b", "a"})
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		got := resolveOK(t, forward, "/v1/resolve/svc?key="+key).Instance.Instance
		want := resolveOK(t, reverse, "/v1/resolve/svc?key="+key).Instance.Instance
		if got != want {
			t.Fatalf("key %q: order-dependent choice %q vs %q", key, got, want)
		}
	}
}

func TestResolveWeightShares(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "light", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "heavy", registerBody("http://b:1", "v1", "z1", 90, 60))
	counts := map[string]int{}
	const total = 4000
	for i := 0; i < total; i++ {
		name := resolveOK(t, h, fmt.Sprintf("/v1/resolve/svc?key=k-%d", i)).Instance.Instance
		counts[name]++
	}
	// Expected 400/3600; allow generous slack for hash variance.
	if counts["light"] < 250 || counts["light"] > 600 {
		t.Fatalf("light share %d/%d, want roughly 10%%", counts["light"], total)
	}
	if counts["heavy"] < total-600 || counts["heavy"] > total-250 {
		t.Fatalf("heavy share %d/%d, want roughly 90%%", counts["heavy"], total)
	}
}

func TestResolveMinimalMigration(t *testing.T) {
	h := Handler()
	names := []string{"n1", "n2", "n3", "n4"}
	for _, n := range names {
		mustRegister(t, h, "svc", n, registerBody("http://"+n+":1", "v1", "z1", 10, 300))
	}
	const keys = 500
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("route-%d", i)
		before[key] = resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
	}

	// Removing one instance only reassigns the keys that pointed at it.
	_, victim := mustRegister(t, h, "svc", "n4", registerBody("http://n4:1", "v1", "z1", 10, 300))
	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/n4", "",
		map[string]string{leaseHeader: victim.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete n4 = %d, want 204", rec.Code)
	}
	moved := 0
	for key, owner := range before {
		now := resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
		if owner != "n4" && now != owner {
			t.Fatalf("key %q migrated from %q to %q after unrelated removal", key, owner, now)
		}
		if now != owner {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("no keys migrated after removing an instance")
	}

	// Adding one instance only steals keys; nothing moves between old owners.
	mustRegister(t, h, "svc", "n5", registerBody("http://n5:1", "v1", "z1", 10, 300))
	afterRemoval := make(map[string]string, keys)
	for key := range before {
		afterRemoval[key] = resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
	}
	for key, owner := range afterRemoval {
		now := resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
		if now != owner && now != "n5" {
			t.Fatalf("key %q migrated from %q to %q on addition; only the new instance may steal", key, owner, now)
		}
	}
}

func TestResolveOverwriteSameWeightKeepsOwnership(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 300))
	mustRegister(t, h, "svc", "i2", registerBody("http://b:1", "v1", "z1", 20, 300))
	const keys = 200
	before := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("k-%d", i)
		before[key] = resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance
	}
	// Re-register i1 with the same weight but a new endpoint and metadata.
	body := `{"endpoint":"http://a-new:9","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"metadata":{"gen":"2"}}`
	if code, _ := mustRegister(t, h, "svc", "i1", body); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	for key, owner := range before {
		resp := resolveOK(t, h, "/v1/resolve/svc?key="+key)
		if resp.Instance.Instance != owner {
			t.Fatalf("key %q moved %q -> %q after same-weight overwrite", key, owner, resp.Instance.Instance)
		}
		if owner == "i1" && (resp.Instance.Endpoint != "http://a-new:9" || resp.Instance.Metadata["gen"] != "2") {
			t.Fatalf("overwrite not reflected: %+v", resp.Instance)
		}
	}
}

func TestResolveFiltersAndNoFallback(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "v1z1", registerBody("http://a:1", "v1", "z1", 10, 60))
	mustRegister(t, h, "svc", "v2z1", registerBody("http://b:1", "v2", "z1", 10, 60))
	mustRegister(t, h, "svc", "v1z2", registerBody("http://c:1", "v1", "z2", 10, 60))

	if got := resolveOK(t, h, "/v1/resolve/svc?key=k&version=v2").Instance.Instance; got != "v2z1" {
		t.Fatalf("version filter chose %q, want v2z1", got)
	}
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k&zone=z2").Instance.Instance; got != "v1z2" {
		t.Fatalf("zone filter chose %q, want v1z2", got)
	}
	if got := resolveOK(t, h, "/v1/resolve/svc?key=k&version=v1&zone=z1").Instance.Instance; got != "v1z1" {
		t.Fatalf("combined filter chose %q, want v1z1", got)
	}
	// No live instance matches: 503, never a fallback to another version/zone.
	for _, target := range []string{
		"/v1/resolve/svc?key=k&version=v9",
		"/v1/resolve/svc?key=k&zone=z9",
		"/v1/resolve/ghost?key=k",
	} {
		rec := resolve(t, h, target)
		if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "no_available_instance" {
			t.Fatalf("GET %s = %d (%s), want 503 no_available_instance", target, rec.Code, errorCode(t, rec))
		}
		requireJSON(t, rec)
	}
}

func TestResolveSkipsExpiredAndDeregistered(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "short", registerBody("http://a:1", "v1", "z1", 10, 10))
	mustRegister(t, h, "svc", "long", registerBody("http://b:1", "v1", "z1", 10, 300))

	// Find a key owned by the short-lived instance.
	owned := ""
	for i := 0; ; i++ {
		key := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/svc?key="+key).Instance.Instance == "short" {
			owned = key
			break
		}
	}
	now = now.Add(11 * time.Second) // "short" expires
	if got := resolveOK(t, h, "/v1/resolve/svc?key="+owned).Instance.Instance; got != "long" {
		t.Fatalf("after expiry key resolves to %q, want long", got)
	}
	// Deregistering the survivor leaves nothing to resolve.
	_, reg := mustRegister(t, h, "svc", "long", registerBody("http://b:1", "v1", "z1", 10, 300))
	rec := do(t, h, http.MethodDelete, "/v1/services/svc/instances/long", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rec.Code)
	}
	if rec := resolve(t, h, "/v1/resolve/svc?key="+owned); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("resolve with no live instances = %d, want 503", rec.Code)
	}
}

func TestResolveValidationErrors(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://a:1", "v1", "z1", 10, 60))
	cases := []struct {
		name   string
		target string
	}{
		{"missing key", "/v1/resolve/svc"},
		{"empty key", "/v1/resolve/svc?key="},
		{"key too long", "/v1/resolve/svc?key=" + strings.Repeat("a", 257)},
		{"key invalid utf8", "/v1/resolve/svc?key=%ff%fe"},
		{"key bad escape", "/v1/resolve/svc?key=%zz"},
		{"unknown param", "/v1/resolve/svc?key=k&foo=bar"},
		{"duplicate key", "/v1/resolve/svc?key=a&key=b"},
		{"duplicate version", "/v1/resolve/svc?key=k&version=v1&version=v2"},
		{"empty version", "/v1/resolve/svc?key=k&version="},
		{"empty zone", "/v1/resolve/svc?key=k&zone="},
		{"bad service name", "/v1/resolve/bad%20name?key=k"},
		{"service too long", "/v1/resolve/" + strings.Repeat("a", 65) + "?key=k"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := resolve(t, h, tc.target)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
			}
			requireJSON(t, rec)
			if code := errorCode(t, rec); code != "validation_error" {
				t.Fatalf("error code = %q, want validation_error", code)
			}
		})
	}
	// Boundary: a 256-byte key is accepted.
	rec := resolve(t, h, "/v1/resolve/svc?key="+strings.Repeat("a", 256))
	if rec.Code != http.StatusOK {
		t.Fatalf("256-byte key = %d, want 200", rec.Code)
	}
}

func TestResolveMethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := do(t, h, method, "/v1/resolve/svc?key=k", "", nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s = %d, want 405", method, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodGet {
			t.Fatalf("%s Allow = %q, want GET", method, allow)
		}
		requireJSON(t, rec)
		if code := errorCode(t, rec); code != "method_not_allowed" {
			t.Fatalf("%s error code = %q, want method_not_allowed", method, code)
		}
	}
}

func TestResolveConcurrentStable(t *testing.T) {
	h := Handler()
	for _, n := range []string{"i1", "i2", "i3"} {
		mustRegister(t, h, "svc", n, registerBody("http://"+n+":1", "v1", "z1", 10, 60))
	}
	want := resolveOK(t, h, "/v1/resolve/svc?key=hot").Instance.Instance
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if got := resolveOK(t, h, "/v1/resolve/svc?key=hot").Instance.Instance; got != want {
					t.Errorf("concurrent resolve = %q, want %q", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}
