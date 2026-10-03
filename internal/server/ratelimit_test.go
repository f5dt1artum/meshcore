package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func registerRateLimitBody(endpoint, version, zone string, weight, ttl, rps, burst int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"rateLimit":{"requestsPerSecond":%d,"burst":%d}}`,
		endpoint, version, zone, weight, ttl, rps, burst)
}

func mustRegisterRateLimit(t *testing.T, h http.Handler, service, name string, rps, burst int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerRateLimitBody("http://"+name+":1", "v1", "z1", 10, 300, rps, burst))
	if code != http.StatusCreated {
		t.Fatalf("register with rateLimit = %d, want 201 (body %+v)", code, resp)
	}
	return resp
}

func TestRegisterRateLimitValidation(t *testing.T) {
	h := Handler()
	base := `{"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":60,"rateLimit":%s}`
	bad := []struct {
		name string
		rl   string
	}{
		{"missing burst", `{"requestsPerSecond":10}`},
		{"missing requestsPerSecond", `{"burst":10}`},
		{"empty object", `{}`},
		{"rps zero", `{"requestsPerSecond":0,"burst":10}`},
		{"rps too large", `{"requestsPerSecond":10001,"burst":10}`},
		{"burst zero", `{"requestsPerSecond":10,"burst":0}`},
		{"burst too large", `{"requestsPerSecond":10,"burst":10001}`},
		{"rps fractional", `{"requestsPerSecond":1.5,"burst":10}`},
		{"burst fractional", `{"requestsPerSecond":10,"burst":2.5}`},
		{"rps string", `{"requestsPerSecond":"10","burst":10}`},
		{"unknown field", `{"requestsPerSecond":10,"burst":10,"windowSeconds":5}`},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/bad",
				fmt.Sprintf(base, tc.rl), nil)
			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
				t.Fatalf("status = %d (%s), want 400 validation_error", rec.Code, errorCode(t, rec))
			}
		})
	}

	// A rejected overwrite leaves the original instance and its lease intact.
	reg := mustRegisterRateLimit(t, h, "svc", "victim", 5, 5)
	rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/victim",
		fmt.Sprintf(base, `{"requestsPerSecond":0,"burst":10}`), nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad overwrite = %d, want 400", rec.Code)
	}
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/victim/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat after rejected overwrite = %d, want 204", rec.Code)
	}
}

func TestRateLimitViewEcho(t *testing.T) {
	h := Handler()
	mustRegisterRateLimit(t, h, "svc", "limited", 7, 3)
	mustRegister(t, h, "svc", "plain", registerBody("http://plain:1", "v1", "z1", 10, 300))

	// Register response echoes the policy.
	code, resp := mustRegister(t, h, "svc", "limited2",
		registerRateLimitBody("http://limited2:1", "v1", "z1", 10, 300, 4, 2))
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}
	if resp.RateLimit == nil || resp.RateLimit.RequestsPerSecond != 4 || resp.RateLimit.Burst != 2 {
		t.Fatalf("register view does not echo rateLimit: %+v", resp.RateLimit)
	}

	// Discovery echoes it per instance and omits it when unconfigured.
	for _, inst := range discoveryInstances(t, h, "svc") {
		switch inst.Instance {
		case "limited":
			if inst.RateLimit == nil || inst.RateLimit.RequestsPerSecond != 7 || inst.RateLimit.Burst != 3 {
				t.Fatalf("discovery view does not echo rateLimit: %+v", inst.RateLimit)
			}
		case "plain":
			if inst.RateLimit != nil {
				t.Fatalf("unconfigured instance leaks rateLimit: %+v", inst.RateLimit)
			}
		}
	}

	// Resolve echoes it on the chosen instance.
	rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("resolve = %d, want 200", rec.Code)
	}
	var resolved resolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resolved); err != nil {
		t.Fatalf("resolve response is not JSON: %v", err)
	}
	if resolved.Instance.Instance == "limited" && resolved.Instance.RateLimit == nil {
		t.Fatal("resolve view of limited instance omits rateLimit")
	}

	// Acquire echoes it in the instance view.
	got := acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	if got.Instance.Instance == "limited" && got.Instance.RateLimit == nil {
		t.Fatal("acquire view of limited instance omits rateLimit")
	}

	// The field is absent from the raw JSON of unconfigured instances.
	rec = do(t, h, http.MethodPut, "/v1/services/svc/instances/plain2",
		registerBody("http://plain2:1", "v1", "z1", 10, 300), nil)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("register response is not JSON: %v", err)
	}
	if _, present := raw["rateLimit"]; present {
		t.Fatalf("register view leaks rateLimit: %s", rec.Body.String())
	}
}

func TestAcquireConsumesTokens(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 1, 2)

	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("third acquire = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
}

func TestTokenRefillContinuousAndCapped(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 2, 2)

	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))

	// Half a token is not spendable, but the fractional balance is kept.
	now = now.Add(250 * time.Millisecond)
	rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire with half a token = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
	now = now.Add(250 * time.Millisecond)
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))

	// A long idle stretch refills only up to burst.
	now = now.Add(10 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec = doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire beyond burst = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
}

func TestPermitSettlementDoesNotReturnTokens(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 1, 1)

	// Completion does not return the token.
	tok := acquireOK(t, h, "svc", acquireBody("k", 30, "")).PermitToken
	rec := do(t, h, http.MethodPost, "/v1/admission/svc/permits/"+tok+"/complete",
		`{"outcome":"success"}`, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	rec = doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire after complete = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}

	// Neither does release.
	now = now.Add(time.Second) // one token refills
	tok = acquireOK(t, h, "svc", acquireBody("k", 30, "")).PermitToken
	if rec := releasePermit(t, h, "svc", tok); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rec.Code)
	}
	rec = doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire after release = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}

	// Nor does permit expiry.
	now = now.Add(time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 5, ""))
	now = now.Add(6 * time.Second) // permit expires; one token refills
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec = doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire after permit expiry = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
}

func TestOverwriteResetsBucket(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 1, 1)

	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second acquire = %d, want 429", rec.Code)
	}

	// Overwriting starts a fresh, full bucket.
	if code, _ := mustRegister(t, h, "svc", "i1",
		registerRateLimitBody("http://i1:2", "v1", "z1", 10, 300, 1, 1)); code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
}

func TestRateLimitSkipsToNextCandidate(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "a", 1, 1)
	mustRegisterRateLimit(t, h, "svc", "b", 1, 1)

	// Two keys for which a outranks b.
	var keys []string
	for i := 0; len(keys) < 2; i++ {
		key := fmt.Sprintf("key-%d", i)
		if rendezvousScore(key, "a", 10) > rendezvousScore(key, "b", 10) {
			keys = append(keys, key)
		}
	}

	// Drain a's only token, then verify the next acquire falls through to b
	// without consuming anything from a.
	if got := acquireOK(t, h, "svc", acquireBody(keys[0], 30, "")); got.Instance.Instance != "a" {
		t.Fatalf("first acquire chose %s, want a", got.Instance.Instance)
	}
	if got := acquireOK(t, h, "svc", acquireBody(keys[1], 30, "")); got.Instance.Instance != "b" {
		t.Fatalf("second acquire chose %s, want b", got.Instance.Instance)
	}
	// Both buckets are now empty.
	rec := doAcquire(t, h, "svc", acquireBody(keys[0], 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("third acquire = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
	// After one second each bucket holds exactly one token again.
	now = now.Add(time.Second)
	if got := acquireOK(t, h, "svc", acquireBody(keys[0], 30, "")); got.Instance.Instance != "a" {
		t.Fatalf("after refill acquire chose %s, want a", got.Instance.Instance)
	}
}

func TestConcurrencyLimitedTakesPrecedenceOverRateLimit(t *testing.T) {
	h := Handler()
	body := `{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"maxConcurrency":1,"rateLimit":{"requestsPerSecond":10,"burst":10}}`
	if code, _ := mustRegister(t, h, "svc", "i1", body); code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "concurrency_limited" {
		t.Fatalf("second acquire = %d (%s), want 429 concurrency_limited", rec.Code, errorCode(t, rec))
	}
}

func TestUnhealthyRecoveryKeepsBucket(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	body := `{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"healthPolicy":{"failureThreshold":1,"successThreshold":1},"rateLimit":{"requestsPerSecond":1,"burst":1}}`
	code, reg := mustRegister(t, h, "svc", "i1", body)
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}

	// Drain the only token; a heartbeat must not reset the bucket.
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat = %d, want 204", rec.Code)
	}

	// Cycle the instance through unhealthy and back to healthy.
	if code := reportSeq(t, h, "svc", "i1", reg.LeaseToken, 1, "fail"); code != http.StatusNoContent {
		t.Fatalf("fail report = %d, want 204", code)
	}
	if code := reportSeq(t, h, "svc", "i1", reg.LeaseToken, 2, "pass"); code != http.StatusNoContent {
		t.Fatalf("pass report = %d, want 204", code)
	}

	// The bucket is still empty: health transitions kept its state.
	rec = doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("acquire after recovery = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
}

func TestReadOnlyEndpointsDoNotConsumeTokens(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 1, 1)

	for i := 0; i < 3; i++ {
		if rec := do(t, h, http.MethodGet, "/v1/resolve/svc?key=k", "", nil); rec.Code != http.StatusOK {
			t.Fatalf("resolve = %d, want 200", rec.Code)
		}
		if rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil); rec.Code != http.StatusOK {
			t.Fatalf("discovery = %d, want 200", rec.Code)
		}
	}
	// The single token is still there, and only one acquire succeeds.
	acquireOK(t, h, "svc", acquireBody("k", 30, ""))
	rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("second acquire = %d (%s), want 429 rate_limited", rec.Code, errorCode(t, rec))
	}
}

func TestConcurrentAcquiresNeverOverspend(t *testing.T) {
	h := Handler()
	mustRegisterRateLimit(t, h, "svc", "i1", 1, 10)

	var wg sync.WaitGroup
	var mu sync.Mutex
	created, limited := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := doAcquire(t, h, "svc", acquireBody("k", 30, ""))
			mu.Lock()
			defer mu.Unlock()
			switch rec.Code {
			case http.StatusCreated:
				created++
			case http.StatusTooManyRequests:
				limited++
			default:
				t.Errorf("unexpected acquire status %d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if created != 10 || limited != 40 {
		t.Fatalf("created = %d, limited = %d; want 10 and 40", created, limited)
	}
}
