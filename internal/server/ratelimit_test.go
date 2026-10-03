package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func registerRateBody(endpoint, version, zone string, weight, ttl, rps, burst int) string {
	return fmt.Sprintf(`{"endpoint":%q,"version":%q,"zone":%q,"weight":%d,"ttlSeconds":%d,"rateLimit":{"requestsPerSecond":%d,"burst":%d}}`,
		endpoint, version, zone, weight, ttl, rps, burst)
}

func mustRegisterRate(t *testing.T, h http.Handler, service, name string, rps, burst int) registerResponse {
	t.Helper()
	code, resp := mustRegister(t, h, service, name,
		registerRateBody("http://"+name+":1", "v1", "z1", 10, 300, rps, burst))
	if code != http.StatusCreated {
		t.Fatalf("register with rateLimit = %d, want 201 (body %+v)", code, resp)
	}
	if resp.RateLimit == nil || resp.RateLimit.RequestsPerSecond != rps || resp.RateLimit.Burst != burst {
		t.Fatalf("register view does not echo rateLimit %d/%d: %+v", rps, burst, resp.RateLimit)
	}
	return resp
}

func acquireExpect(t *testing.T, h http.Handler, service, body string, wantCode int, wantErr string) {
	t.Helper()
	rec := doAcquire(t, h, service, body)
	if rec.Code != wantCode {
		t.Fatalf("acquire = %d, want %d (body %s)", rec.Code, wantCode, rec.Body.String())
	}
	if wantErr != "" {
		if code := errorCode(t, rec); code != wantErr {
			t.Fatalf("acquire error = %q, want %q", code, wantErr)
		}
	}
}

func TestRateLimitValidation(t *testing.T) {
	h := Handler()
	base := `"endpoint":"http://a:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":60`
	bad := []string{
		`{"rateLimit":{}}`,                                                  // both fields missing
		`{"rateLimit":{"requestsPerSecond":5}}`,                             // burst missing
		`{"rateLimit":{"burst":5}}`,                                         // requestsPerSecond missing
		`{"rateLimit":{"requestsPerSecond":0,"burst":5}}`,                   // below range
		`{"rateLimit":{"requestsPerSecond":10001,"burst":5}}`,               // above range
		`{"rateLimit":{"requestsPerSecond":5,"burst":0}}`,                   // below range
		`{"rateLimit":{"requestsPerSecond":5,"burst":10001}}`,               // above range
		`{"rateLimit":{"requestsPerSecond":1.5,"burst":5}}`,                 // non-integer
		`{"rateLimit":{"requestsPerSecond":5,"burst":2.5}}`,                 // non-integer
		`{"rateLimit":{"requestsPerSecond":"5","burst":5}}`,                 // wrong type
		`{"rateLimit":{"requestsPerSecond":5,"burst":5,"extra":1}}`,         // unknown field
		`{"rateLimit":5}`,                                                   // not an object
		`{"rateLimit":{"requestsPerSecond":-3,"burst":5}}`,                  // negative
		`{"rateLimit":{"requestsPerSecond":null,"burst":5}}`,                // explicit null
		`{"rateLimit":{"requestsPerSecond":5,"burst":5},"unknownPolicy":{}}`, // unknown sibling
	}
	for i, rl := range bad {
		body := "{" + base + "," + rl + "}"
		rec := do(t, h, http.MethodPut, fmt.Sprintf("/v1/services/svc/instances/bad-%d", i), body, nil)
		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
			t.Fatalf("case %d: status = %d (%s), want 400 validation_error", i, rec.Code, errorCode(t, rec))
		}
	}
	// Boundary values are accepted.
	for _, v := range [][2]int{{1, 1}, {10000, 10000}, {1, 10000}, {10000, 1}} {
		name := fmt.Sprintf("ok-%d-%d", v[0], v[1])
		code, resp := mustRegister(t, h, "svc", name,
			registerRateBody("http://a:1", "v1", "z1", 10, 60, v[0], v[1]))
		if code != http.StatusCreated {
			t.Fatalf("boundary %v: status = %d, want 201 (body %+v)", v, code, resp)
		}
	}
}

func TestRateLimitInvalidOverwriteKeepsOriginal(t *testing.T) {
	h := Handler()
	reg := mustRegisterRate(t, h, "svc", "i1", 5, 7)

	// An invalid overwrite attempt must not touch the stored instance.
	body := `{"endpoint":"http://i1:2","version":"v1","zone":"z1","weight":10,"ttlSeconds":60,"rateLimit":{"requestsPerSecond":5,"burst":0}}`
	rec := do(t, h, http.MethodPut, "/v1/services/svc/instances/i1", body, nil)
	if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "validation_error" {
		t.Fatalf("invalid overwrite = %d (%s), want 400 validation_error", rec.Code, errorCode(t, rec))
	}

	// The original lease token still works and the view is unchanged.
	rec = do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat with original lease = %d, want 204", rec.Code)
	}
	instances := discoveryInstances(t, h, "svc")
	if len(instances) != 1 || instances[0].RateLimit == nil ||
		instances[0].RateLimit.RequestsPerSecond != 5 || instances[0].RateLimit.Burst != 7 {
		t.Fatalf("discovery after invalid overwrite: %+v", instances)
	}
}

func TestRateLimitEchoedEverywhere(t *testing.T) {
	h := Handler()
	mustRegisterRate(t, h, "svc", "limited", 3, 4)
	mustRegister(t, h, "svc", "plain", registerBody("http://p:1", "v1", "z1", 10, 60))

	// Discovery echoes the policy for the limited instance and omits it for
	// the unconfigured one.
	rec := do(t, h, http.MethodGet, "/v1/discovery/svc", "", nil)
	var disc struct {
		Instances []map[string]json.RawMessage `json:"instances"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &disc); err != nil {
		t.Fatalf("discovery not JSON: %v", err)
	}
	seen := map[string]bool{}
	for _, inst := range disc.Instances {
		var name string
		_ = json.Unmarshal(inst["instance"], &name)
		raw, present := inst["rateLimit"]
		if name == "limited" {
			if !present {
				t.Fatal("discovery omits rateLimit for limited instance")
			}
			var rl rateLimitView
			if err := json.Unmarshal(raw, &rl); err != nil || rl.RequestsPerSecond != 3 || rl.Burst != 4 {
				t.Fatalf("discovery rateLimit = %s, want 3/4", raw)
			}
		} else if present {
			t.Fatalf("discovery leaks rateLimit for unconfigured instance %s", name)
		}
		seen[name] = true
	}
	if !seen["limited"] || !seen["plain"] {
		t.Fatalf("discovery missing instances: %v", seen)
	}

	// Resolve echoes it when the limited instance wins.
	rr := resolveOK(t, h, "/v1/resolve/svc?key=k&version=v1")
	if rr.Instance.Instance == "limited" {
		if rr.Instance.RateLimit == nil || rr.Instance.RateLimit.RequestsPerSecond != 3 {
			t.Fatalf("resolve view missing rateLimit: %+v", rr.Instance)
		}
	} else if rr.Instance.RateLimit != nil {
		t.Fatalf("resolve view leaks rateLimit: %+v", rr.Instance)
	}

	// Acquire echoes it: route to the limited instance only via zone filter
	// is not possible (same zone), so register it alone in another service.
	h2 := Handler()
	mustRegisterRate(t, h2, "svc2", "only", 8, 9)
	resp := acquireOK(t, h2, "svc2", acquireBody("k", 30, ""))
	if resp.Instance.RateLimit == nil || resp.Instance.RateLimit.RequestsPerSecond != 8 || resp.Instance.RateLimit.Burst != 9 {
		t.Fatalf("acquire view missing rateLimit: %+v", resp.Instance)
	}
}

func TestRateLimitAcquireConsumesTokens(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRate(t, h, "svc", "i1", 1, 2)

	// The bucket starts full: two acquires succeed, the third is limited.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")

	// After one second at 1 rps exactly one whole token is available again.
	now = now.Add(time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitFractionalAndCap(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRate(t, h, "svc", "i1", 2, 1)

	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	// 0.4s at 2 rps yields 0.8 tokens: not spendable.
	now = now.Add(400 * time.Millisecond)
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
	// Another 0.2s reaches 1.2, capped at burst = 1: exactly one spend.
	now = now.Add(200 * time.Millisecond)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
	// A long idle stretch cannot accumulate beyond burst (10s at 2 rps would
	// be 20 tokens uncapped, and the lease still has time to live).
	now = now.Add(10 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitFailedChecksSpendNothing(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	// Cap 1, bucket 1: the second acquire is rejected on concurrency while a
	// token is available; that rejection must not spend the token.
	mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"maxConcurrency":1,"rateLimit":{"requestsPerSecond":1,"burst":1}}`)

	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	now = now.Add(time.Second) // one token refills, slot still occupied
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "concurrency_limited")
	// Freeing the slot must reveal the untouched token.
	if rec := releasePermit(t, h, "svc", tok); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rec.Code)
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
}

func TestRateLimitNoRefundOnPermitEnd(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegisterRate(t, h, "svc", "i1", 1, 2)

	// Release returns no token.
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	if rec := releasePermit(t, h, "svc", tok); rec.Code != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rec.Code)
	}
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")

	// Completion returns no token either.
	now = now.Add(time.Second) // one token refills
	tok = acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken
	rec := do(t, h, http.MethodPost, "/v1/admission/svc/permits/"+tok+"/complete", `{"outcome":"success"}`, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")

	// Permit expiry returns no token: refill to a full bucket, spend both
	// tokens on short-lived permits, then let them expire. The 1.5s refill
	// afterwards yields 1.5 tokens: one spend succeeds, the fractional rest
	// is not spendable. Had the two expired permits refunded their tokens,
	// the bucket would have capped at 2 and allowed a second spend.
	now = now.Add(2 * time.Second)
	acquireOK(t, h, "svc", acquireBody("k", 1, ""))
	acquireOK(t, h, "svc", acquireBody("k", 1, ""))
	now = now.Add(1500 * time.Millisecond)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitErrorPrecedence(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()

	// No instance at all: 503 no_available_instance regardless of limiting.
	acquireExpect(t, h, "empty", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "no_available_instance")

	// Circuit-blocked with an empty bucket: circuit_open wins.
	mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"circuitBreaker":{"failureThreshold":1,"openSeconds":300},"rateLimit":{"requestsPerSecond":1,"burst":1}}`)
	tok := acquireOK(t, h, "svc", acquireBody("k", 60, "")).PermitToken // spends the only token
	rec := do(t, h, http.MethodPost, "/v1/admission/svc/permits/"+tok+"/complete", `{"outcome":"failure"}`, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("complete = %d, want 204", rec.Code)
	}
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "circuit_open")

	// Concurrency-exhausted with an empty bucket: concurrency_limited wins.
	mustRegister(t, h, "svc2", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"maxConcurrency":1,"rateLimit":{"requestsPerSecond":1,"burst":1}}`)
	acquireOK(t, h, "svc2", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc2", acquireBody("k", 60, ""), http.StatusTooManyRequests, "concurrency_limited")
}

func TestRateLimitOverwriteStartsFull(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	reg := mustRegisterRate(t, h, "svc", "i1", 1, 1)

	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")

	// Overwriting issues a new lease and a fresh, full bucket.
	code, resp := mustRegister(t, h, "svc", "i1", registerRateBody("http://i1:2", "v1", "z1", 10, 300, 1, 1))
	if code != http.StatusOK {
		t.Fatalf("overwrite = %d, want 200", code)
	}
	if resp.LeaseToken == reg.LeaseToken {
		t.Fatal("overwrite reused the lease token")
	}
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")

	// The old lease token is dead.
	rec := do(t, h, http.MethodPost, "/v1/services/svc/instances/i1/heartbeat", "",
		map[string]string{leaseHeader: reg.LeaseToken})
	if rec.Code != http.StatusConflict {
		t.Fatalf("heartbeat with old lease = %d, want 409", rec.Code)
	}
}

func TestRateLimitLeaseExpiryDropsBucket(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":10,"rateLimit":{"requestsPerSecond":1,"burst":1}}`)

	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	now = now.Add(11 * time.Second) // lease expires; the bucket dies with it
	// Re-registering the same name starts a fresh full bucket even though
	// less than one refill interval passed for the new record.
	mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"rateLimit":{"requestsPerSecond":1,"burst":1}}`)
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitSurvivesHealthFlapping(t *testing.T) {
	s := newServer()
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	h := s.handler()
	code, reg := mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"healthPolicy":{"failureThreshold":1,"successThreshold":1},"rateLimit":{"requestsPerSecond":1,"burst":1}}`)
	if code != http.StatusCreated {
		t.Fatalf("register = %d, want 201", code)
	}

	acquireOK(t, h, "svc", acquireBody("k", 60, "")) // bucket empty
	if code := reportSeq(t, h, "svc", "i1", reg.LeaseToken, 1, "fail"); code != http.StatusNoContent {
		t.Fatalf("fail report = %d, want 204", code)
	}
	// Unhealthy instances are not routable at all.
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusServiceUnavailable, "no_available_instance")
	if code := reportSeq(t, h, "svc", "i1", reg.LeaseToken, 2, "pass"); code != http.StatusNoContent {
		t.Fatalf("pass report = %d, want 204", code)
	}
	// Recovery keeps the drained bucket; no time has passed, so no refill.
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitDiscoveryAndResolveSpendNothing(t *testing.T) {
	h := Handler()
	mustRegisterRate(t, h, "svc", "i1", 1, 1)
	for i := 0; i < 3; i++ {
		discoveryInstances(t, h, "svc")
		resolveOK(t, h, "/v1/resolve/svc?key=k")
	}
	// The single token is still there for the first acquire only.
	acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	acquireExpect(t, h, "svc", acquireBody("k", 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitSkipsToNextRankedCandidate(t *testing.T) {
	h := Handler()
	// Two instances, same zone/version; find a key whose top-ranked
	// candidate is the drained one by probing resolve.
	mustRegisterRate(t, h, "svc", "a", 1, 1)
	mustRegisterRate(t, h, "svc", "b", 1, 1)

	key := ""
	for i := 0; ; i++ {
		cand := fmt.Sprintf("k-%d", i)
		if resolveOK(t, h, "/v1/resolve/svc?key="+cand).Instance.Instance == "a" {
			key = cand
			break
		}
	}
	// Drain a's bucket; the same key must fall through to b.
	acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	resp := acquireOK(t, h, "svc", acquireBody(key, 60, ""))
	if resp.Instance.Instance != "b" {
		t.Fatalf("second acquire routed to %s, want b", resp.Instance.Instance)
	}
	// Both buckets empty now: the key is rate_limited, with no fallback to
	// other versions or zones.
	acquireExpect(t, h, "svc", acquireBody(key, 60, ""), http.StatusTooManyRequests, "rate_limited")
}

func TestRateLimitConcurrentAcquiresNeverOverspend(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1",
		`{"endpoint":"http://i1:1","version":"v1","zone":"z1","weight":10,"ttlSeconds":300,"maxConcurrency":100,"rateLimit":{"requestsPerSecond":1,"burst":10}}`)

	const total = 50
	codes := make([]int, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := doAcquire(t, h, "svc", acquireBody("k", 60, ""))
			codes[i] = rec.Code
		}(i)
	}
	wg.Wait()
	granted, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusCreated:
			granted++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("unexpected acquire status %d", c)
		}
	}
	if granted != 10 || limited != total-10 {
		t.Fatalf("granted = %d, limited = %d; want 10 and %d", granted, limited, total-10)
	}
}

func TestUnlimitedInstanceNeverRateLimited(t *testing.T) {
	h := Handler()
	mustRegister(t, h, "svc", "i1", registerBody("http://i1:1", "v1", "z1", 10, 60))
	for i := 0; i < 20; i++ {
		acquireOK(t, h, "svc", acquireBody("k", 60, ""))
	}
}
