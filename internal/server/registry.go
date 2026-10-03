package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

// Lease and admission outcomes surfaced to the HTTP layer.
var (
	errNotFound             = errors.New("instance not found")
	errLeaseConflict        = errors.New("lease conflict")
	errHealthDisabled       = errors.New("health check disabled")
	errInvalidHealthReport  = errors.New("invalid health report")
	errHealthReportConflict = errors.New("health report conflict")
	errStaleHealthReport    = errors.New("stale health report")
	errNoRoutableInstance   = errors.New("no routable instance")
	errConcurrencyLimited   = errors.New("concurrency limited")
	errCircuitOpen          = errors.New("circuit open")
	errPermitNotFound       = errors.New("permit not found")
)

// Health statuses exposed in the public instance representation.
const (
	healthDisabled  = "disabled"
	healthHealthy   = "healthy"
	healthUnhealthy = "unhealthy"
)

// Per-instance circuit-breaker states. The breaker is independent of the
// health policy: it tracks permit outcomes rather than health reports, is
// invisible to discovery and resolve, and never changes healthStatus.
const (
	breakerClosed   = "closed"
	breakerOpen     = "open"
	breakerHalfOpen = "half_open"
)

// healthState tracks the optional health policy of an instance and the
// consecutive-report counters that drive transitions. The zero value is the
// disabled state: no policy, no reports accepted, always routable.
type healthState struct {
	configured       bool
	failureThreshold int
	successThreshold int
	status           string // healthHealthy or healthUnhealthy once configured

	consecutivePass int
	consecutiveFail int
	lastSequence    int64
	lastStatusPass  bool
	hasSequence     bool
}

// breakerState is the optional per-instance circuit breaker. The zero value
// is the disabled state: no breaker configured, admission is unaffected. A
// configured breaker starts closed; consecutive permit failures open it. The
// breaker is evaluated lazily against the caller's clock, so an open breaker
// flips to half_open exactly when openSeconds elapse without any timer.
type breakerState struct {
	configured       bool
	failureThreshold int
	openSeconds      int

	status          string // breakerClosed, breakerOpen or breakerHalfOpen
	consecutiveFail int
	openedAt        time.Time // when the current open period started
}

// instance is one registered service instance plus its lease state. The
// exported fields are set once at registration; token and expiresAt are
// mutated under the registry lock.
type instance struct {
	Service        string
	Instance       string
	Endpoint       string
	Version        string
	Zone           string
	Weight         int
	TTLSeconds     int
	Metadata       map[string]string
	MaxConcurrency int // zero means unbounded

	token     string
	expiresAt time.Time
	health    healthState
	breaker   breakerState
}

func (i *instance) live(now time.Time) bool { return now.Before(i.expiresAt) }

// healthStatus is the public health representation: "disabled" when no
// health policy was registered, otherwise the current healthy/unhealthy
// state driven by reported check results.
func (i *instance) healthStatus() string {
	if !i.health.configured {
		return healthDisabled
	}
	return i.health.status
}

// permit is one outstanding short-lived admission permit. It references the
// exact instance record it was granted against, so overwriting,
// deregistering or losing the lease of that record invalidates the permit
// immediately even though the permit itself has not yet expired. probe marks
// the single trial permit a half_open breaker hands out while its outcome is
// pending; no further permit may be granted to that instance until the trial
// ends.
type permit struct {
	token     string
	service   string
	inst      *instance
	expiresAt time.Time
	probe     bool
}

// registry holds all live and expired-but-unpurged instances. Expiration is
// evaluated lazily against the caller's clock, so an expired instance can
// never reappear in snapshots or be revived by a stale lease token.
type registry struct {
	mu        sync.Mutex
	instances map[string]map[string]*instance // service -> instance name -> record
	permits   map[string]*permit              // outstanding permits by opaque token
	inUse     map[*instance]int               // unexpired permits counted per record
}

func newRegistry() *registry {
	return &registry{
		instances: make(map[string]map[string]*instance),
		permits:   make(map[string]*permit),
		inUse:     make(map[*instance]int),
	}
}

// newLeaseToken mints a random opaque token. Lease and permit tokens share
// the shape but live in independent namespaces.
func newLeaseToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// newPermitToken mints the unique opaque token identifying one permit.
func newPermitToken() string { return newLeaseToken() }

// register inserts inst, replacing any previous record with the same name and
// minting a fresh lease token (the old token stops matching immediately). It
// returns a copy of the stored record and whether a live instance was
// overwritten.
func (r *registry) register(inst *instance, now time.Time) (instance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst.token = newLeaseToken()
	inst.expiresAt = now.Add(time.Duration(inst.TTLSeconds) * time.Second)
	byName, ok := r.instances[inst.Service]
	if !ok {
		byName = make(map[string]*instance)
		r.instances[inst.Service] = byName
	}
	overwritten := false
	if prev, ok := byName[inst.Instance]; ok {
		if prev.live(now) {
			overwritten = true
		}
		// The replacement is a new instance: permits granted against the
		// previous record stop being releasable and free its quota at once.
		r.invalidatePermitsLocked(prev)
	}
	byName[inst.Instance] = inst
	return *inst, overwritten
}

// renew extends the lease of a live instance whose current token matches.
func (r *registry) renew(service, name, token string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.lookupLocked(service, name, now)
	if inst == nil {
		return errNotFound
	}
	if token == "" || token != inst.token {
		return errLeaseConflict
	}
	inst.expiresAt = now.Add(time.Duration(inst.TTLSeconds) * time.Second)
	return nil
}

// reportHealth applies one health report to a live instance. bodyOK reports
// whether the request body decoded and validated; it is checked only after
// existence, token and policy so those errors take precedence. Reports never
// extend the lease. Sequence handling: a strictly larger sequence advances
// state; the same sequence with the same status is an idempotent no-op, with
// a different status a conflict; a smaller sequence is stale.
func (r *registry) reportHealth(service, name, token string, bodyOK bool, seq int64, pass bool, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.lookupLocked(service, name, now)
	if inst == nil {
		return errNotFound
	}
	if token == "" || token != inst.token {
		return errLeaseConflict
	}
	if !inst.health.configured {
		return errHealthDisabled
	}
	if !bodyOK {
		return errInvalidHealthReport
	}
	h := &inst.health
	if h.hasSequence {
		if seq < h.lastSequence {
			return errStaleHealthReport
		}
		if seq == h.lastSequence {
			if pass == h.lastStatusPass {
				return nil
			}
			return errHealthReportConflict
		}
	}
	h.hasSequence = true
	h.lastSequence = seq
	h.lastStatusPass = pass
	if pass {
		h.consecutivePass++
		h.consecutiveFail = 0
		if h.consecutivePass >= h.successThreshold {
			h.status = healthHealthy
		}
	} else {
		h.consecutiveFail++
		h.consecutivePass = 0
		if h.consecutiveFail >= h.failureThreshold {
			h.status = healthUnhealthy
		}
	}
	return nil
}

// deregister removes a live instance whose current token matches.
func (r *registry) deregister(service, name, token string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.lookupLocked(service, name, now)
	if inst == nil {
		return errNotFound
	}
	if token == "" || token != inst.token {
		return errLeaseConflict
	}
	r.invalidatePermitsLocked(inst)
	delete(r.instances[service], name)
	if len(r.instances[service]) == 0 {
		delete(r.instances, service)
	}
	return nil
}

// lookupLocked returns the live record or nil; expired records are treated as
// absent. Callers must hold r.mu.
func (r *registry) lookupLocked(service, name string, now time.Time) *instance {
	inst := r.instances[service][name]
	if inst == nil || !inst.live(now) {
		return nil
	}
	return inst
}

// snapshot returns copies of the live instances of service matching the
// version/zone filters (empty string disables a filter), sorted by instance
// name. Unhealthy instances are excluded from routing and discovery; only
// healthy or health-disabled instances appear. Copies are taken under the
// lock so callers can read them race-free.
func (r *registry) snapshot(service, version, zone string, now time.Time) []instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []instance
	for _, inst := range r.instances[service] {
		if !inst.live(now) {
			continue
		}
		if inst.health.configured && inst.health.status == healthUnhealthy {
			continue
		}
		if version != "" && inst.Version != version {
			continue
		}
		if zone != "" && inst.Zone != zone {
			continue
		}
		out = append(out, *inst)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Instance < out[b].Instance })
	return out
}

// openBreakerLocked starts a fresh open period on a configured breaker and
// clears the closed-state failure counter. Callers must hold r.mu.
func openBreakerLocked(b *breakerState, now time.Time) {
	b.status = breakerOpen
	b.openedAt = now
	b.consecutiveFail = 0
}

// dropPermitLocked removes one permit and releases the quota it occupied on
// its instance. Callers must hold r.mu.
func (r *registry) dropPermitLocked(p *permit) {
	delete(r.permits, p.token)
	r.decInUseLocked(p.inst)
}

// decInUseLocked gives back one occupied concurrency slot on inst. Callers
// must hold r.mu.
func (r *registry) decInUseLocked(inst *instance) {
	if n := r.inUse[inst] - 1; n > 0 {
		r.inUse[inst] = n
	} else {
		delete(r.inUse, inst)
	}
}

// prunePermitsLocked discards every permit that is no longer valid at now:
// permits past their own expiry and permits bound to an instance whose lease
// has expired. Overwritten and deregistered instances are removed eagerly by
// their callers. A trial permit whose time ran out restarts the open period
// of its breaker; losing the instance lease discards the breaker with the
// record, so no transition is needed. Callers must hold r.mu.
func (r *registry) prunePermitsLocked(now time.Time) {
	for token, p := range r.permits {
		if now.Before(p.expiresAt) && p.inst.live(now) {
			continue
		}
		delete(r.permits, token)
		r.decInUseLocked(p.inst)
		if p.probe && p.inst.breaker.configured && p.inst.live(now) {
			openBreakerLocked(&p.inst.breaker, now)
		}
	}
}

// invalidatePermitsLocked releases every outstanding permit bound to inst.
// Overwriting or deregistering an instance invalidates its old permits
// immediately and gives the quota back. Callers must hold r.mu.
func (r *registry) invalidatePermitsLocked(inst *instance) {
	for token, p := range r.permits {
		if p.inst != inst {
			continue
		}
		delete(r.permits, token)
		r.decInUseLocked(inst)
	}
}

// acquiredPermit is the result of a successful admission: a copy of the
// chosen instance together with the opaque token and its expiry.
type acquiredPermit struct {
	inst      instance
	token     string
	expiresAt time.Time
}

// acquire atomically selects an instance for key and occupies one permit
// slot on it. Candidates are the live, non-unhealthy instances of service
// matching the version/zone filters, ranked by the same weighted rendezvous
// score used by resolve, best first; the first-ranked candidate with a free
// slot wins. Unbounded instances (MaxConcurrency == 0) always admit. The
// lookup and the occupancy happen under one lock, so the number of valid
// permits can never exceed an instance's quota.
//
// Circuit breakers only winnow admission; they never affect discovery or
// resolve. Candidates are still walked in ranked order. An open breaker
// skips its instance; a half_open breaker with an outstanding trial permit
// skips it too; a half_open breaker without one is breaker-eligible and, if
// the candidate has a free concurrency slot, receives the single trial
// permit. The open-to-half_open flip happens lazily on the first acquire at
// or after openSeconds. acquire returns errNoRoutableInstance when the
// filter matches no live, healthy instance, errCircuitOpen when every match
// is stopped solely by its breaker, and errConcurrencyLimited when at least
// one match passes its breaker check but all such matches are at capacity.
func (r *registry) acquire(service, key, version, zone string, ttl time.Duration, now time.Time) (acquiredPermit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePermitsLocked(now)

	var candidates []*instance
	for _, inst := range r.instances[service] {
		if !inst.live(now) {
			continue
		}
		if inst.health.configured && inst.health.status == healthUnhealthy {
			continue
		}
		if version != "" && inst.Version != version {
			continue
		}
		if zone != "" && inst.Zone != zone {
			continue
		}
		candidates = append(candidates, inst)
	}
	if len(candidates) == 0 {
		return acquiredPermit{}, errNoRoutableInstance
	}
	// Rank by descending rendezvous score; the instance name breaks the
	// measure-zero tie exactly as selectInstance does.
	sort.Slice(candidates, func(a, b int) bool {
		sa := rendezvousScore(key, candidates[a].Instance, candidates[a].Weight)
		sb := rendezvousScore(key, candidates[b].Instance, candidates[b].Weight)
		if sa != sb {
			return sa > sb
		}
		return candidates[a].Instance < candidates[b].Instance
	})

	var chosen *instance
	probe := false
	allBreakerBlocked := true
	for _, cand := range candidates {
		willProbe := false
		if cand.breaker.configured {
			b := &cand.breaker
			if b.status == breakerOpen {
				// The open period ends lazily: the first admission attempt at
				// or after openSeconds turns the breaker into half_open.
				if !now.Before(b.openedAt.Add(time.Duration(b.openSeconds) * time.Second)) {
					b.status = breakerHalfOpen
				} else {
					continue // blocked only by the breaker
				}
			}
			if b.status == breakerHalfOpen {
				// At most one trial permit may be outstanding; a pending
				// trial blocks the instance on breaker grounds alone.
				if r.hasProbeLocked(cand) {
					continue
				}
				willProbe = true
			}
		}
		// This candidate passed the breaker check; ranking and the normal
		// concurrency semantics still decide whether it admits now.
		allBreakerBlocked = false
		if cand.MaxConcurrency == 0 || r.inUse[cand] < cand.MaxConcurrency {
			chosen = cand
			probe = willProbe
			break
		}
	}
	if chosen == nil {
		if allBreakerBlocked {
			return acquiredPermit{}, errCircuitOpen
		}
		return acquiredPermit{}, errConcurrencyLimited
	}

	token := newPermitToken()
	for {
		if _, taken := r.permits[token]; !taken {
			break
		}
		token = newPermitToken()
	}
	expiresAt := now.Add(ttl)
	r.permits[token] = &permit{
		token:     token,
		service:   service,
		inst:      chosen,
		expiresAt: expiresAt,
		probe:     probe,
	}
	r.inUse[chosen]++
	return acquiredPermit{inst: *chosen, token: token, expiresAt: expiresAt}, nil
}

// hasProbeLocked reports whether inst currently has an outstanding trial
// permit. Callers must hold r.mu.
func (r *registry) hasProbeLocked(inst *instance) bool {
	for _, p := range r.permits {
		if p.inst == inst && p.probe {
			return true
		}
	}
	return false
}

// release releases a valid permit granted for service. Unknown tokens,
// expired permits, tokens bound to another service and permits invalidated
// by overwrite, deregistration or lease expiry all return errPermitNotFound.
// Releasing a half_open trial permit without an outcome restarts the open
// period; releasing a normal permit never touches the breaker.
func (r *registry) release(service, token string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePermitsLocked(now)
	p, ok := r.permits[token]
	if !ok || p.service != service {
		return errPermitNotFound
	}
	if p.probe && p.inst.breaker.configured {
		openBreakerLocked(&p.inst.breaker, now)
	}
	r.dropPermitLocked(p)
	return nil
}

// complete reports the outcome of a valid permit granted for service and
// releases it. The lookup rules mirror release: unknown, expired,
// wrong-service or invalidated permits all return errPermitNotFound, so a
// repeat completion and a later DELETE both 404.
//
// Outcomes drive only a configured breaker. In closed state completions are
// applied in acceptance order: success clears the consecutive-failure
// counter, failure increments it and opens the breaker at the threshold.
// Completions of permits that were outstanding while the breaker is open
// only free quota. A half_open trial permit's success closes and resets the
// breaker; its failure starts a fresh open period.
func (r *registry) complete(service, token string, success bool, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePermitsLocked(now)
	p, ok := r.permits[token]
	if !ok || p.service != service {
		return errPermitNotFound
	}
	b := &p.inst.breaker
	if b.configured {
		switch {
		case p.probe:
			if success {
				b.status = breakerClosed
				b.consecutiveFail = 0
			} else {
				openBreakerLocked(b, now)
			}
		case b.status == breakerClosed:
			if success {
				b.consecutiveFail = 0
			} else {
				b.consecutiveFail++
				if b.consecutiveFail >= b.failureThreshold {
					openBreakerLocked(b, now)
				}
			}
		}
	}
	r.dropPermitLocked(p)
	return nil
}
