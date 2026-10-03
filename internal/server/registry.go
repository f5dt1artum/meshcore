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
	errInvalidCompletion    = errors.New("invalid completion")
)

// Health statuses exposed in the public instance representation.
const (
	healthDisabled  = "disabled"
	healthHealthy   = "healthy"
	healthUnhealthy = "unhealthy"
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

// Circuit breaker states of an instance with a circuitBreaker policy. The
// zero value is the unconfigured state: no policy, never blocked.
const (
	circuitClosed   = "closed"
	circuitOpen     = "open"
	circuitHalfOpen = "half_open"
)

// circuitState tracks the optional circuit breaker policy of an instance.
// Consecutive failed completions in the closed state open the circuit for
// openSeconds; afterwards a single probe permit decides whether the circuit
// closes again or reopens. The zero value is the unconfigured state, which
// keeps the pre-existing admission behaviour.
type circuitState struct {
	configured       bool
	failureThreshold int
	openSeconds      int
	state            string // circuitClosed, circuitOpen or circuitHalfOpen once configured

	consecutiveFail  int
	openUntil        time.Time
	probeOutstanding bool // a half-open probe permit is currently out
}

// blockedFor reports whether the instance may not receive a new permit at
// now, lazily advancing an expired open circuit to half-open. probe is set
// when a permit granted now would be the half-open probe. Callers must hold
// r.mu.
func (c *circuitState) blockedFor(now time.Time) (blocked, probe bool) {
	if !c.configured {
		return false, false
	}
	switch c.state {
	case circuitOpen:
		if now.Before(c.openUntil) {
			return true, false
		}
		c.state = circuitHalfOpen
		return false, true
	case circuitHalfOpen:
		if c.probeOutstanding {
			return true, false
		}
		return false, true
	default: // circuitClosed
		return false, false
	}
}

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
	circuit   circuitState
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
// immediately even though the permit itself has not yet expired.
type permit struct {
	token     string
	service   string
	inst      *instance
	expiresAt time.Time
	probe     bool // granted as the half-open probe of inst's circuit
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

// dropPermitLocked removes one permit and releases the quota it occupied on
// its instance. Callers must hold r.mu.
func (r *registry) dropPermitLocked(p *permit) {
	delete(r.permits, p.token)
	if n := r.inUse[p.inst] - 1; n > 0 {
		r.inUse[p.inst] = n
	} else {
		delete(r.inUse, p.inst)
	}
}

// prunePermitsLocked discards every permit that is no longer valid at now:
// permits past their own expiry and permits bound to an instance whose lease
// has expired. Overwritten and deregistered instances are removed eagerly by
// their callers. An expired probe permit re-opens its circuit. Callers must
// hold r.mu.
func (r *registry) prunePermitsLocked(now time.Time) {
	for token, p := range r.permits {
		if !now.Before(p.expiresAt) || !p.inst.live(now) {
			delete(r.permits, token)
			if n := r.inUse[p.inst] - 1; n > 0 {
				r.inUse[p.inst] = n
			} else {
				delete(r.inUse, p.inst)
			}
			r.reopenIfProbeLocked(p, now)
		}
	}
}

// reopenIfProbeLocked re-opens the circuit when a half-open probe permit
// ends without a completion outcome (released or expired). Callers must hold
// r.mu.
func (r *registry) reopenIfProbeLocked(p *permit, now time.Time) {
	c := &p.inst.circuit
	if c.configured && p.probe && c.state == circuitHalfOpen {
		c.state = circuitOpen
		c.probeOutstanding = false
		c.openUntil = now.Add(time.Duration(c.openSeconds) * time.Second)
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
		if n := r.inUse[inst] - 1; n > 0 {
			r.inUse[inst] = n
		} else {
			delete(r.inUse, inst)
		}
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
// slot wins. Unbounded instances (MaxConcurrency == 0) always admit.
// Instances whose circuit is open, or half-open with a probe already out,
// are skipped in rank order; a half-open circuit grants its single probe
// permit to the first acquire that reaches it. The lookup and the occupancy
// happen under one lock, so the number of valid permits can never exceed an
// instance's quota. It returns errNoRoutableInstance when the filter matches
// nothing, errCircuitOpen when every match is blocked only by its circuit,
// and errConcurrencyLimited when every circuit-passing match is at capacity.
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

	// Drop candidates blocked by their circuit, keeping rank order.
	type eligible struct {
		inst  *instance
		probe bool
	}
	var passing []eligible
	for _, cand := range candidates {
		blocked, probe := cand.circuit.blockedFor(now)
		if !blocked {
			passing = append(passing, eligible{inst: cand, probe: probe})
		}
	}
	if len(passing) == 0 {
		return acquiredPermit{}, errCircuitOpen
	}

	var chosen *eligible
	for i := range passing {
		cand := passing[i].inst
		if cand.MaxConcurrency == 0 || r.inUse[cand] < cand.MaxConcurrency {
			chosen = &passing[i]
			break
		}
	}
	if chosen == nil {
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
		inst:      chosen.inst,
		expiresAt: expiresAt,
		probe:     chosen.probe,
	}
	r.inUse[chosen.inst]++
	if chosen.probe {
		chosen.inst.circuit.probeOutstanding = true
	}
	return acquiredPermit{inst: *chosen.inst, token: token, expiresAt: expiresAt}, nil
}

// release releases a valid permit granted for service. Unknown tokens,
// expired permits, tokens bound to another service and permits invalidated
// by overwrite, deregistration or lease expiry all return errPermitNotFound.
// Releasing a half-open probe permit re-opens its circuit.
func (r *registry) release(service, token string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePermitsLocked(now)
	p, ok := r.permits[token]
	if !ok || p.service != service {
		return errPermitNotFound
	}
	r.dropPermitLocked(p)
	r.reopenIfProbeLocked(p, now)
	return nil
}

// complete settles a valid permit granted for service with a success or
// failure outcome, freeing its slot and invalidating the token. Existence
// and service checks run before the body validation so errPermitNotFound
// takes precedence, matching the health-report error ordering. Completions
// drive the circuit of the permit's instance: in the closed state a failure
// extends the consecutive-failure streak (opening the circuit at the
// threshold) and a success resets it; a probe completion closes the circuit
// on success and re-opens it on failure; completions while the circuit is
// open only free the slot.
func (r *registry) complete(service, token string, bodyOK, success bool, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePermitsLocked(now)
	p, ok := r.permits[token]
	if !ok || p.service != service {
		return errPermitNotFound
	}
	if !bodyOK {
		return errInvalidCompletion
	}
	r.dropPermitLocked(p)
	c := &p.inst.circuit
	if !c.configured {
		return nil
	}
	switch {
	case p.probe:
		// The half-open probe decides the circuit on its own.
		c.probeOutstanding = false
		if success {
			c.state = circuitClosed
			c.consecutiveFail = 0
		} else {
			c.state = circuitOpen
			c.openUntil = now.Add(time.Duration(c.openSeconds) * time.Second)
		}
	case c.state == circuitClosed:
		if success {
			c.consecutiveFail = 0
		} else {
			c.consecutiveFail++
			if c.consecutiveFail >= c.failureThreshold {
				c.state = circuitOpen
				c.consecutiveFail = 0
				c.openUntil = now.Add(time.Duration(c.openSeconds) * time.Second)
			}
		}
	}
	// Open and (non-probe) half-open completions only free the slot.
	return nil
}
