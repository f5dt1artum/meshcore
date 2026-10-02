package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

// Lease operation outcomes surfaced to the HTTP layer.
var (
	errNotFound            = errors.New("instance not found")
	errLeaseConflict       = errors.New("lease conflict")
	errHealthDisabled      = errors.New("health reporting disabled")
	errHealthConflict      = errors.New("health report conflicts with an accepted sequence")
	errStaleHealthReport   = errors.New("health report has a stale sequence")
	errInvalidHealthReport = errors.New("invalid health report body")
)

// Health policy/status values. When no policy is configured the status is
// disabled and the instance always participates in routing.
const (
	healthStatusDisabled  = "disabled"
	healthStatusHealthy   = "healthy"
	healthStatusUnhealthy = "unhealthy"
)

// healthPolicy configures consecutive-report thresholds. Zero values mean the
// instance has no policy (status disabled).
type healthPolicy struct {
	FailureThreshold int
	SuccessThreshold int
}

func (p healthPolicy) enabled() bool { return p != healthPolicy{} }

// instance is one registered service instance plus its lease and health
// state. The exported fields are set once at registration; token, expiresAt
// and the health fields are mutated under the registry lock.
type instance struct {
	Service    string
	Instance   string
	Endpoint   string
	Version    string
	Zone       string
	Weight     int
	TTLSeconds int
	Metadata   map[string]string

	token     string
	expiresAt time.Time

	policy        healthPolicy
	healthStatus  string
	successStreak int
	failureStreak int
	lastSeq       *int64
	lastStatus    string
}

func (i *instance) live(now time.Time) bool { return now.Before(i.expiresAt) }

// registry holds all live and expired-but-unpurged instances. Expiration is
// evaluated lazily against the caller's clock, so an expired instance can
// never reappear in snapshots or be revived by a stale lease token.
type registry struct {
	mu        sync.Mutex
	instances map[string]map[string]*instance // service -> instance name -> record
}

func newRegistry() *registry {
	return &registry{instances: make(map[string]map[string]*instance)}
}

func newLeaseToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// register inserts inst, replacing any previous record with the same name and
// minting a fresh lease token (the old token stops matching immediately). It
// returns a copy of the stored record and whether a live instance was
// overwritten. A fresh registration always starts healthy (or disabled when
// policy is the zero value) with cleared counters and sequence.
func (r *registry) register(inst *instance, policy healthPolicy, now time.Time) (instance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst.token = newLeaseToken()
	inst.expiresAt = now.Add(time.Duration(inst.TTLSeconds) * time.Second)
	inst.policy = policy
	if policy.enabled() {
		inst.healthStatus = healthStatusHealthy
	} else {
		inst.healthStatus = healthStatusDisabled
	}
	inst.successStreak, inst.failureStreak = 0, 0
	inst.lastSeq, inst.lastStatus = nil, ""
	byName, ok := r.instances[inst.Service]
	if !ok {
		byName = make(map[string]*instance)
		r.instances[inst.Service] = byName
	}
	overwritten := false
	if prev, ok := byName[inst.Instance]; ok && prev.live(now) {
		overwritten = true
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
	delete(r.instances[service], name)
	if len(r.instances[service]) == 0 {
		delete(r.instances, service)
	}
	return nil
}

// reportHealth applies a health report from a live instance whose current
// token matches. Checks run in precedence order: instance existence, token,
// policy presence and only then the report body (reportOK). After that only a
// strictly greater sequence advances state: the same sequence with the same
// status is an idempotent no-op, the same sequence with a different status
// conflicts, and a smaller sequence is stale. A successful report never
// extends the lease.
func (r *registry) reportHealth(service, name, token string, seq int64, status string, reportOK bool, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.lookupLocked(service, name, now)
	if inst == nil {
		return errNotFound
	}
	if token == "" || token != inst.token {
		return errLeaseConflict
	}
	if !inst.policy.enabled() {
		return errHealthDisabled
	}
	if !reportOK {
		return errInvalidHealthReport
	}
	if inst.lastSeq != nil {
		switch {
		case seq < *inst.lastSeq:
			return errStaleHealthReport
		case seq == *inst.lastSeq:
			if status != inst.lastStatus {
				return errHealthConflict
			}
			return nil // idempotent re-delivery of the accepted report
		}
	}
	seqCopy := seq
	inst.lastSeq = &seqCopy
	inst.lastStatus = status
	switch status {
	case "pass":
		inst.successStreak++
		inst.failureStreak = 0
		if inst.healthStatus == healthStatusUnhealthy && inst.successStreak >= inst.policy.SuccessThreshold {
			inst.healthStatus = healthStatusHealthy
		}
	case "fail":
		inst.failureStreak++
		inst.successStreak = 0
		if inst.healthStatus == healthStatusHealthy && inst.failureStreak >= inst.policy.FailureThreshold {
			inst.healthStatus = healthStatusUnhealthy
		}
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

// snapshot returns copies of the routable live instances of service matching
// the version/zone filters (empty string disables a filter), sorted by
// instance name. Unhealthy instances are excluded; disabled (no policy) and
// healthy ones are included. Copies are taken under the lock so callers can
// read them race-free.
func (r *registry) snapshot(service, version, zone string, now time.Time) []instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []instance
	for _, inst := range r.instances[service] {
		if !inst.live(now) {
			continue
		}
		if inst.healthStatus == healthStatusUnhealthy {
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
