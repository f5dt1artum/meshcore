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
	errNotFound             = errors.New("instance not found")
	errLeaseConflict        = errors.New("lease conflict")
	errHealthDisabled       = errors.New("health check disabled")
	errInvalidHealthReport  = errors.New("invalid health report")
	errHealthReportConflict = errors.New("health report conflict")
	errStaleHealthReport    = errors.New("stale health report")
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

// instance is one registered service instance plus its lease state. The
// exported fields are set once at registration; token and expiresAt are
// mutated under the registry lock.
type instance struct {
	Service    string
	Instance   string
	Endpoint   string
	Version    string
	Zone       string
	Weight     int
	TTLSeconds int
	Metadata   map[string]string
	// MaxConcurrency caps the number of simultaneously valid admission
	// permits; nil means unlimited.
	MaxConcurrency *int

	token     string
	expiresAt time.Time
	health    healthState
	// permits maps permit token to permit expiry. It is replaced wholesale
	// on overwrite, so permits of a superseded record die with it.
	permits map[string]time.Time
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
// overwritten.
func (r *registry) register(inst *instance, now time.Time) (instance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst.token = newLeaseToken()
	inst.expiresAt = now.Add(time.Duration(inst.TTLSeconds) * time.Second)
	inst.permits = make(map[string]time.Time)
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
