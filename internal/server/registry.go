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
	errNotFound      = errors.New("instance not found")
	errLeaseConflict = errors.New("lease conflict")
)

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

	token     string
	expiresAt time.Time
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
// name. Copies are taken under the lock so callers can read them race-free.
func (r *registry) snapshot(service, version, zone string, now time.Time) []instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []instance
	for _, inst := range r.instances[service] {
		if !inst.live(now) {
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
