package server

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// Instance is the public view of a registered service instance.
type Instance struct {
	Instance   string            `json:"instance"`
	Endpoint   string            `json:"endpoint"`
	Version    string            `json:"version"`
	Zone       string            `json:"zone"`
	Weight     int               `json:"weight"`
	TTLSeconds int               `json:"ttlSeconds"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	ExpiresAt  time.Time         `json:"expiresAt"`
}

// registration is the live, mutable record held by the registry.
type registration struct {
	service    string
	instance   string
	endpoint   string
	version    string
	zone       string
	weight     int
	ttl        time.Duration
	metadata   map[string]string
	leaseToken string
	expiresAt  time.Time
}

// expiredLocked reports whether the record is past its lease deadline.
// Callers must hold mu.
func (r *registration) expiredLocked(now time.Time) bool {
	return !now.Before(r.expiresAt)
}

// snapshotLocked builds the immutable public view returned to clients.
// Callers must hold mu.
func (r *registration) snapshotLocked() Instance {
	var meta map[string]string
	if len(r.metadata) > 0 {
		meta = make(map[string]string, len(r.metadata))
		for k, v := range r.metadata {
			meta[k] = v
		}
	}
	return Instance{
		Instance:   r.instance,
		Endpoint:   r.endpoint,
		Version:    r.version,
		Zone:       r.zone,
		Weight:     r.weight,
		TTLSeconds: int(r.ttl / time.Second),
		Metadata:   meta,
		ExpiresAt:  r.expiresAt,
	}
}

// registry is the in-process, in-memory store of service instances. It is
// empty on every process start. A single mutex guards every read and write,
// and expiry plus token checks happen while the lock is held, so a stale
// token from a superseded or released lease can never extend or resurrect a
// registration.
type registry struct {
	mu      sync.Mutex
	records map[string]*registration
	now     func() time.Time
}

func newRegistry() *registry {
	return &registry{
		records: make(map[string]*registration),
		now:     time.Now,
	}
}

func regKey(service, instance string) string {
	return service + "/" + instance
}

func newLeaseToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// register inserts or supersedes a registration. A fresh lease token is
// issued on every call, so any token held for a previous registration of the
// same key stops working immediately. It returns the new token, the public
// snapshot and whether a still-live registration was overwritten.
func (reg *registry) register(service, instance, endpoint, version, zone string, weight int, ttl time.Duration, metadata map[string]string) (string, Instance, bool, error) {
	token, err := newLeaseToken()
	if err != nil {
		return "", Instance{}, false, err
	}

	meta := make(map[string]string, len(metadata))
	for k, v := range metadata {
		meta[k] = v
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	now := reg.now()
	key := regKey(service, instance)
	_, overwritten := reg.lookupLocked(key, now)
	rec := &registration{
		service:    service,
		instance:   instance,
		endpoint:   endpoint,
		version:    version,
		zone:       zone,
		weight:     weight,
		ttl:        ttl,
		metadata:   meta,
		leaseToken: token,
		expiresAt:  now.Add(ttl),
	}
	reg.records[key] = rec
	return token, rec.snapshotLocked(), overwritten, nil
}

type leaseResult int

const (
	leaseOK leaseResult = iota
	leaseNotFound
	leaseConflict
)

// lookupLocked returns the live record for the key, deleting and reporting
// absent when the registration is missing or expired. Callers must hold mu.
func (reg *registry) lookupLocked(key string, now time.Time) (*registration, bool) {
	rec, ok := reg.records[key]
	if !ok {
		return nil, false
	}
	if rec.expiredLocked(now) {
		delete(reg.records, key)
		return nil, false
	}
	return rec, true
}

func checkLease(rec *registration, token string) leaseResult {
	if token == "" || token != rec.leaseToken {
		return leaseConflict
	}
	return leaseOK
}

// renew extends the lease of a live instance whose token matches. The new
// deadline is computed from the moment the request is accepted.
func (reg *registry) renew(service, instance, token string) (Instance, leaseResult) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	now := reg.now()
	rec, ok := reg.lookupLocked(regKey(service, instance), now)
	if !ok {
		return Instance{}, leaseNotFound
	}
	if result := checkLease(rec, token); result != leaseOK {
		return Instance{}, result
	}
	rec.expiresAt = now.Add(rec.ttl)
	return rec.snapshotLocked(), leaseOK
}

// release removes a live instance whose token matches.
func (reg *registry) release(service, instance, token string) leaseResult {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	key := regKey(service, instance)
	rec, ok := reg.lookupLocked(key, reg.now())
	if !ok {
		return leaseNotFound
	}
	if result := checkLease(rec, token); result != leaseOK {
		return result
	}
	delete(reg.records, key)
	return leaseOK
}

// snapshot returns the current live instances of a service sorted by
// instance name, optionally filtered by exact version and zone. Expired
// records encountered during the scan are pruned and never returned.
func (reg *registry) snapshot(service, version, zone string) []Instance {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	now := reg.now()
	out := make([]Instance, 0)
	for key, rec := range reg.records {
		if rec.service != service {
			continue
		}
		if rec.expiredLocked(now) {
			delete(reg.records, key)
			continue
		}
		if version != "" && rec.version != version {
			continue
		}
		if zone != "" && rec.zone != zone {
			continue
		}
		out = append(out, rec.snapshotLocked())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Instance < out[j].Instance })
	return out
}
