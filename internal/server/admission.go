package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"
	"unicode/utf8"
)

// Admission outcomes surfaced to the HTTP layer.
var (
	errNoAvailableInstance = errors.New("no available instance")
	errConcurrencyLimited  = errors.New("concurrency limited")
	errPermitNotFound      = errors.New("permit not found")
)

// acquire atomically picks an instance for key and occupies one concurrency
// slot on it. Candidates are the live, non-unhealthy instances matching the
// version/zone filters, ordered by weighted rendezvous score (highest first,
// instance name breaking ties); the first candidate with a free slot wins.
// Instances without a configured limit always have a free slot. Acquiring a
// permit never extends the instance lease or changes its health state.
func (r *registry) acquire(service, version, zone, key string, ttlSeconds int, now time.Time) (instance, string, time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
		return instance{}, "", time.Time{}, errNoAvailableInstance
	}
	sort.Slice(candidates, func(a, b int) bool { return candidates[a].Instance < candidates[b].Instance })
	type scored struct {
		inst  *instance
		score float64
	}
	ranked := make([]scored, len(candidates))
	for i, cand := range candidates {
		ranked[i] = scored{cand, rendezvousScore(key, cand.Instance, cand.Weight)}
	}
	// Stable sort keeps the name order computed above on (measure-zero) ties.
	sort.SliceStable(ranked, func(a, b int) bool { return ranked[a].score > ranked[b].score })
	for _, cand := range ranked {
		inst := cand.inst
		// Expired permits release their slots lazily.
		for token, expiry := range inst.permits {
			if !now.Before(expiry) {
				delete(inst.permits, token)
			}
		}
		if inst.MaxConcurrency != nil && len(inst.permits) >= *inst.MaxConcurrency {
			continue
		}
		token := newLeaseToken()
		expiresAt := now.Add(time.Duration(ttlSeconds) * time.Second)
		inst.permits[token] = expiresAt
		return *inst, token, expiresAt, nil
	}
	return instance{}, "", time.Time{}, errConcurrencyLimited
}

// releasePermit drops one valid permit, freeing its slot. Permits that are
// unknown, expired, or belong to an instance that was overwritten,
// deregistered or whose lease expired are all reported as not found.
func (r *registry) releasePermit(service, token string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if token == "" {
		return errPermitNotFound
	}
	for _, inst := range r.instances[service] {
		if !inst.live(now) {
			continue
		}
		expiry, ok := inst.permits[token]
		if !ok {
			continue
		}
		delete(inst.permits, token)
		if !now.Before(expiry) {
			return errPermitNotFound
		}
		return nil
	}
	return errPermitNotFound
}

// acquireRequest is the body of POST /v1/admission/{service}/acquire. The
// key, version and zone fields carry the same meaning as the /v1/resolve
// query parameters; pointers distinguish an absent field from an empty one.
type acquireRequest struct {
	Key              *string `json:"key"`
	Version          *string `json:"version"`
	Zone             *string `json:"zone"`
	PermitTTLSeconds *int    `json:"permitTTLSeconds"`
}

func (r *acquireRequest) valid() bool {
	if r.Key == nil || *r.Key == "" || len(*r.Key) > maxKeyBytes || !utf8.ValidString(*r.Key) {
		return false
	}
	if r.PermitTTLSeconds == nil || *r.PermitTTLSeconds < 1 || *r.PermitTTLSeconds > 300 {
		return false
	}
	if r.Version != nil && *r.Version == "" {
		return false
	}
	if r.Zone != nil && *r.Zone == "" {
		return false
	}
	return true
}

// acquireResponse is the success body of an acquire call. The permit token
// is unique and opaque; expiresAt is the permit's own expiry, unrelated to
// the instance lease.
type acquireResponse struct {
	Service     string       `json:"service"`
	Key         string       `json:"key"`
	Instance    instanceView `json:"instance"`
	PermitToken string       `json:"permitToken"`
	ExpiresAt   string       `json:"expiresAt"`
}

// handleAcquire serves /v1/admission/{service}/acquire.
func (s *server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service := r.PathValue("service")
	if !validName(service) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	var req acquireRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || dec.Decode(&struct{}{}) != io.EOF || !req.valid() {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	var version, zone string
	if req.Version != nil {
		version = *req.Version
	}
	if req.Zone != nil {
		zone = *req.Zone
	}
	inst, token, expiresAt, err := s.registry.acquire(service, version, zone, *req.Key, *req.PermitTTLSeconds, s.now())
	switch {
	case err == nil:
	case errors.Is(err, errNoAvailableInstance):
		writeError(w, http.StatusServiceUnavailable, "no_available_instance")
		return
	case errors.Is(err, errConcurrencyLimited):
		writeError(w, http.StatusTooManyRequests, "concurrency_limited")
		return
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
		return
	}
	writeJSON(w, http.StatusCreated, acquireResponse{
		Service:     service,
		Key:         *req.Key,
		Instance:    viewOf(inst),
		PermitToken: token,
		ExpiresAt:   expiresAt.UTC().Format(time.RFC3339),
	})
}

// handlePermit serves /v1/admission/{service}/permits/{permitToken}.
func (s *server) handlePermit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service := r.PathValue("service")
	if !validName(service) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	if err := s.registry.releasePermit(service, r.PathValue("permitToken"), s.now()); err != nil {
		writeError(w, http.StatusNotFound, "permit_not_found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
