package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
	"unicode/utf8"
)

// acquireRequest is the body of POST /v1/admission/{service}/acquire. The
// pointers distinguish an absent optional filter from an empty string; key
// and permitTTLSeconds are required.
type acquireRequest struct {
	Key              string  `json:"key"`
	Version          *string `json:"version"`
	Zone             *string `json:"zone"`
	PermitTTLSeconds *int    `json:"permitTTLSeconds"`
}

func (r *acquireRequest) valid() bool {
	if r.Key == "" || len(r.Key) > maxKeyBytes || !utf8.ValidString(r.Key) {
		return false
	}
	if r.Version != nil && *r.Version == "" {
		return false
	}
	if r.Zone != nil && *r.Zone == "" {
		return false
	}
	if r.PermitTTLSeconds == nil || *r.PermitTTLSeconds < 1 || *r.PermitTTLSeconds > 300 {
		return false
	}
	return true
}

type acquireResponse struct {
	Service     string       `json:"service"`
	Key         string       `json:"key"`
	Instance    instanceView `json:"instance"`
	PermitToken string       `json:"permitToken"`
	ExpiresAt   string       `json:"expiresAt"`
}

// handleAcquire serves /v1/admission/{service}/acquire. It selects an
// instance with the same weighted rendezvous ranking as resolve and
// atomically occupies one concurrency slot on the first-ranked instance
// that has room. Acquiring a permit neither extends the instance lease nor
// changes its health state.
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
	version, zone := "", ""
	if req.Version != nil {
		version = *req.Version
	}
	if req.Zone != nil {
		zone = *req.Zone
	}
	ttl := time.Duration(*req.PermitTTLSeconds) * time.Second
	got, err := s.registry.acquire(service, req.Key, version, zone, ttl, s.now())
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, acquireResponse{
			Service:     service,
			Key:         req.Key,
			Instance:    viewOf(got.inst),
			PermitToken: got.token,
			ExpiresAt:   got.expiresAt.UTC().Format(time.RFC3339),
		})
	case errors.Is(err, errNoRoutableInstance):
		writeError(w, http.StatusServiceUnavailable, "no_available_instance")
	case errors.Is(err, errConcurrencyLimited):
		writeError(w, http.StatusTooManyRequests, "concurrency_limited")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
	}
}

// handlePermit serves /v1/admission/{service}/permits/{permitToken}. A
// valid permit is released and its slot freed; anything else is an unknown
// permit.
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
	token := r.PathValue("permitToken")
	err := s.registry.release(service, token, s.now())
	if err != nil {
		writeError(w, http.StatusNotFound, "permit_not_found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
