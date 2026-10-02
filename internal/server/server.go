// Package server exposes the public HTTP surface of MeshCore.
//
// In addition to the frozen process health check it provides an in-process
// service registry: instances register with a TTL-backed lease, renew or
// release it with an opaque token, and are discovered through filtered
// snapshots. All registry state is in-memory, so a process restart starts
// from an empty registry.
package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Version is the release identifier.
const Version = "0.1.0"

// LeaseHeader carries the opaque lease token on renew and release requests.
const LeaseHeader = "X-Meshcore-Lease"

const maxRegistrationBody = 64 << 10 // 64 KiB is ample for a registration.

var namePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type health struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version"`
}

type apiError struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// server bundles the registry with the HTTP handlers.
type server struct {
	reg *registry
}

// Handler returns the HTTP surface served by MeshCore.
func Handler() http.Handler {
	return newHandler(newRegistry())
}

// newHandler wires the HTTP routes to a specific registry, which lets tests
// control the clock.
func newHandler(reg *registry) http.Handler {
	s := &server{reg: reg}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v1/services/{service}/instances/{instance}", s.handleInstance)
	mux.HandleFunc("/v1/services/{service}/instances/{instance}/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/v1/discovery/{service}", s.handleDiscovery)
	mux.HandleFunc("/", s.handleNotFound)
	return mux
}

func (s *server) handleNotFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not_found")
}

// handleHealthz is the frozen baseline behavior; do not change it.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, `{"error":{"code":"method_not_allowed"}}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(health{Status: "ok", Service: "meshcore", Version: Version})
}

type registrationRequest struct {
	Endpoint   string            `json:"endpoint"`
	Version    string            `json:"version"`
	Zone       string            `json:"zone"`
	Weight     int               `json:"weight"`
	TTLSeconds int               `json:"ttlSeconds"`
	Metadata   map[string]string `json:"metadata"`
}

type registrationResponse struct {
	Instance   string            `json:"instance"`
	Endpoint   string            `json:"endpoint"`
	Version    string            `json:"version"`
	Zone       string            `json:"zone"`
	Weight     int               `json:"weight"`
	TTLSeconds int               `json:"ttlSeconds"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	LeaseToken string            `json:"leaseToken"`
	ExpiresAt  string            `json:"expiresAt"`
}

type discoveryResponse struct {
	Instances []Instance `json:"instances"`
}

// handleInstance serves PUT (register) and DELETE (release) on an instance.
func (s *server) handleInstance(w http.ResponseWriter, r *http.Request) {
	service, instance, ok := validPathNames(w, r)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.register(w, r, service, instance)
	case http.MethodDelete:
		s.release(w, r, service, instance)
	default:
		methodNotAllowed(w, http.MethodPut, http.MethodDelete)
	}
}

// handleHeartbeat serves POST (renew) on an instance.
func (s *server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	service, instance, ok := validPathNames(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	_, result := s.reg.renew(service, instance, r.Header.Get(LeaseHeader))
	switch result {
	case leaseOK:
		w.WriteHeader(http.StatusNoContent)
	case leaseNotFound:
		writeError(w, http.StatusNotFound, "instance_not_found")
	default:
		writeError(w, http.StatusConflict, "lease_conflict")
	}
}

// handleDiscovery serves GET snapshots of a service.
func (s *server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	if !validName(service) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	query := r.URL.Query()
	version, zone := query.Get("version"), query.Get("zone")
	if !validDiscoveryQuery(query) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	instances := s.reg.snapshot(service, version, zone)
	writeJSON(w, http.StatusOK, discoveryResponse{Instances: instances})
}

func (s *server) register(w http.ResponseWriter, r *http.Request, service, instance string) {
	var req registrationRequest
	if err := decodeRegistration(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	if !validRegistration(&req) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	token, inst, overwritten, err := s.reg.register(
		service, instance,
		req.Endpoint, req.Version, req.Zone,
		req.Weight, time.Duration(req.TTLSeconds)*time.Second, req.Metadata,
	)
	if err != nil {
		// crypto/rand failure is the only server-side failure possible.
		writeError(w, http.StatusInternalServerError, "internal_error")
		return
	}
	status := http.StatusCreated
	if overwritten {
		status = http.StatusOK
	}
	writeJSON(w, status, registrationResponse{
		Instance:   inst.Instance,
		Endpoint:   inst.Endpoint,
		Version:    inst.Version,
		Zone:       inst.Zone,
		Weight:     inst.Weight,
		TTLSeconds: inst.TTLSeconds,
		Metadata:   inst.Metadata,
		LeaseToken: token,
		ExpiresAt:  inst.ExpiresAt.Format(time.RFC3339),
	})
}

func (s *server) release(w http.ResponseWriter, r *http.Request, service, instance string) {
	switch s.reg.release(service, instance, r.Header.Get(LeaseHeader)) {
	case leaseOK:
		w.WriteHeader(http.StatusNoContent)
	case leaseNotFound:
		writeError(w, http.StatusNotFound, "instance_not_found")
	default:
		writeError(w, http.StatusConflict, "lease_conflict")
	}
}

// validPathNames extracts and validates the service and instance path
// segments, writing a validation error when either is malformed.
func validPathNames(w http.ResponseWriter, r *http.Request) (service, instance string, ok bool) {
	service, instance = r.PathValue("service"), r.PathValue("instance")
	if !validName(service) || !validName(instance) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return "", "", false
	}
	return service, instance, true
}

func validName(name string) bool {
	return namePattern.MatchString(name)
}

func validRegistration(req *registrationRequest) bool {
	if strings.TrimSpace(req.Endpoint) == "" ||
		strings.TrimSpace(req.Version) == "" ||
		strings.TrimSpace(req.Zone) == "" {
		return false
	}
	if req.Weight < 1 || req.Weight > 100 {
		return false
	}
	if req.TTLSeconds < 1 || req.TTLSeconds > 300 {
		return false
	}
	return true
}

// validDiscoveryQuery accepts only non-empty version and zone filters.
func validDiscoveryQuery(values url.Values) bool {
	for key, vs := range values {
		if key != "version" && key != "zone" {
			return false
		}
		for _, v := range vs {
			if v == "" {
				return false
			}
		}
	}
	return true
}

// decodeRegistration strictly decodes one JSON object from the request body.
// Any malformed, trailing, oversized or wrongly typed payload is reported as
// an error that the caller maps to a validation error.
func decodeRegistration(w http.ResponseWriter, r *http.Request, req *registrationRequest) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRegistrationBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(req); err != nil {
		return err
	}
	// Reject trailing data after the JSON object.
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errTrailingData
		}
		return err
	}
	return nil
}

var errTrailingData = errors.New("trailing data after JSON object")

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	sort.Strings(allowed)
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
}

func writeError(w http.ResponseWriter, status int, code string) {
	var payload apiError
	payload.Error.Code = code
	writeJSON(w, status, payload)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
