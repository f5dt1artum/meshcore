package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"
	"unicode/utf8"
)

// leaseHeader carries the lease token on heartbeat and deregister requests.
const leaseHeader = "X-Meshcore-Lease"

// namePattern constrains service and instance names: 1-64 ASCII letters,
// digits, dots, underscores or hyphens.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func validName(s string) bool { return namePattern.MatchString(s) }

type healthPolicyRequest struct {
	FailureThreshold int `json:"failureThreshold"`
	SuccessThreshold int `json:"successThreshold"`
}

func (p healthPolicyRequest) valid() bool {
	return p.FailureThreshold >= 1 && p.FailureThreshold <= 10 &&
		p.SuccessThreshold >= 1 && p.SuccessThreshold <= 10
}

type registerRequest struct {
	Endpoint     string               `json:"endpoint"`
	Version      string               `json:"version"`
	Zone         string               `json:"zone"`
	Weight       int                  `json:"weight"`
	TTLSeconds   int                  `json:"ttlSeconds"`
	Metadata     map[string]string    `json:"metadata"`
	HealthPolicy *healthPolicyRequest `json:"healthPolicy"`
}

func (r *registerRequest) valid() bool {
	if r.Endpoint == "" || r.Version == "" || r.Zone == "" {
		return false
	}
	if r.Weight < 1 || r.Weight > 100 || r.TTLSeconds < 1 || r.TTLSeconds > 300 {
		return false
	}
	for k, v := range r.Metadata {
		if k == "" || v == "" {
			return false
		}
	}
	return true
}

// instanceView is the public representation of a registered instance. The
// lease token is deliberately excluded; it only appears in registerResponse.
type instanceView struct {
	Service      string            `json:"service"`
	Instance     string            `json:"instance"`
	Endpoint     string            `json:"endpoint"`
	Version      string            `json:"version"`
	Zone         string            `json:"zone"`
	Weight       int               `json:"weight"`
	TTLSeconds   int               `json:"ttlSeconds"`
	Metadata     map[string]string `json:"metadata"`
	HealthStatus string            `json:"healthStatus"`
	ExpiresAt    string            `json:"expiresAt"`
}

func viewOf(inst instance) instanceView {
	return instanceView{
		Service:      inst.Service,
		Instance:     inst.Instance,
		Endpoint:     inst.Endpoint,
		Version:      inst.Version,
		Zone:         inst.Zone,
		Weight:       inst.Weight,
		TTLSeconds:   inst.TTLSeconds,
		Metadata:     inst.Metadata,
		HealthStatus: inst.healthStatus,
		ExpiresAt:    inst.expiresAt.UTC().Format(time.RFC3339),
	}
}

type registerResponse struct {
	instanceView
	LeaseToken string `json:"leaseToken"`
}

type discoveryResponse struct {
	Service   string         `json:"service"`
	Instances []instanceView `json:"instances"`
}

type resolveResponse struct {
	Service  string       `json:"service"`
	Key      string       `json:"key"`
	Instance instanceView `json:"instance"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

// handleInstance serves /v1/services/{service}/instances/{instance}.
func (s *server) handleInstance(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		s.register(w, r)
	case http.MethodDelete:
		s.deregister(w, r)
	default:
		w.Header().Set("Allow", "PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}

func (s *server) register(w http.ResponseWriter, r *http.Request) {
	service, name := r.PathValue("service"), r.PathValue("instance")
	if !validName(service) || !validName(name) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	var req registerRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || dec.Decode(&struct{}{}) != io.EOF || !req.valid() ||
		(req.HealthPolicy != nil && !req.HealthPolicy.valid()) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	metadata := make(map[string]string, len(req.Metadata))
	for k, v := range req.Metadata {
		metadata[k] = v
	}
	inst := &instance{
		Service:    service,
		Instance:   name,
		Endpoint:   req.Endpoint,
		Version:    req.Version,
		Zone:       req.Zone,
		Weight:     req.Weight,
		TTLSeconds: req.TTLSeconds,
		Metadata:   metadata,
	}
	var policy healthPolicy
	if req.HealthPolicy != nil {
		policy = healthPolicy{
			FailureThreshold: req.HealthPolicy.FailureThreshold,
			SuccessThreshold: req.HealthPolicy.SuccessThreshold,
		}
	}
	stored, overwritten := s.registry.register(inst, policy, s.now())
	status := http.StatusCreated
	if overwritten {
		status = http.StatusOK
	}
	writeJSON(w, status, registerResponse{instanceView: viewOf(stored), LeaseToken: stored.token})
}

func (s *server) deregister(w http.ResponseWriter, r *http.Request) {
	service, name := r.PathValue("service"), r.PathValue("instance")
	if !validName(service) || !validName(name) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	err := s.registry.deregister(service, name, r.Header.Get(leaseHeader), s.now())
	writeLeaseResult(w, err)
}

// handleHeartbeat serves /v1/services/{service}/instances/{instance}/heartbeat.
func (s *server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service, name := r.PathValue("service"), r.PathValue("instance")
	if !validName(service) || !validName(name) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	err := s.registry.renew(service, name, r.Header.Get(leaseHeader), s.now())
	writeLeaseResult(w, err)
}

func writeLeaseResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, errNotFound):
		writeError(w, http.StatusNotFound, "instance_not_found")
	case errors.Is(err, errLeaseConflict):
		writeError(w, http.StatusConflict, "lease_conflict")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
	}
}

// healthReportRequest is the body of a health report: a monotonically
// increasing sequence and a pass/fail status, and nothing else. Pointer fields
// let validation reject a missing field even when its zero value is legal.
type healthReportRequest struct {
	Sequence *int64  `json:"sequence"`
	Status   *string `json:"status"`
}

func (r *healthReportRequest) valid() bool {
	return r.Sequence != nil && *r.Sequence >= 0 &&
		r.Status != nil && (*r.Status == "pass" || *r.Status == "fail")
}

// handleHealth serves /v1/services/{service}/instances/{instance}/health.
func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service, name := r.PathValue("service"), r.PathValue("instance")
	if !validName(service) || !validName(name) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	// Decode permissively: resource/token/policy precedence is decided by the
	// registry, which returns errInvalidHealthReport only after those checks
	// pass. A syntactically invalid body therefore still yields 404/409/409
	// for a missing instance, bad token or disabled policy.
	var req healthReportRequest
	var seq int64
	var status string
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	reportOK := dec.Decode(&req) == nil && dec.Decode(&struct{}{}) == io.EOF && req.valid()
	if reportOK {
		seq, status = *req.Sequence, *req.Status
	}
	err := s.registry.reportHealth(service, name, r.Header.Get(leaseHeader), seq, status, reportOK, s.now())
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, errNotFound):
		writeError(w, http.StatusNotFound, "instance_not_found")
	case errors.Is(err, errLeaseConflict):
		writeError(w, http.StatusConflict, "lease_conflict")
	case errors.Is(err, errHealthDisabled):
		writeError(w, http.StatusConflict, "health_check_disabled")
	case errors.Is(err, errHealthConflict):
		writeError(w, http.StatusConflict, "health_report_conflict")
	case errors.Is(err, errStaleHealthReport):
		writeError(w, http.StatusConflict, "stale_health_report")
	case errors.Is(err, errInvalidHealthReport):
		writeError(w, http.StatusBadRequest, "validation_error")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
	}
}

// handleDiscovery serves /v1/discovery/{service}.
func (s *server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service := r.PathValue("service")
	if !validName(service) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	var version, zone string
	for key, values := range query {
		if (key != "version" && key != "zone") || len(values) != 1 || values[0] == "" {
			writeError(w, http.StatusBadRequest, "validation_error")
			return
		}
		if key == "version" {
			version = values[0]
		} else {
			zone = values[0]
		}
	}
	instances := s.registry.snapshot(service, version, zone, s.now())
	views := make([]instanceView, 0, len(instances))
	for _, inst := range instances {
		views = append(views, viewOf(inst))
	}
	writeJSON(w, http.StatusOK, discoveryResponse{Service: service, Instances: views})
}

// maxKeyBytes bounds the URL-decoded routing key accepted by handleResolve.
const maxKeyBytes = 256

// handleResolve serves /v1/resolve/{service}: it maps the caller's routing
// key onto a single live instance via consistent hashing. The selection is
// computed from the current snapshot only; it never creates or extends a
// lease.
func (s *server) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	service := r.PathValue("service")
	if !validName(service) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	var key, version, zone string
	for param, values := range query {
		if len(values) != 1 || values[0] == "" {
			writeError(w, http.StatusBadRequest, "validation_error")
			return
		}
		switch param {
		case "key":
			key = values[0]
		case "version":
			version = values[0]
		case "zone":
			zone = values[0]
		default:
			writeError(w, http.StatusBadRequest, "validation_error")
			return
		}
	}
	if key == "" || len(key) > maxKeyBytes || !utf8.ValidString(key) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	instances := s.registry.snapshot(service, version, zone, s.now())
	chosen, ok := selectInstance(key, instances)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "no_available_instance")
		return
	}
	writeJSON(w, http.StatusOK, resolveResponse{Service: service, Key: key, Instance: viewOf(chosen)})
}
