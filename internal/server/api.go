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
	FailureThreshold *int `json:"failureThreshold"`
	SuccessThreshold *int `json:"successThreshold"`
}

type circuitBreakerRequest struct {
	FailureThreshold *int `json:"failureThreshold"`
	OpenSeconds      *int `json:"openSeconds"`
}

type rateLimitRequest struct {
	RequestsPerSecond *int `json:"requestsPerSecond"`
	Burst             *int `json:"burst"`
}

type registerRequest struct {
	Endpoint       string                 `json:"endpoint"`
	Version        string                 `json:"version"`
	Zone           string                 `json:"zone"`
	Weight         int                    `json:"weight"`
	TTLSeconds     int                    `json:"ttlSeconds"`
	Metadata       map[string]string      `json:"metadata"`
	HealthPolicy   *healthPolicyRequest   `json:"healthPolicy"`
	MaxConcurrency *int                   `json:"maxConcurrency"`
	CircuitBreaker *circuitBreakerRequest `json:"circuitBreaker"`
	RateLimit      *rateLimitRequest      `json:"rateLimit"`
}

func (r *registerRequest) valid() bool {
	if r.Endpoint == "" || r.Version == "" || r.Zone == "" {
		return false
	}
	if r.Weight < 1 || r.Weight > 100 || r.TTLSeconds < 1 || r.TTLSeconds > 300 {
		return false
	}
	if r.MaxConcurrency != nil && (*r.MaxConcurrency < 1 || *r.MaxConcurrency > 10000) {
		return false
	}
	for k, v := range r.Metadata {
		if k == "" || v == "" {
			return false
		}
	}
	if r.HealthPolicy != nil {
		p := r.HealthPolicy
		if p.FailureThreshold == nil || p.SuccessThreshold == nil {
			return false
		}
		if *p.FailureThreshold < 1 || *p.FailureThreshold > 10 ||
			*p.SuccessThreshold < 1 || *p.SuccessThreshold > 10 {
			return false
		}
	}
	if r.CircuitBreaker != nil {
		cb := r.CircuitBreaker
		if cb.FailureThreshold == nil || cb.OpenSeconds == nil {
			return false
		}
		if *cb.FailureThreshold < 1 || *cb.FailureThreshold > 20 ||
			*cb.OpenSeconds < 1 || *cb.OpenSeconds > 300 {
			return false
		}
	}
	if r.RateLimit != nil {
		rl := r.RateLimit
		if rl.RequestsPerSecond == nil || rl.Burst == nil {
			return false
		}
		if *rl.RequestsPerSecond < 1 || *rl.RequestsPerSecond > 10000 ||
			*rl.Burst < 1 || *rl.Burst > 10000 {
			return false
		}
	}
	return true
}

// rateLimitView is the public echo of a configured token-bucket policy.
type rateLimitView struct {
	RequestsPerSecond int `json:"requestsPerSecond"`
	Burst             int `json:"burst"`
}

// instanceView is the public representation of a registered instance. The
// lease token is deliberately excluded; it only appears in registerResponse.
// MaxConcurrency is omitted for instances registered without a cap, where
// concurrency is unbounded. RateLimit is omitted for instances registered
// without a policy, where admission is never rate limited.
type instanceView struct {
	Service        string            `json:"service"`
	Instance       string            `json:"instance"`
	Endpoint       string            `json:"endpoint"`
	Version        string            `json:"version"`
	Zone           string            `json:"zone"`
	Weight         int               `json:"weight"`
	TTLSeconds     int               `json:"ttlSeconds"`
	Metadata       map[string]string `json:"metadata"`
	ExpiresAt      string            `json:"expiresAt"`
	HealthStatus   string            `json:"healthStatus"`
	MaxConcurrency int               `json:"maxConcurrency,omitempty"`
	RateLimit      *rateLimitView    `json:"rateLimit,omitempty"`
}

func viewOf(inst instance) instanceView {
	v := instanceView{
		Service:        inst.Service,
		Instance:       inst.Instance,
		Endpoint:       inst.Endpoint,
		Version:        inst.Version,
		Zone:           inst.Zone,
		Weight:         inst.Weight,
		TTLSeconds:     inst.TTLSeconds,
		Metadata:       inst.Metadata,
		ExpiresAt:      inst.expiresAt.UTC().Format(time.RFC3339),
		HealthStatus:   inst.healthStatus(),
		MaxConcurrency: inst.MaxConcurrency,
	}
	if inst.rateLimit.configured {
		v.RateLimit = &rateLimitView{
			RequestsPerSecond: inst.rateLimit.requestsPerSecond,
			Burst:             inst.rateLimit.burst,
		}
	}
	return v
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
	if err := dec.Decode(&req); err != nil || dec.Decode(&struct{}{}) != io.EOF || !req.valid() {
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
	if req.MaxConcurrency != nil {
		inst.MaxConcurrency = *req.MaxConcurrency
	}
	if p := req.HealthPolicy; p != nil {
		inst.health = healthState{
			configured:       true,
			failureThreshold: *p.FailureThreshold,
			successThreshold: *p.SuccessThreshold,
			status:           healthHealthy,
		}
	}
	if cb := req.CircuitBreaker; cb != nil {
		inst.circuit = circuitState{
			configured:       true,
			failureThreshold: *cb.FailureThreshold,
			openSeconds:      *cb.OpenSeconds,
			state:            circuitClosed,
		}
	}
	if rl := req.RateLimit; rl != nil {
		inst.rateLimit = rateLimitState{
			configured:        true,
			requestsPerSecond: *rl.RequestsPerSecond,
			burst:             *rl.Burst,
		}
	}
	stored, overwritten := s.registry.register(inst, s.now())
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

// healthReportRequest is the body of a health report. Pointers distinguish
// an absent field from a zero value; both fields are required.
type healthReportRequest struct {
	Sequence *int64  `json:"sequence"`
	Status   *string `json:"status"`
}

// handleHealth serves /v1/services/{service}/instances/{instance}/health.
// Existence, lease token and policy checks run inside the registry under one
// lock so concurrent reports, heartbeats and deregistrations stay atomic;
// the body is decoded up front but its validation error is only surfaced
// after those checks, matching the documented error precedence.
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
	var req healthReportRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	bodyOK := dec.Decode(&req) == nil && dec.Decode(&struct{}{}) == io.EOF &&
		req.Sequence != nil && *req.Sequence >= 0 &&
		req.Status != nil && (*req.Status == "pass" || *req.Status == "fail")
	var seq int64
	pass := false
	if bodyOK {
		seq = *req.Sequence
		pass = *req.Status == "pass"
	}
	err := s.registry.reportHealth(service, name, r.Header.Get(leaseHeader), bodyOK, seq, pass, s.now())
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, errNotFound):
		writeError(w, http.StatusNotFound, "instance_not_found")
	case errors.Is(err, errLeaseConflict):
		writeError(w, http.StatusConflict, "lease_conflict")
	case errors.Is(err, errHealthDisabled):
		writeError(w, http.StatusConflict, "health_check_disabled")
	case errors.Is(err, errInvalidHealthReport):
		writeError(w, http.StatusBadRequest, "validation_error")
	case errors.Is(err, errHealthReportConflict):
		writeError(w, http.StatusConflict, "health_report_conflict")
	case errors.Is(err, errStaleHealthReport):
		writeError(w, http.StatusConflict, "stale_health_report")
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
