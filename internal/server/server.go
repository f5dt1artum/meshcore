// Package server exposes the public HTTP surface of MeshCore.
//
// Besides process health, the surface covers in-process service registration,
// lease renewal, deregistration and discovery. Registrations live in memory
// only: a process restart starts from an empty registry.
package server

import (
	"encoding/json"
	"net/http"
	"time"
)

// Version is the baseline release identifier.
const Version = "0.1.0"

type health struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version"`
}

// server bundles the registry with a clock so tests can control expiration.
type server struct {
	registry *registry
	now      func() time.Time
}

func newServer() *server {
	return &server{registry: newRegistry(), now: time.Now}
}

// Handler returns the HTTP surface served by MeshCore.
func Handler() http.Handler {
	return newServer().handler()
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, `{"error":{"code":"method_not_allowed"}}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(health{Status: "ok", Service: "meshcore", Version: Version})
	})
	mux.HandleFunc("/v1/services/{service}/instances/{instance}", s.handleInstance)
	mux.HandleFunc("/v1/services/{service}/instances/{instance}/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/v1/discovery/{service}", s.handleDiscovery)
	mux.HandleFunc("/v1/resolve/{service}", s.handleResolve)
	return mux
}
