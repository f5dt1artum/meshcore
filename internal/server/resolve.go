package server

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"net/url"
	"unicode/utf8"
)

// maxKeyBytes bounds the decoded route key accepted by resolve.
const maxKeyBytes = 256

type resolveResponse struct {
	Service  string       `json:"service"`
	Key      string       `json:"key"`
	Instance instanceView `json:"instance"`
}

// handleResolve serves GET /v1/resolve/{service}?key=... with the same
// optional version/zone filters as discovery. The chosen instance is computed
// from the current live snapshot only; resolving neither creates nor renews a
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
	hasKey := false
	for name, values := range query {
		if len(values) != 1 || values[0] == "" {
			writeError(w, http.StatusBadRequest, "validation_error")
			return
		}
		switch name {
		case "key":
			key, hasKey = values[0], true
		case "version":
			version = values[0]
		case "zone":
			zone = values[0]
		default:
			writeError(w, http.StatusBadRequest, "validation_error")
			return
		}
	}
	if !hasKey || len(key) < 1 || len(key) > maxKeyBytes || !utf8.ValidString(key) {
		writeError(w, http.StatusBadRequest, "validation_error")
		return
	}
	instances := s.registry.snapshot(service, version, zone, s.now())
	if len(instances) == 0 {
		writeError(w, http.StatusServiceUnavailable, "no_available_instance")
		return
	}
	writeJSON(w, http.StatusOK, resolveResponse{
		Service:  service,
		Key:      key,
		Instance: viewOf(selectInstance(instances, key)),
	})
}

// selectInstance performs weighted rendezvous hashing (HRW) over the live,
// already filtered candidates and returns the winner. Each candidate's score
// is a pure function of the route key, the instance name and its weight, so
// the result is independent of registration order, map iteration and other
// services. Because scores are computed per candidate, removing an instance
// leaves every key that did not land on it untouched, while adding one can
// only move keys onto the newcomer: the minimal-migration semantics of
// consistent hashing. Endpoint, metadata and lease state never enter the
// score, so a same-name, same-weight overwrite keeps every key's ownership;
// changing a weight changes the topology.
func selectInstance(instances []instance, key string) instance {
	best := 0
	bestScore := rendezvousScore(key, instances[0].Instance, instances[0].Weight)
	for i := 1; i < len(instances); i++ {
		score := rendezvousScore(key, instances[i].Instance, instances[i].Weight)
		// Equal scores are vanishingly unlikely, but the name tiebreak keeps
		// selection deterministic even then.
		if score > bestScore || (score == bestScore && instances[i].Instance < instances[best].Instance) {
			best, bestScore = i, score
		}
	}
	return instances[best]
}

// rendezvousScore maps (key, name, weight) to a score in (0, +Inf). The hash
// is normalized to u in (0, 1] and the closed-form weighted HRW score is
// weight/-ln(u); with equal weights this reduces to picking the largest hash,
// plain rendezvous hashing, while shares stay proportional to weight across a
// large, spread-out set of route keys.
func rendezvousScore(key, name string, weight int) float64 {
	h := fnv.New64a()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	h.Write(lenBuf[:])
	io.WriteString(h, key)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(name)))
	h.Write(lenBuf[:])
	io.WriteString(h, name)
	// Normalize into (0, 1): the shift avoids u == 1, whose -ln is zero.
	u := (float64(h.Sum64()>>1) + 1.0) / (float64(uint64(1)<<63) + 1.0)
	return float64(weight) / -math.Log(u)
}
