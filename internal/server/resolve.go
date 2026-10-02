package server

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// selectInstance picks one instance for key using weighted rendezvous (HRW)
// hashing. The score of a candidate depends only on the key, the instance
// name and its weight, so the choice is independent of registration order
// and of any other service. Removing a candidate only reassigns the keys
// that pointed at it; adding one only steals keys from existing candidates.
// Expected shares are proportional to weight.
func selectInstance(key string, candidates []instance) (instance, bool) {
	var best instance
	bestScore := math.Inf(-1)
	found := false
	for _, cand := range candidates {
		score := rendezvousScore(key, cand.Instance, cand.Weight)
		// Candidates arrive sorted by name; a strict greater-than keeps the
		// lexicographically smallest name on the (measure-zero) tie.
		if !found || score > bestScore {
			best, bestScore, found = cand, score, true
		}
	}
	return best, found
}

// rendezvousScore hashes (key, name) into a uniform u in (0, 1) and returns
// -weight / ln(u), the standard weighted-HRW score: for each key the winner
// is drawn with probability proportional to weight.
func rendezvousScore(key, name string, weight int) float64 {
	h := fnv.New64a()
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
	_, _ = h.Write(lenBuf[:n])
	_, _ = h.Write([]byte(key))
	n = binary.PutUvarint(lenBuf[:], uint64(len(name)))
	_, _ = h.Write(lenBuf[:n])
	_, _ = h.Write([]byte(name))
	// Map the 64-bit hash into (0, 1) exclusive so ln(u) stays finite.
	u := (float64(h.Sum64()>>11) + 0.5) / (1 << 53)
	return -float64(weight) / math.Log(u)
}
