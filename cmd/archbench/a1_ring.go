// This file implements experiment A1, which justifies the consistent-hashing
// ring's hash function and its number of virtual nodes per physical node. It
// places 100,000 keys on rings built with FNV-1a and with the store's default
// MD5 hash at several virtual-node counts, and reports how evenly the
// resulting replica load is spread across five nodes.

package main

import (
	"fmt"
	"hash/fnv"
	"math"

	"dynamostore/store"
)

// fnv64 hashes a string with 64-bit FNV-1a.
//
// It is the cheap, non-cryptographic alternative that A1 compares against the
// MD5-based hash the store uses by default.
//
// Parameters:
//   - s: the string to hash (a ring token or a key).
//
// Returns:
//   - uint64: the FNV-1a hash of s.
func fnv64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// ringBalance runs experiment A1 and appends its table to the report.
//
// For each hash function and virtual-node count it builds a five-node ring,
// asks for the N=3 preference list of 100,000 keys, and counts how many
// replicas land on each node. With N=3 of 5 nodes, a perfectly balanced ring
// puts 60% of keys on every node. It reports the least and most loaded
// node's share, their ratio, and the coefficient of variation across nodes.
func ringBalance() {
	w("## A1. Ring hash function and virtual nodes\n\n")
	w("100,000 keys, 5 nodes, N=3. Each node would hold 60%% of the keys if perfectly balanced. Max/min is the most-loaded node's share over the least-loaded node's share.\n\n")
	w("| hash | vnodes/node | min share | max share | max/min | coefficient of variation |\n|---|---|---|---|---|---|\n")
	const keys = 100000
	ids := nodes(5)
	// A nil hash function selects store.NewRing, i.e. the store's default
	// MD5-based hash, so the default is measured exactly as shipped.
	for _, h := range []struct {
		name string
		f    func(string) uint64
	}{{"FNV-1a", fnv64}, {"MD5", nil}} {
		for _, vn := range []int{1, 8, 64, 256} {
			var r *store.Ring
			if h.f == nil {
				r = store.NewRing(ids, vn)
			} else {
				r = store.NewRingWithHash(ids, vn, h.f)
			}
			// Count replicas, not primaries: storage and request load follow
			// every node in the preference list, not just the first.
			cnt := map[string]float64{}
			for k := 0; k < keys; k++ {
				for _, p := range r.PreferenceList(fmt.Sprintf("user:%d", k), 3) {
					cnt[p]++
				}
			}
			// Convert counts to each node's share of keys and accumulate the
			// min, max, sum and sum of squares in one pass.
			mn, mx, sum, sq := math.Inf(1), 0.0, 0.0, 0.0
			for _, id := range ids {
				c := cnt[id] / keys
				mn, mx = math.Min(mn, c), math.Max(mx, c)
				sum += c
				sq += c * c
			}
			// Coefficient of variation = population standard deviation / mean,
			// with variance computed as E[x^2] - E[x]^2.
			mean := sum / 5
			cv := math.Sqrt(sq/5-mean*mean) / mean
			// A node that owns no keys at all (possible with 1 vnode) makes
			// the ratio unbounded; print infinity instead of dividing by zero.
			ratio := "∞"
			if mn > 0 {
				ratio = fmt.Sprintf("%.2f", mx/mn)
			}
			w("| %s | %d | %.1f%% | %.1f%% | %s | %.3f |\n", h.name, vn, 100*mn, 100*mx, ratio, cv)
		}
	}
}
