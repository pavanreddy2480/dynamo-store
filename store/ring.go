package store

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
)

// Ring is a consistent-hash ring with virtual nodes.
//
// Each physical node owns several tokens (positions) on a 64-bit ring.
// Many small, scattered ranges per node spread keys and replica load far
// more evenly than one token per node would.
type Ring struct {
	tokens []uint64            // all tokens, sorted
	owner  map[uint64]string   // token -> physical node
	nodes  []string            // physical nodes
	hash   func(string) uint64 // maps keys and tokens onto the ring
}

// NewRing places vnodes tokens per physical node, hashing with MD5.
func NewRing(nodes []string, vnodes int) *Ring { return NewRingWithHash(nodes, vnodes, hash64) }

// NewRingWithHash is NewRing with a custom hash (used to compare hashes).
func NewRingWithHash(nodes []string, vnodes int, hash64 func(string) uint64) *Ring {
	r := &Ring{owner: map[uint64]string{}, nodes: append([]string(nil), nodes...), hash: hash64}
	for _, n := range nodes {
		for i := 0; i < vnodes; i++ {
			t := hash64(fmt.Sprintf("%s#%d", n, i))
			r.tokens = append(r.tokens, t)
			r.owner[t] = n
		}
	}
	sort.Slice(r.tokens, func(i, j int) bool { return r.tokens[i] < r.tokens[j] })
	return r
}

// hash64 maps s onto the ring using the first 8 bytes of its MD5 digest.
func hash64(s string) uint64 {
	sum := md5.Sum([]byte(s)) // Dynamo hashes keys with MD5
	return binary.BigEndian.Uint64(sum[:8])
}

// Walk returns every physical node in ring order starting at key's
// position, without duplicates. The first N entries are the key's
// preference list; the rest are the fallbacks used by sloppy quorums.
func (r *Ring) Walk(key string) []string {
	h := r.hash(key)
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= h })
	seen := map[string]bool{}
	out := make([]string, 0, len(r.nodes))
	for j := 0; j < len(r.tokens) && len(out) < len(r.nodes); j++ {
		n := r.owner[r.tokens[(i+j)%len(r.tokens)]]
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// PreferenceList returns the n replicas responsible for key.
func (r *Ring) PreferenceList(key string, n int) []string {
	w := r.Walk(key)
	if n > len(w) {
		n = len(w)
	}
	return w[:n]
}
