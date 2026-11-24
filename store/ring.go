// This file implements consistent hashing with virtual nodes: the Ring
// that maps every key to its preference list (the N nodes that store it)
// and to the ordered list of fallback nodes used by sloppy quorums.

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
//
// A Ring is immutable after construction and safe for concurrent use.
type Ring struct {
	tokens []uint64            // all tokens, sorted
	owner  map[uint64]string   // token -> physical node
	nodes  []string            // physical nodes
	hash   func(string) uint64 // maps keys and tokens onto the ring
}

// NewRing places vnodes tokens per physical node, hashing with MD5.
//
// Parameters:
//   - nodes: the physical node ids.
//   - vnodes: the number of tokens per physical node.
//
// Returns:
//   - *Ring: the constructed ring.
func NewRing(nodes []string, vnodes int) *Ring { return NewRingWithHash(nodes, vnodes, hash64) }

// NewRingWithHash is NewRing with a custom hash (used to compare hashes).
//
// Token i of node n is placed at hash64("n#i").
//
// Parameters:
//   - nodes: the physical node ids; the slice is copied.
//   - vnodes: the number of tokens per physical node.
//   - hash64: maps strings (keys and token names) onto the 64-bit ring.
//
// Returns:
//   - *Ring: the constructed ring.
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
//
// Parameters:
//   - s: the key or token name to hash.
//
// Returns:
//   - uint64: the position on the ring.
func hash64(s string) uint64 {
	sum := md5.Sum([]byte(s)) // Dynamo hashes keys with MD5
	return binary.BigEndian.Uint64(sum[:8])
}

// Walk returns every physical node in ring order starting at key's
// position, without duplicates. The first N entries are the key's
// preference list; the rest are the fallbacks used by sloppy quorums.
//
// Parameters:
//   - key: the key to place on the ring.
//
// Returns:
//   - []string: all physical node ids, each exactly once, in walk order.
func (r *Ring) Walk(key string) []string {
	h := r.hash(key)
	// The key belongs to the first token at or after its hash; the modulo
	// below wraps past the last token back to the start of the ring.
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= h })
	seen := map[string]bool{}
	out := make([]string, 0, len(r.nodes))
	// Skip tokens of nodes already listed, so consecutive vnodes of one
	// physical node never count as two replicas.
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
//
// Parameters:
//   - key: the key to look up.
//   - n: the number of replicas; capped at the number of physical nodes.
//
// Returns:
//   - []string: the first n distinct nodes of Walk(key).
func (r *Ring) PreferenceList(key string, n int) []string {
	w := r.Walk(key)
	if n > len(w) {
		n = len(w)
	}
	return w[:n]
}

// NewFixedRing divides the hash space into q equal partitions and deals
// them to the nodes in turn (Dynamo's "strategy 3": Q fixed pieces, about
// Q/N per node). Partition i starts at i*2^64/q and belongs to nodes[i%N],
// so neighbouring partitions have different owners and the usual clockwise
// walk finds distinct replicas quickly.
//
// Parameters:
//   - nodes: the physical node ids.
//   - q: the number of partitions; at least len(nodes).
//
// Returns:
//   - *Ring: the constructed ring; its partition boundaries never move.
func NewFixedRing(nodes []string, q int) *Ring {
	r := &Ring{owner: map[uint64]string{}, nodes: append([]string(nil), nodes...), hash: hash64}
	for i := 0; i < q; i++ {
		t := uint64(i) * (^uint64(0)/uint64(q) + 1)
		r.tokens = append(r.tokens, t)
		r.owner[t] = nodes[i%len(nodes)]
	}
	return r
}

// WithNode returns a copy of a fixed ring after node id joins: it takes its
// fair share (q/N') of the partitions, evenly spaced around the ring. Only
// those partitions change owner.
//
// Parameters:
//   - id: the joining node.
//
// Returns:
//   - *Ring: the new ring; the receiver is unchanged.
func (r *Ring) WithNode(id string) *Ring {
	n := &Ring{owner: map[uint64]string{}, tokens: r.tokens, nodes: append(append([]string(nil), r.nodes...), id), hash: r.hash}
	count := map[string]int{}
	for t, o := range r.owner {
		n.owner[t] = o
		count[o]++
	}
	// Take evenly spaced partitions: spacing them out keeps the new node's
	// pieces apart, so its load is spread like the other nodes'. Donors
	// lose partitions in proportion to how many they own.
	want := len(r.tokens) / len(n.nodes)
	for j := 0; j < want; j++ {
		n.owner[n.tokens[j*len(n.tokens)/want]] = id
	}
	return n
}

// WithoutNode returns a copy of a fixed ring after node id leaves: each of
// its partitions goes to whichever remaining node holds the fewest.
//
// Parameters:
//   - id: the leaving node.
//
// Returns:
//   - *Ring: the new ring; the receiver is unchanged.
func (r *Ring) WithoutNode(id string) *Ring {
	n := &Ring{owner: map[uint64]string{}, tokens: r.tokens, hash: r.hash}
	count := map[string]int{}
	for _, o := range r.nodes {
		if o != id {
			n.nodes = append(n.nodes, o)
		}
	}
	for t, o := range r.owner {
		n.owner[t] = o
		count[o]++
	}
	for _, t := range r.tokens {
		if n.owner[t] != id {
			continue
		}
		to := n.nodes[0]
		for _, o := range n.nodes {
			if count[o] < count[to] {
				to = o
			}
		}
		n.owner[t] = to
		count[to]++
	}
	return n
}
