// This file implements consistent hashing with virtual nodes: the Ring
// that maps every key to its preference list (the N nodes that store it)
// and to the ordered list of fallback nodes used by sloppy quorums.

package ring

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
	plists [][]string          // plists[i]: Walk order for keys in partition i
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
	return r.index()
}

// Hash64 exposes the ring's key hash (MD5, first 8 bytes) so experiments can
// compare placement schemes that hash keys the same way.
func Hash64(s string) uint64 { return hash64(s) }

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

// Partition returns the index of the partition (the gap between two
// consecutive tokens) that key falls in. Every key of a partition has the
// same preference list, which is what lets replicas keep one Merkle tree
// per partition.
//
// Parameters:
//   - key: the key to place on the ring.
//
// Returns:
//   - int: the key's partition, in [0, number of tokens).
//   - uint64: the key's ring hash, so callers need not hash it again.
func (r *Ring) Partition(key string) (int, uint64) {
	h := r.hash(key)
	// The key belongs to the first token at or after its hash; the modulo
	// wraps past the last token back to the start of the ring.
	return sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= h }) % len(r.tokens), h
}

// walkFrom lists every physical node in ring order starting at token i,
// skipping nodes already listed, so consecutive tokens of one physical node
// never count as two replicas.
func (r *Ring) walkFrom(i int) []string {
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

// index sorts the tokens and precomputes every partition's walk. Every
// constructor calls it last.
func (r *Ring) index() *Ring {
	sort.Slice(r.tokens, func(i, j int) bool { return r.tokens[i] < r.tokens[j] })
	r.plists = make([][]string, len(r.tokens))
	for i := range r.tokens {
		r.plists[i] = r.walkFrom(i)
	}
	return r
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
//     The slice is shared; callers must not modify it.
func (r *Ring) Walk(key string) []string {
	p, _ := r.Partition(key)
	return r.plists[p]
}

// PartitionList returns the n replicas of partition p.
//
// Parameters:
//   - p: a partition index from Partition.
//   - n: the number of replicas; capped at the number of physical nodes.
//
// Returns:
//   - []string: the first n nodes of the partition's walk (shared; do not
//     modify).
func (r *Ring) PartitionList(p, n int) []string {
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	return r.plists[p][:n]
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
	p, _ := r.Partition(key)
	return r.PartitionList(p, n)
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
	return r.index()
}

// WithNode returns a copy of a fixed ring after node id joins: it takes its
// fair share (q/N') of the partitions, always taken from the node that owns
// the most, so every old node gives up about the same number. Only those
// partitions change owner.
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
	// Take want partitions one at a time. Each time, the donor is the node
	// that owns the most partitions right now (ties: lowest name), so every
	// old node gives up a fair share whatever Q and N are. The cursor steps
	// around the ring by q/want, which spreads the newcomer's pieces apart.
	want := len(r.tokens) / len(n.nodes)
	stride := len(r.tokens) / want
	cursor := 0
	for j := 0; j < want; j++ {
		donor := ""
		for _, o := range r.nodes {
			if donor == "" || count[o] > count[donor] || (count[o] == count[donor] && o < donor) {
				donor = o
			}
		}
		for k := 0; k < len(n.tokens); k++ {
			i := (cursor + k) % len(n.tokens)
			if n.owner[n.tokens[i]] == donor {
				n.owner[n.tokens[i]] = id
				count[donor]--
				cursor = i + stride
				break
			}
		}
	}
	return n.index()
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
	return n.index()
}

// Nodes returns the physical nodes on the ring, in the order they were
// added. The slice must not be modified.
func (r *Ring) Nodes() []string { return r.nodes }

// Partitions returns the number of partitions (tokens) on the ring.
func (r *Ring) Partitions() int { return len(r.tokens) }
