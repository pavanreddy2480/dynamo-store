// This file implements the Merkle tree a node keeps for each peer it shares
// keys with. Anti-entropy compares two such trees top-down and only
// exchanges the keys under leaves whose hashes differ (see repair.go).
//
//	                 h[1]  root
//	              /        \
//	          h[2]          h[3]
//	         /    \        /    \
//	      h[4]   h[5]   h[6]   h[7]   leaves: XOR of their keys' hashes
//	      k1,k9  k4     -      k2,k7
//
// A key's leaf is fixed by the top bits of its ring hash, so two nodes put
// the same key in the same leaf. A leaf hash is the XOR of its keys'
// hashes, so changing one key updates its leaf in O(1); the path to the
// root is then rehashed in O(depth).

package store

import (
	"hash/fnv"
	"strconv"
)

// merkle is a binary hash tree over the keys one node shares with one peer.
//
// It is stored as an implicit heap: h[1] is the root, the children of h[i]
// are h[2i] and h[2i+1], and the leaves are h[L..2L) with L = 1<<depth.
// It is not safe for concurrent use; the owning Node guards it with mu.
type merkle struct {
	depth int                 // levels below the root; L = 1<<depth leaves
	h     []uint64            // heap of node hashes, length 2L
	keys  []map[string]uint64 // per leaf: key -> key hash
}

// newMerkle returns an empty tree with 1<<depth leaves.
//
// Parameters:
//   - depth: levels below the root, in [0, 24].
//
// Returns:
//   - *merkle: a tree in which every hash is zero.
func newMerkle(depth int) *merkle {
	L := 1 << depth
	return &merkle{depth: depth, h: make([]uint64, 2*L), keys: make([]map[string]uint64, L)}
}

// leafOf returns the heap index of the leaf that holds key.
//
// Parameters:
//   - ringHash: the key's position on the ring (hash64(key)).
//
// Returns:
//   - int: an index in [L, 2L).
func (t *merkle) leafOf(ringHash uint64) int {
	if t.depth == 0 {
		return 1
	}
	return 1<<t.depth + int(ringHash>>(64-t.depth))
}

// set records that key now hashes to kh (0 removes the key), updating its
// leaf and the path to the root.
//
// Parameters:
//   - key: the key that changed.
//   - ringHash: hash64(key), which picks the leaf.
//   - kh: hashVersions of the key's new sibling set, or 0 to remove it.
//
// Returns:
//   - int: the number of tree nodes rehashed (for cost accounting).
func (t *merkle) set(key string, ringHash, kh uint64) int {
	i := t.leafOf(ringHash)
	m := t.keys[i-len(t.keys)]
	if m == nil {
		m = map[string]uint64{}
		t.keys[i-len(t.keys)] = m
	}
	old := m[key]
	if old == kh {
		return 0
	}
	if kh == 0 {
		delete(m, key)
	} else {
		m[key] = kh
	}
	t.h[i] ^= old ^ kh
	return 1 + t.rehash(i)
}

// rehash recomputes every ancestor of heap index i.
//
// Parameters:
//   - i: a heap index whose own hash is already up to date.
//
// Returns:
//   - int: the number of ancestors rehashed.
func (t *merkle) rehash(i int) int {
	n := 0
	for i > 1 {
		i /= 2
		t.h[i] = mix(t.h[2*i], t.h[2*i+1])
		n++
	}
	return n
}

// leafKeys returns a copy of the key hashes stored under leaf i.
//
// Parameters:
//   - i: a leaf's heap index, in [L, 2L).
//
// Returns:
//   - map[string]uint64: key -> key hash; empty for an empty leaf.
func (t *merkle) leafKeys(i int) map[string]uint64 {
	out := map[string]uint64{}
	for k, h := range t.keys[i-len(t.keys)] {
		out[k] = h
	}
	return out
}

// mix combines two child hashes into their parent's hash. The order of the
// children matters, and two empty children give an empty parent so an
// empty subtree costs nothing to compare.
//
// Parameters:
//   - a, b: the left and right child hashes.
//
// Returns:
//   - uint64: the parent hash.
func mix(a, b uint64) uint64 {
	if a == 0 && b == 0 {
		return 0
	}
	return splitmix(a ^ splitmix(b+0x9e3779b97f4a7c15))
}

// splitmix is the SplitMix64 finaliser, a fast well-mixing 64-bit hash.
func splitmix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}

// hashVersions hashes a key together with its canonical sibling set.
//
// Two replicas holding the same versions for a key get the same hash,
// because merge keeps sibling sets sorted and VC.String is sorted by node.
//
// Parameters:
//   - key: the key; included so equal sets under different keys differ.
//   - vs: the key's sibling set, in canonical order.
//
// Returns:
//   - uint64: a non-zero hash, or 0 when vs is empty (the key is absent).
func hashVersions(key string, vs []Version) uint64 {
	if len(vs) == 0 {
		return 0
	}
	f := fnv.New64a()
	f.Write([]byte(key))
	for _, v := range vs {
		f.Write([]byte{0})
		f.Write([]byte(v.Value))
		f.Write([]byte(v.Dot.Node + ":" + strconv.FormatUint(v.Dot.Counter, 10) + v.Ctx.String() + strconv.FormatInt(v.TS, 10)))
	}
	if h := f.Sum64(); h != 0 {
		return h
	}
	return 1
}
