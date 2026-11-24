// This file implements anti-entropy, the background exchange that makes two
// replicas agree on every key they share. Two strategies are kept so they
// can be measured against each other (experiment A6):
//
//   - MerkleSync (default): compare per-peer Merkle trees top-down and send
//     only the keys under leaves whose hashes differ.
//   - FullStateSync: send every shared key both ways, whatever its state.
//
// A Merkle exchange started by node a with peer b:
//
//	a                                   b
//	|-- merkleReq{[1], [root]} -------->|  compare with b's tree for a
//	|<------------- merkleResp{diff} ---|  indices whose hashes differ
//	|-- merkleReq{children of diff} --->|  ... one level per round trip,
//	|<------------- merkleResp{diff} ---|      until diff holds only leaves
//	|-- leafReq{leaves, key hashes} --->|
//	|<--- leafResp{b's items, want} ----|  b sends keys whose hash differs
//	|-- pushReq{a's items for want} --->|  and asks for the ones it lacks
//
// Equal trees end the exchange after the first round trip.

package store

import (
	"context"
	"sync/atomic"
)

// SyncMode selects the anti-entropy strategy.
type SyncMode int

const (
	// MerkleSync compares Merkle trees and exchanges only differing keys.
	MerkleSync SyncMode = iota
	// FullStateSync sends every shared key in both directions (the
	// original strategy, kept to measure what Merkle trees save).
	FullStateSync
)

// SyncStats counts the traffic of anti-entropy exchanges, both directions
// together. Bytes uses the size model of versionsSize.
type SyncStats struct {
	RoundTrips int64 // request/response pairs
	Hashes     int64 // tree or key hashes sent
	Keys       int64 // keys whose versions were sent
	Bytes      int64 // estimated payload bytes
}

// syncCounters is the atomic form of SyncStats kept by each Node.
type syncCounters struct{ rt, hashes, keys, bytes atomic.Int64 }

// add records one round trip that carried the given hashes and items.
func (s *syncCounters) add(hashes int, items map[string][]Version, extraBytes int) {
	s.rt.Add(1)
	s.hashes.Add(int64(hashes))
	s.keys.Add(int64(len(items)))
	s.bytes.Add(int64(8*hashes + extraBytes + itemsSize(items)))
}

// load returns the counters as a SyncStats value.
func (s *syncCounters) load() SyncStats {
	return SyncStats{s.rt.Load(), s.hashes.Load(), s.keys.Load(), s.bytes.Load()}
}

// itemsSize estimates the wire size of a batch of keys and their versions:
// the key, then per version its value, dot, timestamp and context entries.
func itemsSize(items map[string][]Version) int {
	n := 0
	for k, vs := range items {
		n += len(k)
		for _, v := range vs {
			n += len(v.Value) + len(v.Dot.Node) + 16
			for node := range v.Ctx {
				n += len(node) + 8
			}
		}
	}
	return n
}

// Anti-entropy messages; see the diagram at the top of this file.
type (
	// syncReq carries one side of a full-state exchange: every key the
	// sender owns that the receiver should also hold.
	syncReq struct{ Items map[string][]Version }
	// syncResp is the receiver's half of a full-state exchange.
	syncResp struct{ Items map[string][]Version }
	// merkleReq sends the sender's hashes for some heap indices of its
	// tree for the receiver.
	merkleReq struct {
		Nodes  []int    // heap indices
		Hashes []uint64 // the sender's hash at each index
	}
	// merkleResp lists the indices of a merkleReq whose hashes differ.
	merkleResp struct{ Diff []int }
	// leafReq sends every key hash under leaves that differ.
	leafReq struct {
		Leaves []int             // differing leaves (heap indices)
		Keys   map[string]uint64 // the sender's keys under them
	}
	// leafResp returns the receiver's versions for keys whose hashes
	// differ, and the keys it wants from the sender.
	leafResp struct {
		Items map[string][]Version
		Want  []string
	}
	// pushReq delivers versions the receiver asked for; answered by
	// putResp.
	pushReq struct{ Items map[string][]Version }
)

// put stores a merged sibling set for an owned key and keeps the per-peer
// Merkle trees in step. Every write to nd.data goes through it.
//
// The caller holds nd.mu.
//
// Parameters:
//   - k: the key.
//   - vs: the key's new sibling set, already merged.
func (nd *Node) put(k string, vs []Version) {
	nd.data[k] = vs
	if nd.cfg.AntiEntropy != MerkleSync {
		return
	}
	if nd.cfg.LazyTrees {
		nd.dirty[k] = struct{}{}
		return
	}
	nd.updateTrees(k)
}

// updateTrees rehashes key k into the tree of every peer that shares it.
// The caller holds nd.mu.
func (nd *Node) updateTrees(k string) {
	pl := nd.ring.PreferenceList(k, nd.cfg.N)
	if !contains(pl, nd.ID) {
		return
	}
	rh, kh := nd.ring.hash(k), hashVersions(k, nd.data[k])
	for _, p := range pl {
		if p != nd.ID {
			nd.treeFor(p).set(k, rh, kh)
		}
	}
}

// tree returns this node's Merkle tree for peer, bringing the trees up to
// date first when LazyTrees is on: every key written since the last call is
// hashed once, however many times it was written. The caller holds nd.mu.
func (nd *Node) tree(peer string) *merkle {
	for k := range nd.dirty {
		nd.updateTrees(k)
		delete(nd.dirty, k)
	}
	return nd.treeFor(peer)
}

// treeFor returns the tree for peer, creating it if needed, without
// flushing dirty keys. The caller holds nd.mu.
func (nd *Node) treeFor(peer string) *merkle {
	t := nd.trees[peer]
	if t == nil {
		t = newMerkle(nd.cfg.MerkleDepth)
		nd.trees[peer] = t
	}
	return t
}

// contains reports whether s holds x.
func contains(s []string, x string) bool {
	for _, e := range s {
		if e == x {
			return true
		}
	}
	return false
}

// SyncStats returns the anti-entropy traffic this node started.
func (nd *Node) SyncStats() SyncStats { return nd.stats.load() }

// itemsFor returns this node's owned keys whose preference list includes peer.
//
// Only owned data is offered; hints are delivered by hinted handoff
// instead. Takes nd.mu.
//
// Parameters:
//   - peer: the node the items will be sent to.
//
// Returns:
//   - map[string][]Version: key -> deep copy of this node's sibling set,
//     for every owned key that peer should also hold.
func (nd *Node) itemsFor(peer string) map[string][]Version {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	out := map[string][]Version{}
	for k, vs := range nd.data {
		if contains(nd.ring.PreferenceList(k, nd.cfg.N), peer) {
			out[k] = cloneVersions(vs)
		}
	}
	return out
}

// absorb merges versions received during anti-entropy into owned data.
//
// Takes nd.mu.
//
// Parameters:
//   - items: key -> versions sent by the peer; cloned before being stored.
func (nd *Node) absorb(items map[string][]Version) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for k, vs := range items {
		nd.put(k, nd.rules().merge(nd.data[k], cloneVersions(vs)...))
	}
}

// handleSync answers a full-state request: take the peer's items, then
// send back everything this node owns that the peer should also hold.
//
// Absorbing first means the reply already includes anything the peer sent,
// so both sides end the exchange with the same versions for shared keys.
func (nd *Node) handleSync(from string, r syncReq) syncResp {
	nd.absorb(r.Items)
	return syncResp{Items: nd.itemsFor(from)}
}

// handleMerkle answers a merkleReq: which of the sender's hashes differ
// from this node's tree for the sender.
func (nd *Node) handleMerkle(from string, r merkleReq) merkleResp {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	t := nd.tree(from)
	var diff []int
	for i, idx := range r.Nodes {
		if t.h[idx] != r.Hashes[i] {
			diff = append(diff, idx)
		}
	}
	return merkleResp{Diff: diff}
}

// handleLeaf answers a leafReq: send this node's versions of every key
// under the given leaves whose hash differs from the sender's, and ask for
// every key the sender has with a hash this node does not.
func (nd *Node) handleLeaf(from string, r leafReq) leafResp {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	t := nd.tree(from)
	resp := leafResp{Items: map[string][]Version{}}
	for _, leaf := range r.Leaves {
		for k, h := range t.leafKeys(leaf) {
			if r.Keys[k] != h {
				resp.Items[k] = cloneVersions(nd.data[k])
			}
		}
	}
	for k, h := range r.Keys {
		if kh := hashVersions(k, nd.data[k]); kh != h {
			resp.Want = append(resp.Want, k)
		}
	}
	return resp
}

// syncWith runs one anti-entropy exchange with peer, using the configured
// SyncMode.
//
// Each round trip waits at most ReplicaTimeout.
//
// Parameters:
//   - peer: the node to synchronise with.
//
// Returns:
//   - error: nil on success; ErrTimeout if a request or reply was lost or
//     late. An interrupted exchange is harmless: whatever was absorbed is
//     merged, and the next round finishes the job.
func (nd *Node) syncWith(peer string) error {
	if nd.cfg.AntiEntropy == FullStateSync {
		return nd.fullSync(peer)
	}
	return nd.merkleSync(peer)
}

// send performs one round trip to peer with a ReplicaTimeout deadline.
func (nd *Node) send(peer string, req any) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
	defer cancel()
	return nd.net.Send(ctx, nd.ID, peer, req)
}

// fullSync sends every shared key to peer and absorbs peer's reply.
func (nd *Node) fullSync(peer string) error {
	mine := nd.itemsFor(peer)
	resp, err := nd.send(peer, syncReq{Items: mine})
	if err != nil {
		return err
	}
	theirs := resp.(syncResp).Items
	nd.stats.add(0, mine, 0)
	nd.stats.keys.Add(int64(len(theirs)))
	nd.stats.bytes.Add(int64(itemsSize(theirs)))
	nd.absorb(theirs)
	return nil
}

// merkleSync walks this node's tree for peer against peer's tree for this
// node, one level per round trip, then exchanges the keys under the leaves
// that differ.
func (nd *Node) merkleSync(peer string) error {
	nodes := []int{1}
	for {
		nd.mu.Lock()
		t := nd.tree(peer)
		hs := make([]uint64, len(nodes))
		for i, idx := range nodes {
			hs[i] = t.h[idx]
		}
		nd.mu.Unlock()
		resp, err := nd.send(peer, merkleReq{Nodes: nodes, Hashes: hs})
		if err != nil {
			return err
		}
		diff := resp.(merkleResp).Diff
		nd.stats.add(len(nodes), nil, 4*len(nodes)+4*len(diff))
		if len(diff) == 0 {
			return nil
		}
		if diff[0] >= len(t.h)/2 { // leaves reached
			return nd.leafSync(peer, diff)
		}
		nodes = nodes[:0:0]
		for _, i := range diff {
			nodes = append(nodes, 2*i, 2*i+1)
		}
	}
}

// leafSync exchanges the keys under the given differing leaves with peer.
func (nd *Node) leafSync(peer string, leaves []int) error {
	nd.mu.Lock()
	keys := map[string]uint64{}
	for _, leaf := range leaves {
		for k, h := range nd.tree(peer).leafKeys(leaf) {
			keys[k] = h
		}
	}
	nd.mu.Unlock()
	resp, err := nd.send(peer, leafReq{Leaves: leaves, Keys: keys})
	if err != nil {
		return err
	}
	lr := resp.(leafResp)
	keyBytes := 4 * len(leaves)
	for k := range keys {
		keyBytes += len(k)
	}
	for _, k := range lr.Want {
		keyBytes += len(k)
	}
	nd.stats.add(len(keys), lr.Items, keyBytes)
	nd.absorb(lr.Items)
	if len(lr.Want) == 0 {
		return nil
	}
	nd.mu.Lock()
	push := map[string][]Version{}
	for _, k := range lr.Want {
		push[k] = cloneVersions(nd.data[k])
	}
	nd.mu.Unlock()
	if _, err := nd.send(peer, pushReq{Items: push}); err != nil {
		return err
	}
	nd.stats.add(0, push, 0)
	return nil
}
