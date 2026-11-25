// This file defines a storage replica (Node) and its configuration: node
// construction and shutdown, the background hinted-handoff loop, the
// network entry point that dispatches every incoming request, and the
// read-only accessors used by tests and benchmarks. Coordinator logic lives
// in coordinator.go and the repair mechanisms in repair.go.

package store

import (
	"dynamostore/merkle"
	"dynamostore/network"
	"dynamostore/ring"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// NodeConfig configures a replica.
//
// Every node in a Cluster is built from the same NodeConfig (Config.Node);
// only ClockSkew is later changed per node, through Cluster.SetSkew.
type NodeConfig struct {
	N               int           // replicas per key
	Policy          Policy        // conflict handling on the replica
	ReplicaTimeout  time.Duration // how long a coordinator waits for replicas
	ReadRepair      bool          // push newer versions to stale replicas after reads
	HandoffInterval time.Duration // how often hinted handoff is attempted
	ClockSkew       time.Duration // added to this node's wall clock (LWW experiments)
	Clocks          ClockMode     // how versions are ordered
	// ReplicateNewOnly sends only the new version to other replicas instead
	// of the coordinator's whole sibling set (kept to measure why the
	// default matters).
	ReplicateNewOnly bool
	AntiEntropy      SyncMode // how anti-entropy compares replicas
	// AntiEntropyInterval, when positive, makes every node sync with one
	// random reachable peer that often. Zero leaves anti-entropy to explicit
	// Cluster.AntiEntropy calls.
	AntiEntropyInterval time.Duration
	// ReadFromRonly makes a read query only R replicas instead of all N
	// (and so also disables read repair on the replicas not asked).
	ReadFromROnly bool
	// MerkleOneShot fetches a differing partition's whole tree in one round
	// trip after the roots instead of walking it one level per round trip.
	MerkleOneShot bool
	MerkleDepth   int // levels below each partition tree's root: 1<<depth leaves
}

// Node is one storage replica. Any node can coordinate a request.
//
// A node keeps two kinds of state: data, the keys it is a preferred replica
// for, and hints, writes it accepted as a sloppy-quorum fallback for another
// node. Both are sibling sets kept in canonical form by rules.merge.
//
// All mutable state is guarded by mu. No method holds mu while sending on
// the network, so a node keeps answering requests while requests it
// coordinates are in flight.
type Node struct {
	ID   string                    // node id, e.g. "n1"; also its network endpoint id
	cfg  NodeConfig                // replica settings (ClockSkew is guarded by mu)
	net  *network.Network          // network the node is registered on
	ring atomic.Pointer[ring.Ring] // shared ring used to compute preference lists; swapped when a node joins

	mu     sync.Mutex                      // guards data, hints, issued, trees and cfg.ClockSkew
	data   map[string][]Version            // key -> sibling set for keys this node owns
	hints  map[string]map[string][]Version // intended owner -> key -> versions
	issued map[string]uint64               // highest counter this node issued per key
	trees  map[int]*merkle.Tree            // partition -> Merkle tree of its keys (replicated partitions only)

	stop chan struct{}  // closed by close to stop the handoff loop
	wg   sync.WaitGroup // tracks the handoff goroutine so close can wait for it

	coordinated atomic.Int64 // client requests this node coordinated
	stats       syncCounters // anti-entropy traffic this node started
}

// Coordinated returns how many client requests this node coordinated.
//
// The counter is updated atomically and can be read at any time without
// locking.
//
// Returns:
//   - int64: the number of client reads and writes (coordGetReq and
//     coordPutReq) this node has handled since it was created.
func (nd *Node) Coordinated() int64 { return nd.coordinated.Load() }

// newNode creates a replica and registers it on net. Call start to begin
// background hinted handoff.
//
// Parameters:
//   - id: the node's id and network endpoint name.
//   - cfg: replica settings; copied into the node.
//   - net: the network to register the node's handler on.
//   - rg: the ring used to compute preference lists and fallbacks.
//
// Returns:
//   - *Node: the registered node, not yet started.
func newNode(id string, cfg NodeConfig, net *network.Network, rg *ring.Ring) *Node {
	nd := &Node{
		ID: id, cfg: cfg, net: net,
		data:   map[string][]Version{},
		hints:  map[string]map[string][]Version{},
		issued: map[string]uint64{},
		trees:  map[int]*merkle.Tree{},
		stop:   make(chan struct{}),
	}
	nd.ring.Store(rg)
	net.Register(id, nd.handle)
	return nd
}

// start launches the background loops: hinted handoff every
// HandoffInterval and, if AntiEntropyInterval is positive, anti-entropy with
// a random reachable peer. A zero interval disables the matching loop.
//
// The loops run in their own goroutines until close is called. start must
// be called at most once per node.
func (nd *Node) start() {
	loop := func(every time.Duration, f func()) {
		if every <= 0 {
			return
		}
		nd.wg.Add(1)
		go func() {
			defer nd.wg.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-nd.stop:
					return
				case <-t.C:
					f()
				}
			}
		}()
	}
	loop(nd.cfg.HandoffInterval, nd.handoffHints)
	loop(nd.cfg.AntiEntropyInterval, func() {
		peers := nd.ring.Load().Nodes()
		if p := peers[rand.Intn(len(peers))]; p != nd.ID && nd.net.Reachable(nd.ID, p) {
			_ = nd.syncWith(p)
		}
	})
}

// rules returns the version-ordering rules this node is configured with.
//
// Returns:
//   - rules: the node's Policy and ClockMode, bundled for merging and
//     comparing versions.
func (nd *Node) rules() rules { return rules{policy: nd.cfg.Policy, clocks: nd.cfg.Clocks} }

// close stops the background loop and waits for it to exit.
//
// Goroutines started for in-flight requests (replication, read repair) are
// not waited for; they end on their own once their timeouts fire. close
// must be called at most once per node.
func (nd *Node) close() { close(nd.stop); nd.wg.Wait() }

// handle is the node's network entry point. It dispatches one request by
// type and returns the reply. Replica-level requests (putReq, getReq
// and the anti-entropy messages) only touch local state; coordinator requests fan out to other
// replicas.
//
// The Network calls it concurrently, once per delivered message. It panics
// on an unknown request type, which is a programming error rather than a
// network fault.
//
// Parameters:
//   - from: the id of the sending endpoint.
//   - req: the request message; one of the types in messages.go.
//
// Returns:
//   - any: the matching response (putResp, getResp, coordPutResp,
//     coordGetResp or syncResp).
func (nd *Node) handle(from string, req any) any {
	switch r := req.(type) {
	case putReq:
		nd.mu.Lock()
		// A hint naming this node itself is ordinary data for it; keeping
		// it as a hint would only make the node hand it off to itself.
		if r.Hint != "" && r.Hint != nd.ID {
			m := nd.hints[r.Hint]
			if m == nil {
				m = map[string][]Version{}
				nd.hints[r.Hint] = m
			}
			m[r.Key] = nd.rules().merge(m[r.Key], cloneVersions(r.V)...)
		} else {
			nd.put(r.Key, nd.rules().merge(nd.data[r.Key], cloneVersions(r.V)...))
		}
		nd.mu.Unlock()
		return putResp{}
	case getReq:
		return getResp{Versions: nd.localRead(r.Key)}
	case coordPutReq:
		nd.coordinated.Add(1)
		return nd.coordPut(r)
	case coordGetReq:
		nd.coordinated.Add(1)
		return nd.coordGet(r)
	case syncReq:
		return nd.handleSync(from, r)
	case merkleReq:
		return nd.handleMerkle(r)
	case leafReq:
		return nd.handleLeaf(r)
	case pushReq:
		nd.absorb(r.Items)
		return putResp{}
	}
	panic(fmt.Sprintf("unknown request %T", req))
}

// localRead returns owned versions plus any hinted versions held for others.
//
// Hints are included so that a fallback node answering a sloppy read
// returns the writes it is holding on the owner's behalf. Takes nd.mu.
//
// Parameters:
//   - key: the key to read.
//
// Returns:
//   - []Version: a deep copy of the merged sibling set, safe to use after
//     the lock is released; empty if the node holds nothing for key.
func (nd *Node) localRead(key string) []Version {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	vs := cloneVersions(nd.data[key])
	for _, m := range nd.hints {
		if h, ok := m[key]; ok {
			vs = nd.rules().merge(vs, cloneVersions(h)...)
		}
	}
	return vs
}

// Snapshot returns the versions this node owns for key (no hints).
//
// It is meant for tests and benchmarks that inspect replica state directly.
// Takes nd.mu.
//
// Parameters:
//   - key: the key to inspect.
//
// Returns:
//   - []Version: a deep copy of the node's owned sibling set for key.
func (nd *Node) Snapshot(key string) []Version {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return cloneVersions(nd.data[key])
}

// Inject merges vs into this node's owned versions of key, bypassing the
// network. Benchmarks use it to build large replica states quickly.
//
// Parameters:
//   - key: the key to write.
//   - vs: versions to merge; cloned before being stored.
func (nd *Node) Inject(key string, vs ...Version) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	nd.put(key, nd.rules().merge(nd.data[key], cloneVersions(vs)...))
}

// HintCount returns how many hinted keys this node is holding.
//
// Takes nd.mu.
//
// Returns:
//   - int: the number of (owner, key) pairs with undelivered hints; zero
//     once hinted handoff has delivered everything.
func (nd *Node) HintCount() int {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	n := 0
	for _, m := range nd.hints {
		n += len(m)
	}
	return n
}
