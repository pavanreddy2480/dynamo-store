package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/vclock"
)

// ErrUnavailable means fewer than R (or W) replicas answered in time.
var ErrUnavailable = errors.New("quorum not reached")

// Messages exchanged between clients and replicas.
type (
	putReq struct {
		Key  string
		V    []Version
		Hint string // intended owner when stored on a fallback node
	}
	putResp struct{}

	getReq  struct{ Key string }
	getResp struct{ Versions []Version }

	coordPutReq struct {
		Key     string
		Value   string
		Context vclock.VC
		W       int
		Sloppy  bool
	}
	coordPutResp struct {
		Version Version
		Acks    int
		Err     error
	}

	coordGetReq struct {
		Key    string
		R      int
		Sloppy bool
	}
	coordGetResp struct {
		Versions []Version
		Replies  int
		Err      error
	}

	syncReq  struct{ Items map[string][]Version }
	syncResp struct{ Items map[string][]Version }
)

// NodeConfig configures a replica.
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
}

// Node is one storage replica. Any node can coordinate a request.
type Node struct {
	ID   string
	cfg  NodeConfig
	net  *Network
	ring *Ring

	mu     sync.Mutex
	data   map[string][]Version
	hints  map[string]map[string][]Version // intended owner -> key -> versions
	issued map[string]uint64               // highest counter this node issued per key

	stop chan struct{}
	wg   sync.WaitGroup

	coordinated atomic.Int64 // client requests this node coordinated
}

// Coordinated returns how many client requests this node coordinated.
func (nd *Node) Coordinated() int64 { return nd.coordinated.Load() }

func newNode(id string, cfg NodeConfig, net *Network, ring *Ring) *Node {
	nd := &Node{
		ID: id, cfg: cfg, net: net, ring: ring,
		data:   map[string][]Version{},
		hints:  map[string]map[string][]Version{},
		issued: map[string]uint64{},
		stop:   make(chan struct{}),
	}
	net.Register(id, nd.handle)
	return nd
}

func (nd *Node) start() {
	if nd.cfg.HandoffInterval <= 0 {
		return
	}
	nd.wg.Add(1)
	go func() {
		defer nd.wg.Done()
		t := time.NewTicker(nd.cfg.HandoffInterval)
		defer t.Stop()
		for {
			select {
			case <-nd.stop:
				return
			case <-t.C:
				nd.handoffHints()
			}
		}
	}()
}

func (nd *Node) rules() rules { return rules{policy: nd.cfg.Policy, clocks: nd.cfg.Clocks} }

func (nd *Node) close() { close(nd.stop); nd.wg.Wait() }

func (nd *Node) handle(from string, req any) any {
	switch r := req.(type) {
	case putReq:
		nd.mu.Lock()
		if r.Hint != "" && r.Hint != nd.ID {
			m := nd.hints[r.Hint]
			if m == nil {
				m = map[string][]Version{}
				nd.hints[r.Hint] = m
			}
			m[r.Key] = nd.rules().merge(m[r.Key], cloneVersions(r.V)...)
		} else {
			nd.data[r.Key] = nd.rules().merge(nd.data[r.Key], cloneVersions(r.V)...)
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
	}
	panic(fmt.Sprintf("unknown request %T", req))
}

// localRead returns owned versions plus any hinted versions held for others.
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

type target struct{ node, hint string }

// targets returns where a request for key goes. Strict quorums always use
// the N-node preference list. Sloppy quorums replace preferred nodes this
// coordinator believes are down with the next live nodes on the ring and
// tag the write with a hint naming the intended owner.
func (nd *Node) targets(key string, sloppy bool) []target {
	walk := nd.ring.Walk(key)
	n := nd.cfg.N
	pref := walk[:n]
	out := make([]target, 0, n)
	if !sloppy {
		for _, p := range pref {
			out = append(out, target{node: p})
		}
		return out
	}
	fallback := walk[n:]
	fi := 0
	for _, p := range pref {
		if nd.net.Reachable(nd.ID, p) {
			out = append(out, target{node: p})
			continue
		}
		for fi < len(fallback) && !nd.net.Reachable(nd.ID, fallback[fi]) {
			fi++
		}
		if fi < len(fallback) {
			out = append(out, target{node: fallback[fi], hint: p})
			fi++
		} else {
			out = append(out, target{node: p})
		}
	}
	return out
}

func (nd *Node) coordPut(r coordPutReq) coordPutResp {
	ts := nd.targets(r.Key, r.Sloppy)
	selfIdx := -1
	for i, t := range ts {
		if t.node == nd.ID && t.hint == "" {
			selfIdx = i
		}
	}

	// Under one lock: pick a fresh counter, apply the write locally and
	// snapshot the local sibling set. Replicating the whole set (as Riak
	// does) rather than just the new version guarantees that any replica
	// holding dot n:k also holds n:k-1 or something that supersedes it, so
	// a context built from a single replica never claims a write it lacks.
	nd.mu.Lock()
	floor := nd.issued[r.Key]
	if c := r.Context[nd.ID]; c > floor {
		floor = c
	}
	local := append([]Version(nil), nd.data[r.Key]...)
	for _, m := range nd.hints {
		local = append(local, m[r.Key]...)
	}
	for _, v := range local {
		if c := v.Clock()[nd.ID]; c > floor {
			floor = c
		}
	}
	nd.issued[r.Key] = floor + 1
	dot := Dot{Node: nd.ID, Counter: floor + 1}
	ctx := r.Context.Copy()
	if nd.cfg.Clocks == PerNodeVC {
		// Plain vector clock: the version's clock is context + own bump.
		ctx[nd.ID] = dot.Counter
	}
	v := Version{Value: r.Value, Dot: dot, Ctx: ctx, TS: time.Now().Add(nd.cfg.ClockSkew).UnixNano(), Origin: nd.ID}
	if selfIdx >= 0 {
		nd.data[r.Key] = nd.rules().merge(nd.data[r.Key], v)
	} else {
		// Not a replica for this key (sloppy fallback): keep the write as a
		// hint for the first preferred node so it is never lost here.
		owner := nd.ring.PreferenceList(r.Key, nd.cfg.N)[0]
		if nd.hints[owner] == nil {
			nd.hints[owner] = map[string][]Version{}
		}
		nd.hints[owner][r.Key] = nd.rules().merge(nd.hints[owner][r.Key], v)
	}
	snapshot := cloneVersions(nd.data[r.Key])
	for _, m := range nd.hints {
		snapshot = nd.rules().merge(snapshot, cloneVersions(m[r.Key])...)
	}
	if nd.cfg.ReplicateNewOnly {
		snapshot = []Version{v}
	}
	nd.mu.Unlock()

	rctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
	acks := make(chan bool, len(ts))
	for i, t := range ts {
		if i == selfIdx {
			acks <- true
			continue
		}
		go func(t target) {
			_, err := nd.net.Send(rctx, nd.ID, t.node, putReq{Key: r.Key, V: snapshot, Hint: t.hint})
			acks <- err == nil
		}(t)
	}
	ok, done := 0, 0
	for done < len(ts) && ok < r.W {
		if <-acks {
			ok++
		}
		done++
	}
	// Remaining replicas keep receiving the write in the background until
	// the timeout fires; that lag is what makes R+W<=N reads stale.
	go func() { <-rctx.Done(); cancel() }()
	resp := coordPutResp{Version: v, Acks: ok}
	if ok < r.W {
		resp.Err = fmt.Errorf("%w: put %d/%d acks", ErrUnavailable, ok, r.W)
	}
	return resp
}

func (nd *Node) coordGet(r coordGetReq) coordGetResp {
	ts := nd.targets(r.Key, r.Sloppy)
	ctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
	type reply struct {
		t   target
		vs  []Version
		err error
	}
	replies := make(chan reply, len(ts))
	for _, t := range ts {
		go func(t target) {
			resp, err := nd.net.Send(ctx, nd.ID, t.node, getReq{Key: r.Key})
			var vs []Version
			if err == nil {
				vs = resp.(getResp).Versions
			}
			replies <- reply{t, vs, err}
		}(t)
	}
	var got []reply
	var merged []Version
	done := 0
	for done < len(ts) && len(got) < r.R {
		rp := <-replies
		done++
		if rp.err == nil {
			got = append(got, rp)
			merged = nd.rules().merge(merged, rp.vs...)
		}
	}
	resp := coordGetResp{Versions: cloneVersions(merged), Replies: len(got)}
	if len(got) < r.R {
		resp.Err = fmt.Errorf("%w: get %d/%d replies", ErrUnavailable, len(got), r.R)
	}

	// Read repair: wait for the stragglers, then push the merged state to
	// every replica that answered with something older.
	go func() {
		defer cancel()
		for ; done < len(ts); done++ {
			rp := <-replies
			if rp.err == nil {
				got = append(got, rp)
				merged = nd.rules().merge(merged, rp.vs...)
			}
		}
		if !nd.cfg.ReadRepair {
			return
		}
		rctx, rcancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
		defer rcancel()
		for _, rp := range got {
			var missing []Version
			for _, v := range merged {
				if !nd.rules().covers(rp.vs, v) {
					missing = append(missing, v)
				}
			}
			if len(missing) > 0 {
				go nd.net.Send(rctx, nd.ID, rp.t.node, putReq{Key: r.Key, V: missing, Hint: rp.t.hint})
			}
		}
		<-rctx.Done()
	}()
	return resp
}

// handoffHints delivers versions held for other nodes once they are back.
func (nd *Node) handoffHints() {
	nd.mu.Lock()
	pending := map[string]map[string][]Version{}
	for owner, m := range nd.hints {
		if nd.net.Reachable(nd.ID, owner) {
			cp := map[string][]Version{}
			for k, vs := range m {
				cp[k] = cloneVersions(vs)
			}
			pending[owner] = cp
		}
	}
	nd.mu.Unlock()
	for owner, m := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
		allOK := true
		for k, vs := range m {
			if _, err := nd.net.Send(ctx, nd.ID, owner, putReq{Key: k, V: vs}); err != nil {
				allOK = false
			}
		}
		cancel()
		if allOK {
			nd.mu.Lock()
			for k, vs := range m {
				// drop only what was delivered; newer hints may have arrived
				if cur, ok := nd.hints[owner][k]; ok {
					rest := cur[:0:0]
					for _, c := range cur {
						if !nd.rules().covers(vs, c) {
							rest = append(rest, c)
						}
					}
					if len(rest) == 0 {
						delete(nd.hints[owner], k)
					} else {
						nd.hints[owner][k] = rest
					}
				}
			}
			if len(nd.hints[owner]) == 0 {
				delete(nd.hints, owner)
			}
			nd.mu.Unlock()
		}
	}
}

// itemsFor returns this node's owned keys whose preference list includes peer.
func (nd *Node) itemsFor(peer string) map[string][]Version {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	out := map[string][]Version{}
	for k, vs := range nd.data {
		for _, p := range nd.ring.PreferenceList(k, nd.cfg.N) {
			if p == peer {
				out[k] = cloneVersions(vs)
				break
			}
		}
	}
	return out
}

func (nd *Node) absorb(items map[string][]Version) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for k, vs := range items {
		nd.data[k] = nd.rules().merge(nd.data[k], cloneVersions(vs)...)
	}
}

func (nd *Node) handleSync(from string, r syncReq) syncResp {
	nd.absorb(r.Items)
	return syncResp{Items: nd.itemsFor(from)}
}

// syncWith runs one anti-entropy exchange with peer (a full-state exchange
// standing in for Dynamo's Merkle-tree comparison).
func (nd *Node) syncWith(peer string) error {
	ctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
	defer cancel()
	resp, err := nd.net.Send(ctx, nd.ID, peer, syncReq{Items: nd.itemsFor(peer)})
	if err != nil {
		return err
	}
	nd.absorb(resp.(syncResp).Items)
	return nil
}

// Snapshot returns the versions this node owns for key (no hints).
func (nd *Node) Snapshot(key string) []Version {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return cloneVersions(nd.data[key])
}

// HintCount returns how many hinted keys this node is holding.
func (nd *Node) HintCount() int {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	n := 0
	for _, m := range nd.hints {
		n += len(m)
	}
	return n
}
