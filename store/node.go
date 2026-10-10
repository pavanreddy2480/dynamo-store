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
	putResp struct{ OK bool }

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
		Err     string // non-empty when the quorum was not reached
	}

	coordGetReq struct {
		Key    string
		R      int
		Sloppy bool
	}
	coordGetResp struct {
		Versions []Version
		Replies  int
		Err      string
	}

	syncReq  struct{ Items map[string][]Version }
	syncResp struct{ Items map[string][]Version }

	// Liveness probe used by the TCP failure detector.
	pingReq  struct{ Seq int }
	pingResp struct{ Seq int }

	// Admin: run anti-entropy with every reachable peer now.
	syncAllReq  struct{ Rounds int }
	syncAllResp struct{ Peers int }

	// Admin: return this node's full state (owned data and hints).
	dumpReq  struct{ Keys bool }
	dumpResp struct {
		Data      map[string][]Version
		HintCount int
		Recovered int // WAL records replayed at startup
	}
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
	// EntropyInterval runs anti-entropy with all reachable peers
	// periodically (0 = only when triggered).
	EntropyInterval time.Duration
}

// Node is one storage replica. Any node can coordinate a request.
type Node struct {
	ID   string
	cfg  NodeConfig
	net  Transport
	ring *Ring
	wal  *WAL

	recovered int // WAL records replayed when the node started

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

// NewNode creates a replica. If wal is non-nil the node first rebuilds its
// state by replaying the log, then rewrites the log as a compact checkpoint.
// The caller attaches Handle to its transport and calls Start.
func NewNode(id string, cfg NodeConfig, tr Transport, ring *Ring, wal *WAL) (*Node, error) {
	nd := &Node{
		ID: id, cfg: cfg, net: tr, ring: ring, wal: wal,
		data:   map[string][]Version{},
		hints:  map[string]map[string][]Version{},
		issued: map[string]uint64{},
		stop:   make(chan struct{}),
	}
	if wal != nil && wal.mode != SyncNone {
		n, err := wal.replay(nd.apply)
		if err != nil {
			return nil, err
		}
		nd.recovered = n
		if err := wal.rewrite(nd.checkpoint()); err != nil {
			return nil, err
		}
	}
	return nd, nil
}

// Recovered returns how many WAL records were replayed at startup.
func (nd *Node) Recovered() int { return nd.recovered }

// apply replays one WAL record into memory.
func (nd *Node) apply(r walRecord) {
	switch r.Kind {
	case recData:
		nd.data[r.Key] = nd.rules().merge(nd.data[r.Key], r.V...)
	case recHint:
		if nd.hints[r.Owner] == nil {
			nd.hints[r.Owner] = map[string][]Version{}
		}
		nd.hints[r.Owner][r.Key] = nd.rules().merge(nd.hints[r.Owner][r.Key], r.V...)
	case recHintDone:
		nd.dropDelivered(r.Owner, r.Key, r.V)
	case recIssued:
		if r.Issued > nd.issued[r.Key] {
			nd.issued[r.Key] = r.Issued
		}
	}
}

// checkpoint returns records that recreate the current state.
func (nd *Node) checkpoint() []walRecord {
	var out []walRecord
	for k, vs := range nd.data {
		out = append(out, walRecord{Kind: recData, Key: k, V: vs})
	}
	for owner, m := range nd.hints {
		for k, vs := range m {
			out = append(out, walRecord{Kind: recHint, Owner: owner, Key: k, V: vs})
		}
	}
	for k, n := range nd.issued {
		out = append(out, walRecord{Kind: recIssued, Key: k, Issued: n})
	}
	return out
}

// Start launches background work: hinted handoff and periodic anti-entropy.
func (nd *Node) Start() {
	nd.every(nd.cfg.HandoffInterval, nd.handoffHints)
	nd.every(nd.cfg.EntropyInterval, func() { nd.syncAll() })
}

func (nd *Node) every(d time.Duration, f func()) {
	if d <= 0 {
		return
	}
	nd.wg.Add(1)
	go func() {
		defer nd.wg.Done()
		t := time.NewTicker(d)
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

// syncAll runs anti-entropy with every peer this node can reach.
func (nd *Node) syncAll() int {
	n := 0
	for _, p := range nd.ring.nodes {
		if p != nd.ID && nd.net.Reachable(nd.ID, p) && nd.syncWith(p) == nil {
			n++
		}
	}
	return n
}

// Close stops background work and closes the WAL cleanly.
func (nd *Node) Close() {
	close(nd.stop)
	nd.wg.Wait()
	nd.wal.Close()
}

// crash stops the node like kill -9: background work stops and anything
// not yet written to the WAL file is lost.
func (nd *Node) crash() {
	close(nd.stop)
	nd.wg.Wait()
	nd.wal.abandon()
}

func (nd *Node) rules() rules { return rules{policy: nd.cfg.Policy, clocks: nd.cfg.Clocks} }

// Handle processes one request; transports call it for every delivery.
func (nd *Node) Handle(from string, req any) any {
	switch r := req.(type) {
	case putReq:
		var rec walRecord
		nd.mu.Lock()
		if r.Hint != "" && r.Hint != nd.ID {
			m := nd.hints[r.Hint]
			if m == nil {
				m = map[string][]Version{}
				nd.hints[r.Hint] = m
			}
			m[r.Key] = nd.rules().merge(m[r.Key], cloneVersions(r.V)...)
			rec = walRecord{Kind: recHint, Owner: r.Hint, Key: r.Key, V: r.V}
		} else {
			nd.data[r.Key] = nd.rules().merge(nd.data[r.Key], cloneVersions(r.V)...)
			rec = walRecord{Kind: recData, Key: r.Key, V: r.V}
		}
		nd.mu.Unlock()
		// Ack only after the write is in the log (per the WAL's sync mode).
		if err := nd.wal.append(rec); err != nil {
			return nil
		}
		return putResp{OK: true}
	case pingReq:
		return pingResp{}
	case syncAllReq:
		return syncAllResp{Peers: nd.syncAll()}
	case dumpReq:
		nd.mu.Lock()
		d := dumpResp{Data: map[string][]Version{}, Recovered: nd.recovered}
		for k, vs := range nd.data {
			d.Data[k] = cloneVersions(vs)
		}
		for _, m := range nd.hints {
			d.HintCount += len(m)
		}
		nd.mu.Unlock()
		return d
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
	localRecs := []walRecord{{Kind: recIssued, Key: r.Key, Issued: dot.Counter}}
	if selfIdx >= 0 {
		nd.data[r.Key] = nd.rules().merge(nd.data[r.Key], v)
		localRecs = append(localRecs, walRecord{Kind: recData, Key: r.Key, V: []Version{v}})
	} else {
		// Not a replica for this key (sloppy fallback): keep the write as a
		// hint for the first preferred node so it is never lost here.
		owner := nd.ring.PreferenceList(r.Key, nd.cfg.N)[0]
		if nd.hints[owner] == nil {
			nd.hints[owner] = map[string][]Version{}
		}
		nd.hints[owner][r.Key] = nd.rules().merge(nd.hints[owner][r.Key], v)
		localRecs = append(localRecs, walRecord{Kind: recHint, Owner: owner, Key: r.Key, V: []Version{v}})
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
	// The local write is logged in parallel with replication; it counts as
	// this node's ack once it is durable (per the WAL sync mode).
	if selfIdx >= 0 {
		go func() { acks <- nd.wal.append(localRecs...) == nil }()
	} else {
		nd.wal.append(localRecs...) // coordinator only holds a hint; not an ack
	}
	for i, t := range ts {
		if i == selfIdx {
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
		resp.Err = fmt.Sprintf("put %d/%d acks", ok, r.W)
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
		resp.Err = fmt.Sprintf("get %d/%d replies", len(got), r.R)
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
			var recs []walRecord
			nd.mu.Lock()
			for k, vs := range m {
				nd.dropDelivered(owner, k, vs)
				recs = append(recs, walRecord{Kind: recHintDone, Owner: owner, Key: k, V: vs})
			}
			nd.mu.Unlock()
			nd.wal.append(recs...)
		}
	}
}

// dropDelivered removes hints for owner/key that delivered covers; newer
// hints that arrived meanwhile are kept. Caller holds nd.mu (or is replaying).
func (nd *Node) dropDelivered(owner, key string, delivered []Version) {
	cur, ok := nd.hints[owner][key]
	if !ok {
		return
	}
	rest := cur[:0:0]
	for _, c := range cur {
		if !nd.rules().covers(delivered, c) {
			rest = append(rest, c)
		}
	}
	if len(rest) == 0 {
		delete(nd.hints[owner], key)
	} else {
		nd.hints[owner][key] = rest
	}
	if len(nd.hints[owner]) == 0 {
		delete(nd.hints, owner)
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
	var recs []walRecord
	nd.mu.Lock()
	for k, vs := range items {
		before := nd.data[k]
		nd.data[k] = nd.rules().merge(nd.data[k], cloneVersions(vs)...)
		if fmt.Sprint(before) != fmt.Sprint(nd.data[k]) { // log only real changes
			recs = append(recs, walRecord{Kind: recData, Key: k, V: vs})
		}
	}
	nd.mu.Unlock()
	nd.wal.append(recs...)
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
