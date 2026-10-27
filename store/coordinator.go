// This file holds the coordinator side of a Node: choosing where a request
// for a key goes (strict preference list or sloppy-quorum fallbacks), and
// coordinating client writes (coordPut) and reads (coordGet) by fanning them
// out to the replicas and counting quorum replies. Read repair is started
// from coordGet once all replicas have answered.

package store

import (
	"context"
	"fmt"
	"time"
)

// target is one destination of a coordinated request: the node to contact
// and, for a sloppy-quorum fallback, the preferred node it stands in for.
type target struct {
	node string // node the request is sent to
	hint string // preferred node this fallback stands in for; "" if node is preferred
}

// targets returns where a request for key goes. Strict quorums always use
// the N-node preference list. Sloppy quorums replace preferred nodes this
// coordinator believes are down with the next live nodes on the ring and
// tag the write with a hint naming the intended owner.
//
// Parameters:
//   - key: the key the request is for.
//   - sloppy: whether unreachable preferred replicas may be replaced by
//     fallback nodes.
//
// Returns:
//   - []target: exactly N destinations, in preference-list order.
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
	// Fallbacks are consumed in ring order and each is used at most once,
	// so two unreachable preferred nodes never map onto the same stand-in.
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
			// No live fallback left: keep the preferred node. The send will
			// time out and count as a failed reply, so the target list
			// always has N entries.
			out = append(out, target{node: p})
		}
	}
	return out
}

// coordPut coordinates a client write: it creates the new version, stores
// it locally, replicates to the other targets and returns once W of them
// (counting itself) have acknowledged, or once every target has answered or
// timed out.
//
// It starts one goroutine per remote target plus one that cancels the
// replication context when it expires; replication to slower replicas
// continues in the background after coordPut returns.
//
// Parameters:
//   - r: the client's write request (key, value, causal context, W and
//     whether sloppy quorums are allowed).
//
// Returns:
//   - coordPutResp: the new version and the number of acknowledgements;
//     Err wraps ErrUnavailable if fewer than W targets acknowledged. The
//     version is stored on this node (as data or as a hint) even then.
func (nd *Node) coordPut(r coordPutReq) coordPutResp {
	ts := nd.targets(r.Key, r.Sloppy)
	// selfIdx is this node's position in the target list when it is a
	// preferred replica for the key; it stays -1 when the client picked it
	// as a sloppy fallback coordinator.
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
	//
	// The new dot's counter must be higher than any counter this node has
	// used for the key before, including ones it learned about only through
	// the client's context or through versions replicated back to it.
	// Reusing a counter would make two different writes share a dot, and one
	// would silently replace the other.
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
	// Buffered for every target so replication goroutines never block on a
	// coordinator that has already returned.
	acks := make(chan bool, len(ts))
	for i, t := range ts {
		if i == selfIdx {
			// The local write above is this replica's acknowledgement.
			acks <- true
			continue
		}
		go func(t target) {
			_, err := nd.net.Send(rctx, nd.ID, t.node, putReq{Key: r.Key, V: snapshot, Hint: t.hint})
			acks <- err == nil
		}(t)
	}
	// Count acks until W succeed or every target has answered. Failures
	// (timeouts, partitions) count towards done but not ok.
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

// coordGet coordinates a client read: it asks every target, merges the
// first R successful replies into one sibling set and returns it. Replies
// that arrive later are still collected so read repair can use them.
//
// It starts one goroutine per target, plus a background goroutine that
// waits for the remaining replies and, with ReadRepair enabled, sends the
// merged state to every replica that answered with something older.
//
// Parameters:
//   - r: the client's read request (key, R and whether sloppy quorums are
//     allowed).
//
// Returns:
//   - coordGetResp: the merged sibling set from the first R successful
//     replies and how many replies were merged; Err wraps ErrUnavailable if
//     fewer than R targets replied in time.
func (nd *Node) coordGet(r coordGetReq) coordGetResp {
	ts := nd.targets(r.Key, r.Sloppy)
	ctx, cancel := context.WithTimeout(context.Background(), nd.cfg.ReplicaTimeout)
	// reply is one target's answer; vs is nil when err is set.
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
	// Stop as soon as R successful replies are in: the client does not wait
	// for the slowest replicas. Failed replies count towards done only.
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
	// Clone before handing the result out: the read-repair goroutine below
	// keeps merging into merged after this function returns.
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
			// Send only what this replica lacks; a replica that already
			// holds every merged version (or something newer) gets nothing.
			var missing []Version
			for _, v := range merged {
				if !nd.rules().covers(rp.vs, v) {
					missing = append(missing, v)
				}
			}
			if len(missing) > 0 {
				// Keep the hint so a fallback that answered for an absent
				// owner stores the repair as a hint, not as owned data.
				go nd.net.Send(rctx, nd.ID, rp.t.node, putReq{Key: r.Key, V: missing, Hint: rp.t.hint})
			}
		}
		// Hold rctx open until it expires so the fire-and-forget repair
		// sends above are not cancelled early.
		<-rctx.Done()
	}()
	return resp
}
