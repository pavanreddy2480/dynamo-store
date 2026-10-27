// This file holds the background repair mechanisms of a Node that bring
// replicas back in line after failures: hinted handoff (delivering writes a
// fallback node held for an unreachable owner) and anti-entropy (a
// full-state exchange between two replicas). Read repair is started by
// coordGet in coordinator.go.

package store

import "context"

// handoffHints delivers versions held for other nodes once they are back.
// A hint is removed only after its owner acknowledged it, and only the
// versions that were actually sent: a newer hint for the same key that
// arrived meanwhile stays for the next round.
//
// It is called periodically by the loop started in start. It takes nd.mu
// only to copy and later prune hints, never while sending.
func (nd *Node) handoffHints() {
	// Copy the hints for every owner that looks reachable, so the lock is
	// not held across network round trips.
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
		// Prune only when the owner acknowledged every key. A partial
		// failure keeps all of this owner's hints; resending the delivered
		// ones next round is harmless because merge is idempotent.
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
		for _, p := range nd.ring.PreferenceList(k, nd.cfg.N) {
			if p == peer {
				out[k] = cloneVersions(vs)
				break
			}
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
		nd.data[k] = nd.rules().merge(nd.data[k], cloneVersions(vs)...)
	}
}

// handleSync answers an anti-entropy request: take the peer's items, then
// send back everything this node owns that the peer should also hold.
//
// Absorbing first means the reply already includes anything the peer sent,
// so both sides end the exchange with the same versions for shared keys.
//
// Parameters:
//   - from: the id of the peer that started the exchange.
//   - r: the peer's half of the exchange.
//
// Returns:
//   - syncResp: this node's items for every owned key that from should
//     also hold.
func (nd *Node) handleSync(from string, r syncReq) syncResp {
	nd.absorb(r.Items)
	return syncResp{Items: nd.itemsFor(from)}
}

// syncWith runs one anti-entropy exchange with peer (a full-state exchange
// standing in for Dynamo's Merkle-tree comparison).
//
// It blocks for at most ReplicaTimeout.
//
// Parameters:
//   - peer: the node to synchronise with.
//
// Returns:
//   - error: nil on success; ErrTimeout if the request or reply was lost
//     or late, in which case this node absorbed nothing (the peer may
//     still have absorbed this node's items).
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
