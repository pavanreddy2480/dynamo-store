// This file holds the background repair mechanisms of a Node that bring
// replicas back in line after failures: hinted handoff (delivering writes a
// fallback node held for an unreachable owner) and anti-entropy (comparing
// two replicas' Merkle trees, or their full state, and exchanging what
// differs). Read repair is started by coordGet in coordinator.go.

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
