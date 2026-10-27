// This file implements experiment E2: concurrent read-modify-write on a
// single hot key. Several clients repeatedly read the key's set value, add
// a unique item and write it back with the read's context. Afterwards the
// cluster is reconciled and the final value is checked for every item whose
// write was acknowledged, which shows whether a conflict policy and clock
// scheme silently lose concurrent updates.

package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// concResult summarises one concurrent-update run (E2).
type concResult struct {
	// st holds latency and sibling counts for every read and write in the run.
	st *stats
	// ackedItems is the number of items whose write was acknowledged.
	ackedItems int
	// lostItems is how many acknowledged items are missing from the final,
	// reconciled value.
	lostItems int
	// finalVers is the number of versions (siblings) the final read returned.
	finalVers int
}

// concurrentUpdates runs experiment E2 for one configuration: several
// clients do read-modify-write on ONE key at the same time, each adding
// unique items to a set.
//
// Eight writers run for the -duration flag's worth of time. After they stop
// the cluster is given a moment to settle, one anti-entropy round is run and
// an R=3 W=3 client reads the key to determine which acknowledged items
// survived.
//
// Parameters:
//   - q: the read and write quorum used by the writers.
//   - policy: how replicas resolve concurrent versions (keep siblings or
//     last-writer-wins).
//   - clocks: how versions are compared (dotted version vectors or per-node
//     vector clocks).
//
// Returns:
//   - concResult: operation stats plus acknowledged/lost item counts and the
//     number of versions left after reconciliation.
func concurrentUpdates(q rw, policy store.Policy, clocks store.ClockMode) concResult {
	cfg := store.DefaultConfig()
	cfg.Node.Policy = policy
	cfg.Node.Clocks = clocks
	c := store.NewCluster(cfg)
	defer c.Close()
	const key, writers = "hot", 8
	st := &stats{}
	// mu guards acked, the list of items whose write was acknowledged.
	var mu sync.Mutex
	var acked []string
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("w%d", w), q.R, q.W)
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				res, err := cl.Get(key)
				// Staleness is not tracked here (always false): with many
				// concurrent writers the interesting metric is lost items,
				// not whether one read saw the very latest write.
				st.get(time.Since(t0), err == nil, false, len(res.Versions))
				if err != nil {
					continue
				}
				// Merge all siblings the read returned plus a new unique item,
				// and write back with the read's context so the new version
				// supersedes everything this client saw.
				item := fmt.Sprintf("w%d.%d", w, i)
				val := store.UnionResolver(append(res.Values(), item))
				t0 = time.Now()
				_, err = cl.Put(key, val, res.Context)
				st.put(time.Since(t0), err == nil)
				if err == nil {
					mu.Lock()
					acked = append(acked, item)
					mu.Unlock()
				}
			}
		}(w)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()

	// Let in-flight replication finish, then synchronise all replicas so the
	// final read reflects everything the cluster still holds.
	time.Sleep(100 * time.Millisecond)
	c.AntiEntropy()
	reader := c.NewClient("final", 3, 3)
	res, _ := reader.Get(key)
	// Union the remaining siblings the same way a client would reconcile
	// them, then count acknowledged items that did not survive.
	final := store.UnionResolver(res.Values())
	have := map[string]bool{}
	for _, it := range strings.Split(final, ",") {
		have[it] = true
	}
	lost := 0
	for _, it := range acked {
		if !have[it] {
			lost++
		}
	}
	return concResult{st: st, ackedItems: len(acked), lostItems: lost, finalVers: len(res.Versions)}
}
