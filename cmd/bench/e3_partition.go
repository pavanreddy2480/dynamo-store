// This file implements experiment E3: behaviour under a network partition.
// The five-node cluster is split into a 2-node minority and a 3-node
// majority, each with its own clients doing reads and read-modify-writes on
// shared keys. The run measures per-side availability, latency and stale
// reads, then heals the partition and checks whether replicas converge and
// whether any acknowledged write was lost, with strict or sloppy quorums.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// sideStats holds per-side results of a partition run (E3).
type sideStats struct {
	a, b             *stats // operation stats for side A (minority) and side B (majority)
	keysWithSibs     int    // after healing and reconciliation-free read
	converged        bool   // every replica of every key identical after heal
	lostAfterResolve int    // acked writes missing after union reconciliation
}

// partitioned runs experiment E3 for one configuration: 5 nodes split
// {n1,n2} | {n3,n4,n5} with clients on both sides doing reads and
// read-modify-writes on shared keys.
//
// Six clients per side run for the -duration flag's worth of time over 64
// keys. After they stop, the partition is healed, hinted handoff is allowed
// to drain, one anti-entropy round runs, and every key is inspected for
// replica convergence, leftover siblings and lost acknowledged writes.
//
// Parameters:
//   - q: the read and write quorum used by every client.
//   - sloppy: whether clients use sloppy quorums with hinted handoff
//     (fallback nodes stand in for unreachable preferred replicas).
//
// Returns:
//   - sideStats: per-side operation stats plus the post-heal convergence,
//     sibling and lost-write results.
func partitioned(q rw, sloppy bool) sideStats {
	c := store.NewCluster(store.DefaultConfig())
	defer c.Close()
	const keys, perSide = 64, 6
	or := newOracle()
	res := sideStats{a: &stats{}, b: &stats{}}
	sideA, sideB := []string{"n1", "n2"}, []string{"n3", "n4", "n5"}
	var clientsA, clientsB []string
	for i := 0; i < perSide; i++ {
		clientsA = append(clientsA, fmt.Sprintf("a%d", i))
		clientsB = append(clientsB, fmt.Sprintf("b%d", i))
	}
	var stop atomic.Bool
	var wg sync.WaitGroup

	// run is one client's workload: read a random key, and on half of the
	// iterations follow up with a read-modify-write that adds the client's ID
	// to the key's set. Stale reads are judged against the shared oracle, so
	// a minority-side read that misses a majority-side write counts as stale.
	run := func(cl *store.Client, st *stats, seed int64) {
		defer wg.Done()
		id := cl.ID
		rng := rand.New(rand.NewSource(seed))
		cl.Sloppy = sloppy
		for !stop.Load() {
			key := fmt.Sprintf("k%d", rng.Intn(keys))
			before := or.snapshot(key)
			t0 := time.Now()
			g, err := cl.Get(key)
			st.get(time.Since(t0), err == nil, stale(before, g.Versions), len(g.Versions))
			if rng.Intn(2) == 0 {
				continue
			}
			ctx := g.Context // nil after a failed read: a blind write
			t0 = time.Now()
			v, err := cl.Put(key, store.UnionResolver(append(g.Values(), id)), ctx)
			st.put(time.Since(t0), err == nil)
			if err == nil {
				or.ack(key, v)
			}
		}
	}

	// Clients must be registered on the network before it is partitioned so
	// that Partition can place each client endpoint on its side.
	var ca, cb []*store.Client
	for i := range clientsA { // register before partitioning
		ca = append(ca, c.NewClient(clientsA[i], q.R, q.W))
		cb = append(cb, c.NewClient(clientsB[i], q.R, q.W))
	}
	// Side A = minority nodes plus A clients; side B = majority nodes plus B
	// clients. The slices are copied so sideA/sideB are not aliased.
	c.Net.Partition(append(append([]string{}, sideA...), clientsA...), append(append([]string{}, sideB...), clientsB...))
	// Seeds differ per side (0.. vs 100..) so the two sides pick
	// independent key sequences while remaining reproducible.
	for i := 0; i < perSide; i++ {
		wg.Add(2)
		go run(ca[i], res.a, int64(i))
		go run(cb[i], res.b, int64(100+i))
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()

	// Reconnect: hinted handoff + one anti-entropy round, then inspect.
	time.Sleep(50 * time.Millisecond)
	c.Net.Heal()
	c.WaitHintsDrained(time.Second)
	c.AntiEntropy()
	res.converged = true
	reader := c.NewClient("reader", 3, 3)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		// Convergence: compare each preferred replica's raw stored state with
		// the first one's; any difference means the replicas disagree.
		var first string
		for i, id := range c.PreferenceList(key) {
			snap := fmt.Sprint(c.Nodes[id].Snapshot(key))
			if i == 0 {
				first = snap
			} else if snap != first {
				res.converged = false
			}
		}
		// A failed R=3 read after healing also means the key is not in a
		// settled state, so it counts against convergence.
		g, err := reader.Get(key)
		if err != nil {
			res.converged = false
			continue
		}
		if len(g.Versions) > 1 {
			res.keysWithSibs++
		}
		// Every write acknowledged on either side must still be visible.
		for _, a := range or.snapshot(key) {
			seen := false
			for _, v := range g.Versions {
				seen = seen || v.Covers(a)
			}
			if !seen {
				res.lostAfterResolve++
			}
		}
	}
	return res
}
