// This file implements experiment A3, which justifies the default choice of
// repair mechanisms (hinted handoff plus periodic anti-entropy). It writes
// through a network partition using sloppy quorums, heals the partition,
// applies one combination of hinted handoff, read repair and anti-entropy,
// and then measures how many acknowledged writes are still missing from a
// preferred replica and how many keys still have disagreeing replicas.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// repairMode selects which repair mechanisms run after the partition heals.
type repairMode struct {
	name    string // label printed in the report's first column
	handoff bool   // keep hinted handoff enabled and wait for hints to drain
	rr      bool   // enable read repair and read every key once at R=3
	entropy bool   // run one anti-entropy round across all reachable pairs
}

// convergence runs one A3 trial with the given repair mechanisms.
//
// Eight sloppy-quorum clients (R=1, W=2), four on each side of a
// {n1,n2} | {n3,n4,n5} partition, read-modify-write 64 keys for half of the
// -duration flag. The partition is then healed and only the repair steps
// enabled in m are applied, in the order hinted handoff, read repair,
// anti-entropy. Finally every key's three preferred replicas are inspected
// directly through Node.Snapshot.
//
// Parameters:
//   - m: which repair mechanisms to enable after the heal.
//
// Returns:
//   - acked: the number of acknowledged writes across all keys.
//   - under: acknowledged writes not covered by at least one of the key's
//     three preferred replicas (under-replicated).
//   - divergent: keys whose three preferred replicas hold different version
//     sets.
func convergence(m repairMode) (acked, under int, divergent int) {
	cfg := store.DefaultConfig()
	// Hinted handoff is a background loop; a zero interval disables it so
	// hints stay parked on the fallback nodes.
	if !m.handoff {
		cfg.Node.HandoffInterval = 0
	}
	cfg.Node.ReadRepair = m.rr
	c := store.NewCluster(cfg)
	defer c.Close()
	const keys = 64
	var mu sync.Mutex
	// versions records, per key, every version whose Put was acknowledged.
	var versions = map[string][]store.Version{}
	var clients []*store.Client
	var sideA, sideB []string
	for i := 0; i < 4; i++ {
		a := c.NewClient(fmt.Sprintf("a%d", i), 1, 2)
		b := c.NewClient(fmt.Sprintf("b%d", i), 1, 2)
		a.Sloppy, b.Sloppy = true, true
		clients = append(clients, a, b)
		sideA, sideB = append(sideA, a.ID), append(sideB, b.ID)
	}
	// Clients are placed in the partition alongside their side's nodes, so
	// each can only reach the replicas on its own side.
	c.Net.Partition(append([]string{"n1", "n2"}, sideA...), append([]string{"n3", "n4", "n5"}, sideB...))
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				// The Get error is ignored on purpose: a failed read still
				// returns whatever versions it gathered (possibly none), and
				// the client writes on that context rather than skipping, so
				// both partition sides keep producing concurrent writes.
				g, _ := cl.Get(key)
				v, err := cl.Put(key, store.UnionResolver(append(g.Values(), cl.ID)), g.Context)
				if err == nil {
					mu.Lock()
					versions[key] = append(versions[key], v)
					mu.Unlock()
				}
			}
		}(cl, int64(i))
	}
	time.Sleep(*dur / 2)
	stop.Store(true)
	wg.Wait()
	// Let in-flight replica messages settle before the network changes.
	time.Sleep(50 * time.Millisecond)
	c.Net.Heal()
	// Repair step 1: hinted handoff. Fallback nodes push parked writes back
	// to their intended owners; wait (bounded) until all hints are delivered.
	if m.handoff {
		c.WaitHintsDrained(time.Second)
	}
	// Repair step 2: read repair. One R=3 read per key lets the coordinator
	// see every replica's versions and push the missing ones to stale
	// replicas asynchronously.
	if m.rr {
		reader := c.NewClient("reader", 3, 3)
		for k := 0; k < keys; k++ {
			reader.Get(fmt.Sprintf("k%d", k))
		}
		time.Sleep(100 * time.Millisecond) // let repairs land
	}
	// Repair step 3: a single anti-entropy round between all node pairs.
	if m.entropy {
		c.AntiEntropy()
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		var first string
		div := false
		snaps := map[string][]store.Version{}
		// A key is divergent when any preferred replica's version set prints
		// differently from the first replica's.
		for i, id := range c.PreferenceList(key) {
			snaps[id] = c.Nodes[id].Snapshot(key)
			s := fmt.Sprint(snaps[id])
			if i == 0 {
				first = s
			} else if s != first {
				div = true
			}
		}
		if div {
			divergent++
		}
		// An acknowledged write is under-replicated if some preferred replica
		// has no version that covers it (neither holds it nor a successor
		// that saw it). Each write is counted at most once.
		for _, a := range versions[key] {
			acked++
			for _, snap := range snaps {
				ok := false
				for _, s := range snap {
					ok = ok || s.Covers(a)
				}
				if !ok {
					under++
					break
				}
			}
		}
	}
	return
}

// repair runs experiment A3 and appends its table to the report.
//
// It runs convergence once for each repair combination (none, each mechanism
// alone, and the default of hinted handoff plus anti-entropy) and reports
// acknowledged writes, under-replicated writes and divergent keys.
func repair() {
	w("\n## A3. Repair mechanisms after a partition heals\n\n")
	w("Sloppy R=1 W=2 writes on both sides of a {n1,n2} | {n3,n4,n5} partition for %v, then heal. \"Under-replicated\" means an acknowledged write not yet on all 3 preferred replicas. \"Divergent\" means the 3 replicas of a key disagree.\n\n", *dur/2)
	w("| repair after heal | acked writes | under-replicated | divergent keys (of 64) |\n|---|---|---|---|\n")
	for _, m := range []repairMode{
		{"none", false, false, false},
		{"hinted handoff only", true, false, false},
		{"read repair only (one R=3 read per key)", false, true, false},
		{"anti-entropy only (one round)", false, false, true},
		{"hinted handoff + anti-entropy (default)", true, false, true},
	} {
		a, u, d := convergence(m)
		w("| %s | %d | %d (%.1f%%) | %d |\n", m.name, a, u, 100*float64(u)/float64(a), d)
	}
}
