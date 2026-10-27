// This file implements experiment A4, which justifies routing each request to
// a randomly chosen preferred replica instead of always the first reachable
// one. It runs a mixed read/write workload under both policies and reports
// the stale-read rate and how evenly coordination work is spread across the
// five nodes.

package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// coordinatorChoice runs experiment A4 and appends its table to the report.
//
// For each policy it starts a fresh default cluster with 32 single-writer
// keys and 16 readers, all at R=1 W=1, for the -duration flag. A read is
// stale when none of the versions it returns covers the latest version
// acknowledged to the key's writer before the read started. Load share is
// each node's fraction of all requests it coordinated.
func coordinatorChoice() {
	w("\n## A4. Choosing the coordinator\n\n")
	w("Same workload as E1 (32 keys, one writer each, 16 readers), R=1 W=1, for %v. Load share is each node's fraction of coordinated requests; perfect balance is 20%%.\n\n", *dur)
	w("| coordinator | stale reads | coordinator load share n1..n5 | busiest node |\n|---|---|---|---|\n")
	for _, first := range []bool{true, false} {
		c := store.NewCluster(store.DefaultConfig())
		const keys = 32
		// last[k] is the most recent acknowledged version of key k, published
		// by its writer and read by readers to detect stale results.
		last := make([]atomic.Pointer[store.Version], keys)
		var stop atomic.Bool
		var wg sync.WaitGroup
		var reads, stale atomic.Int64
		// Writers: one per key, so each key has a single linear history and
		// "latest acknowledged" is well defined.
		for k := 0; k < keys; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				cl := c.NewClient(fmt.Sprintf("w%d", k), 1, 1)
				cl.FirstCoordinator = first
				var ctx map[string]uint64
				for !stop.Load() {
					v, err := cl.Put(fmt.Sprintf("k%d", k), "x", ctx)
					if err == nil {
						// Chain each write on the previous one so the writer
						// never creates siblings of its own.
						ctx = v.Clock()
						last[k].Store(&v)
					}
				}
			}(k)
		}
		// Readers: pick random keys and check the result against the
		// writer's last acknowledged version observed before the read.
		for r := 0; r < 16; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(r)))
				cl := c.NewClient(fmt.Sprintf("r%d", r), 1, 1)
				cl.FirstCoordinator = first
				for !stop.Load() {
					k := rng.Intn(keys)
					// Snapshot before the read: a write acknowledged after
					// this point is not required to be visible.
					before := last[k].Load()
					g, err := cl.Get(fmt.Sprintf("k%d", k))
					if err != nil {
						continue
					}
					reads.Add(1)
					if before != nil {
						seen := false
						for _, v := range g.Versions {
							seen = seen || v.Covers(*before)
						}
						if !seen {
							stale.Add(1)
						}
					}
				}
			}(r)
		}
		time.Sleep(*dur)
		stop.Store(true)
		wg.Wait()
		// Load share: each node's coordinated-request counter divided by the
		// cluster-wide total, so the shares sum to 100%.
		var total int64
		for _, id := range c.IDs {
			total += c.Nodes[id].Coordinated()
		}
		var shares []string
		busiest := 0.0
		for _, id := range c.IDs {
			s := float64(c.Nodes[id].Coordinated()) / float64(total)
			busiest = math.Max(busiest, s)
			shares = append(shares, fmt.Sprintf("%.0f%%", 100*s))
		}
		c.Close()
		name := "random preferred replica (default)"
		if first {
			name = "always first preferred replica"
		}
		w("| %s | %.1f%% | %s | %.0f%% |\n", name, 100*float64(stale.Load())/float64(reads.Load()), strings.Join(shares, " / "), 100*busiest)
	}
}
