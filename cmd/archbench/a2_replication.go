// This file implements experiment A2, which justifies two coordinator design
// choices: replicating the whole sibling set (rather than only the newly
// written version) and comparing versions with dotted version vectors (rather
// than plain per-node vector clocks). It drives concurrent read-modify-write
// clients against a single hot key and counts acknowledged updates that are
// missing after a final merge.

package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// rmwResult summarises one read-modify-write run of experiment A2.
type rmwResult struct {
	acked    int // items whose Put was acknowledged to the client
	lost     int // acknowledged items absent from the final union of all siblings
	siblings int // number of sibling versions returned by the final read
}

// rmw runs 8 concurrent read-modify-write clients on one key and counts the
// acknowledged items missing from the final union.
//
// Each client loops for the -duration flag: it reads key "hot", adds a unique
// item to the union of the values it saw, and writes the result back with the
// context from its read. Every successful write is recorded as acknowledged.
// After the clients stop, the cluster runs anti-entropy and an R=3 read
// collects every surviving sibling; an acknowledged item missing from their
// union was silently overwritten, which is exactly what a correct design must
// never do.
//
// Parameters:
//   - cfg: the cluster configuration to test (clock mode, replication
//     payload and seed are varied by the caller).
//   - r: the read quorum R used by the 8 writer clients.
//   - wq: the write quorum W used by the 8 writer clients.
//
// Returns:
//   - rmwResult: acknowledged, lost and final sibling counts for this run.
func rmw(cfg store.Config, r, wq int) rmwResult {
	c := store.NewCluster(cfg)
	defer c.Close()
	var mu sync.Mutex
	var acked []string
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("w%d", i), r, wq)
			for j := 0; !stop.Load(); j++ {
				res, err := cl.Get("hot")
				if err != nil {
					continue
				}
				// The item name is unique per client and iteration, so each
				// one identifies exactly one acknowledged write.
				item := fmt.Sprintf("w%d.%d", i, j)
				if _, err := cl.Put("hot", store.UnionResolver(append(res.Values(), item)), res.Context); err == nil {
					mu.Lock()
					acked = append(acked, item)
					mu.Unlock()
				}
			}
		}(i)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	// Give in-flight replication messages time to arrive, then run one
	// anti-entropy round so every replica holds every surviving version. Any
	// item still missing afterwards was dropped, not merely delayed.
	time.Sleep(100 * time.Millisecond)
	c.AntiEntropy()
	res, _ := c.NewClient("final", 3, 3).Get("hot")
	have := map[string]bool{}
	for _, it := range strings.Split(store.UnionResolver(res.Values()), ",") {
		have[it] = true
	}
	out := rmwResult{acked: len(acked), siblings: len(res.Versions)}
	for _, it := range acked {
		if !have[it] {
			out.lost++
		}
	}
	return out
}

// replication runs experiment A2 and appends its table to the report.
//
// It compares three coordinator designs: dotted version vectors replicating
// the whole sibling set (the default), dotted version vectors replicating
// only the new version, and per-node vector clocks. Each design is run at
// R=W=1 and R=W=2, and every row sums three runs with different seeds so a
// rare loss is not hidden by a lucky schedule.
func replication() {
	w("\n## A2. What a coordinator replicates, and how versions are compared\n\n")
	w("8 clients do concurrent read-modify-write on one key for %v, adding a unique item each time. \"Lost\" counts acknowledged items missing after a final union merge. Each row is the sum over 3 runs.\n\n", *dur)
	w("| clocks | replication payload | R,W | acked items | lost items |\n|---|---|---|---|---|\n")
	for _, v := range []struct {
		clocks  store.ClockMode
		newOnly bool
	}{{store.DottedVV, false}, {store.DottedVV, true}, {store.PerNodeVC, false}} {
		payload := "whole sibling set"
		if v.newOnly {
			payload = "new version only"
		}
		// q holds {R, W}.
		for _, q := range [][2]int{{1, 1}, {2, 2}} {
			var tot rmwResult
			for run := 0; run < 3; run++ {
				cfg := store.DefaultConfig()
				cfg.Seed = int64(run + 1)
				cfg.Node.Clocks = v.clocks
				cfg.Node.ReplicateNewOnly = v.newOnly
				r := rmw(cfg, q[0], q[1])
				tot.acked += r.acked
				tot.lost += r.lost
			}
			w("| %s | %s | %d,%d | %d | %d (%.2f%%) |\n", v.clocks, payload, q[0], q[1], tot.acked, tot.lost, 100*float64(tot.lost)/float64(tot.acked))
		}
	}
}
