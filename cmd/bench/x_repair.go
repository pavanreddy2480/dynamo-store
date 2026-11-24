// This file implements ablation experiments X3 (periodic anti-entropy),
// X6 (hinted-handoff interval) and X7 (what read repair and querying all N
// replicas cost), plus the partition-run helper X3, X6 and X8 share.

package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
)

// partRun is the state after a partitioned workload has stopped, with the
// partition still in place.
type partRun struct {
	c         *store.Cluster
	acked     map[string][]store.Version // acknowledged writes per key
	attempted int                        // operations attempted (reads and writes)
	ok        int                        // operations that succeeded
	keys      int
	mu        sync.Mutex
}

// partitionRun starts a cluster from cfg and runs 12 clients (round-robin
// over the groups) doing read-modify-writes on 64 keys for d. With flap the
// partition is healed and re-applied every 200ms. The partition is left in
// place (healed if flap) for the caller to heal and inspect.
func partitionRun(cfg store.Config, q rw, sloppy bool, groups [][]string, flap bool, d time.Duration) *partRun {
	c := store.NewCluster(cfg)
	pr := &partRun{c: c, acked: map[string][]store.Version{}, keys: 64}
	full := make([][]string, len(groups))
	var clients []*store.Client
	for i := 0; i < 12; i++ {
		cl := c.NewClient(fmt.Sprintf("c%d", i), q.R, q.W)
		cl.Sloppy = sloppy
		clients = append(clients, cl)
		full[i%len(groups)] = append(full[i%len(groups)], cl.ID)
	}
	for g := range full {
		full[g] = append(append([]string{}, groups[g]...), full[g]...)
	}
	c.Net.Partition(full...)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				key := fmt.Sprintf("k%d", rng.Intn(pr.keys))
				g, err := cl.Get(key)
				pr.mu.Lock()
				pr.attempted++
				if err == nil {
					pr.ok++
				}
				pr.mu.Unlock()
				v, err := cl.Put(key, store.UnionResolver(append(g.Values(), cl.ID)), g.Context)
				pr.mu.Lock()
				pr.attempted++
				if err == nil {
					pr.ok++
					pr.acked[key] = append(pr.acked[key], v)
				}
				pr.mu.Unlock()
			}
		}(cl, int64(i))
	}
	if flap {
		for t := time.Now().Add(d); time.Now().Before(t); {
			time.Sleep(200 * time.Millisecond)
			c.Net.Heal()
			time.Sleep(200 * time.Millisecond)
			c.Net.Partition(full...)
		}
	} else {
		time.Sleep(d)
	}
	stop.Store(true)
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	return pr
}

// divergent counts keys whose preferred replicas hold different state.
func divergent(c *store.Cluster, keys int) int {
	n := 0
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		var first string
		for i, id := range c.PreferenceList(key) {
			s := fmt.Sprint(c.Nodes[id].Snapshot(key))
			if i == 0 {
				first = s
			} else if s != first {
				n++
				break
			}
		}
	}
	return n
}

// sloppyPartition is the A3 scenario: two sides, sloppy R=1 W=2.
var sloppyPartition = [][]string{{"n1", "n2"}, {"n3", "n4", "n5"}}

// x3Periodic measures how long a background anti-entropy loop takes to
// repair a healed partition on its own, and what it costs when idle.
func x3Periodic() {
	out("\n## X3. Periodic anti-entropy: how often?\n\n")
	out("Before this ablation nothing started anti-entropy; benchmarks called it by hand. Now each node syncs with one random reachable peer every interval. Scenario: A3's sloppy R=1,W=2 writes on both sides of a {n1,n2} | {n3,n4,n5} partition for 1s, hinted handoff off so only anti-entropy repairs, then heal. Time is from heal until the 3 preferred replicas of all 64 keys agree (capped at 8s). Idle cost is measured for 1s after convergence. Mean (min-max) of %d runs.\n\n", *reps)
	out("| interval | time to converge ms | idle messages/s | idle KB/s |\n|---|---|---|---|\n")
	ivs := []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}
	var labels []string
	var conv, idle []spread
	for _, iv := range ivs {
		var msgs, kb []float64
		cs := repeat(func(seed int64) float64 {
			cfg := store.DefaultConfig()
			cfg.Seed = seed
			cfg.Node.HandoffInterval = 0
			cfg.Node.AntiEntropyInterval = iv
			pr := partitionRun(cfg, rw{1, 2}, true, sloppyPartition, false, time.Second)
			defer pr.c.Close()
			pr.c.Net.Heal()
			t0 := time.Now()
			for divergent(pr.c, pr.keys) > 0 && time.Since(t0) < 8*time.Second {
				time.Sleep(5 * time.Millisecond)
			}
			el := time.Since(t0)
			m0, k0 := pr.c.Net.Sent(), pr.c.SyncStats().Bytes
			time.Sleep(time.Second)
			msgs = append(msgs, float64(pr.c.Net.Sent()-m0))
			kb = append(kb, float64(pr.c.SyncStats().Bytes-k0)/1024)
			return float64(el) / float64(time.Millisecond)
		})
		im, ik := mean(msgs), mean(kb)
		out("| %v | %s | %.0f | %.1f |\n", iv, cs, im, ik)
		labels = append(labels, iv.String())
		conv, idle = append(conv, cs), append(idle, spread{im, im, im})
	}
	m, lo, hi := column(conv)
	chart.BarErr(chartPath("x3_converge.svg"), "X3 time to repair a healed partition by anti-entropy interval", "anti-entropy interval", "ms until all replicas agree", labels, []string{"time to converge"}, [][]float64{m}, [][]float64{lo}, [][]float64{hi}, false)
	im, _, _ := column(idle)
	chart.Bar(chartPath("x3_idle.svg"), "X3 idle cost of periodic anti-entropy (converged cluster)", "anti-entropy interval", "network messages per second", labels, []string{"messages/s"}, [][]float64{im}, false)
	out("\n![converge](charts/x3_converge.svg)\n![idle](charts/x3_idle.svg)\n")
}

// mean returns the average of vs.
func mean(vs []float64) float64 {
	s := 0.0
	for _, v := range vs {
		s += v
	}
	return s / float64(len(vs))
}

// x6Handoff sweeps the hinted-handoff interval.
func x6Handoff() {
	out("\n## X6. Hinted-handoff interval\n\n")
	out("Same partition scenario as X3 (anti-entropy off). After heal, how long until every hint is delivered to its owner. Mean (min-max) of %d runs.\n\n| interval | time to drain hints ms |\n|---|---|\n", *reps)
	var labels []string
	var ss []spread
	for _, iv := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond} {
		s := repeat(func(seed int64) float64 {
			cfg := store.DefaultConfig()
			cfg.Seed = seed
			cfg.Node.HandoffInterval = iv
			pr := partitionRun(cfg, rw{1, 2}, true, sloppyPartition, false, time.Second)
			defer pr.c.Close()
			pr.c.Net.Heal()
			t0 := time.Now()
			pr.c.WaitHintsDrained(10 * time.Second)
			return float64(time.Since(t0)) / float64(time.Millisecond)
		})
		out("| %v | %s |\n", iv, s)
		labels, ss = append(labels, iv.String()), append(ss, s)
	}
	m, lo, hi := column(ss)
	chart.BarErr(chartPath("x6_handoff.svg"), "X6 time to deliver hints after heal by handoff interval", "handoff interval", "ms until no hints remain", labels, []string{"drain time"}, [][]float64{m}, [][]float64{lo}, [][]float64{hi}, false)
	out("\n![handoff](charts/x6_handoff.svg)\n")
}

// x7ReadPath measures the traffic cost of read repair and of querying all N
// replicas on a read.
func x7ReadPath() {
	out("\n## X7. Read path: read repair and query fan-out\n\n")
	out("**Read repair cost.** E1 workload, network messages per client operation, read repair on vs off. Mean (min-max) of %d runs.\n\n| R,W | read repair | messages/op | stale reads %% |\n|---|---|---|---|\n", *reps)
	var groups []string
	var on, off []spread
	for _, q := range []rw{{1, 1}, {2, 2}} {
		for _, rr := range []bool{true, false} {
			w := base(q)
			w.cfg.Node.ReadRepair = rr
			s := measureAll(w, mMsgs, mStale)
			out("| %d,%d | %v | %s | %s |\n", q.R, q.W, rr, s[0], s[1])
			if rr {
				on = append(on, s[0])
			} else {
				off = append(off, s[0])
			}
		}
		groups = append(groups, fmt.Sprintf("%d,%d", q.R, q.W))
	}
	m1, l1, h1 := column(on)
	m2, l2, h2 := column(off)
	chart.BarErr(chartPath("x7_repair.svg"), "X7 network messages per operation, read repair on vs off", "R,W", "messages per client operation", groups, []string{"read repair on", "read repair off"}, [][]float64{m1, m2}, [][]float64{l1, l2}, [][]float64{h1, h2}, false)

	out("\n**Query all N replicas vs only R.** A read normally goes to all N replicas and returns after the first R answers. \"R only\" asks just R random replicas. With and without a slow replica (n3 +3ms). Mean (min-max) of %d runs.\n\n| R,W | n3 | query | get p99 ms | get failures %% | messages/op |\n|---|---|---|---|---|---|\n", *reps)
	var fg []string
	var allN, ronly []spread
	for _, q := range []rw{{1, 1}, {2, 2}} {
		for _, slow := range []time.Duration{0, 3 * time.Millisecond} {
			for _, ronlyMode := range []bool{false, true} {
				w := base(q)
				w.slow = slow
				w.cfg.Node.ReadFromROnly = ronlyMode
				s := measureAll(w, mGet99, mGetErr, mMsgs)
				label := map[bool]string{false: "all N", true: "R only"}[ronlyMode]
				n3 := "normal"
				if slow > 0 {
					n3 = "slow"
				}
				out("| %d,%d | %s | %s | %s | %s | %s |\n", q.R, q.W, n3, label, s[0], s[1], s[2])
				if ronlyMode {
					ronly = append(ronly, s[0])
				} else {
					allN = append(allN, s[0])
				}
			}
			fg = append(fg, fmt.Sprintf("R=%d, n3 %s", q.R, map[bool]string{false: "normal", true: "slow"}[slow > 0]))
		}
	}
	m3, l3, h3 := column(allN)
	m4, l4, h4 := column(ronly)
	chart.BarErr(chartPath("x7_fanout.svg"), "X7 read p99 latency: query all N replicas vs only R", "setting", "get p99 (ms)", fg, []string{"all N", "R only"}, [][]float64{m3, m4}, [][]float64{l3, l4}, [][]float64{h3, h4}, false)
	out("\n![repair](charts/x7_repair.svg)\n![fanout](charts/x7_fanout.svg)\n")
}

// keep strings imported for helpers in sibling files
var _ = strings.TrimSpace
