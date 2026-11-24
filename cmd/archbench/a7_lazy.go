// This file implements experiment A7, which compares eager Merkle-tree
// maintenance (every put rehashes its key and the path to the root) with
// lazy maintenance (a put only marks the key dirty; dirty keys are hashed
// once, just before a sync reads the trees). It measures the time spent in
// puts and the extra time the first sync round needs to catch the trees up,
// for workloads that write each key once and workloads that hammer a few
// hot keys. Puts go straight into replica state (Node.Inject), so the
// numbers are CPU cost without simulated network time.

package main

import (
	"fmt"
	"sort"
	"time"

	"dynamostore/store"
)

// lazyRun is one measurement of A7.
type lazyRun struct {
	put   time.Duration // total time of all puts
	flush time.Duration // first sync round minus an immediately repeated one
}

// measureLazy applies writes puts spread round-robin over keys keys, each
// put superseding the key's previous version on all 3 replicas, then runs
// two quiet sync rounds. The first round must also catch lazy trees up; the
// second is the same work without that, so their difference is the flush.
func measureLazy(lazy bool, keys, writes int) lazyRun {
	cfg := store.DefaultConfig()
	cfg.Node.LazyTrees = lazy
	cfg.Node.HandoffInterval = 0
	cfg.Latency = store.LatencyModel{Base: 20 * time.Microsecond}
	c := store.NewCluster(cfg)
	defer c.Close()
	names := make([]string, keys)
	pls := make([][]string, keys)
	for k := range names {
		names[k] = fmt.Sprintf("user:%d", k)
		pls[k] = c.PreferenceList(names[k])
	}
	start := time.Now()
	for i := 0; i < writes; i++ {
		k := i % keys
		n := uint64(i/keys + 1)
		v := store.Version{Value: "0123456789abcdef0123456789abcdef", Dot: store.Dot{Node: pls[k][0], Counter: n}, TS: int64(n), Origin: pls[k][0]}
		if n > 1 {
			v.Ctx = map[string]uint64{pls[k][0]: n - 1}
		}
		for _, id := range pls[k] {
			c.Nodes[id].Inject(names[k], v)
		}
	}
	r := lazyRun{put: time.Since(start)}
	t0 := time.Now()
	c.AntiEntropy()
	first := time.Since(t0)
	t0 = time.Now()
	c.AntiEntropy()
	r.flush = first - time.Since(t0)
	if r.flush < 0 {
		r.flush = 0
	}
	return r
}

// median3 returns the run with the median total (put + flush) of three.
func median3(lazy bool, keys, writes int) lazyRun {
	rs := make([]lazyRun, 3)
	for i := range rs {
		rs[i] = measureLazy(lazy, keys, writes)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].put+rs[i].flush < rs[j].put+rs[j].flush })
	return rs[1]
}

// lazyVsEager runs experiment A7, appends its table to the report and
// draws results/a7_lazy.svg.
func lazyVsEager() {
	w("\n## A7. Eager vs lazy Merkle-tree maintenance\n\n")
	w("Puts applied straight to replica state (3 replicas each), then one anti-entropy round on identical replicas. \"Flush\" is how much longer the first round took than an immediately repeated one (the lazy trees catching up). Median of 3 runs. Total = puts + flush.\n\n")
	w("| workload | strategy | puts | per put | flush | total |\n|---|---|---|---|---|---|\n")
	ws := []struct {
		name         string
		keys, writes int
	}{
		{"each key written once (20,000 keys)", 20000, 20000},
		{"10 writes per key (20,000 keys)", 20000, 200000},
		{"100 writes per key (2,000 keys)", 2000, 200000},
		{"1,000 writes per key (200 keys)", 200, 200000},
	}
	groups := []string{"1 write/key", "10 writes/key", "100 writes/key", "1,000 writes/key"}
	totals := [][]float64{{}, {}}
	for _, wl := range ws {
		for i, lazy := range []bool{false, true} {
			r := median3(lazy, wl.keys, wl.writes)
			name := map[bool]string{false: "eager", true: "lazy"}[lazy]
			w("| %s | %s | %.0fms | %.2fus | %.0fms | %.0fms |\n", wl.name, name, ms(r.put), float64(r.put)/float64(wl.writes)/1e3, ms(r.flush), ms(r.put+r.flush))
			totals[i] = append(totals[i], ms(r.put+r.flush))
		}
	}
	barChart("a7_lazy.svg", "Time to apply 20,000-200,000 puts and sync once (3 replicas each)", "writes per key", "milliseconds", groups, []string{"eager", "lazy"}, totals, false)
	w("\n![eager vs lazy](a7_lazy.svg)\n")
}

// ms converts a duration to milliseconds.
func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
