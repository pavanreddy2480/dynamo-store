// This file implements ablation experiments X9 (sibling and clock growth
// during a long partition), X10 (clients with different R and W in one run)
// and X11 (a node joining a running cluster). They close the gaps the
// earlier ablations listed as untested.

package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
	"dynamostore/vclock"
)

// ---------------------------------------------------------------- X9

// growth is what one X9 run measured.
type growth struct {
	maxSib       float64 // most siblings any replica held for a key, at the end of the partition
	maxClock     float64 // most entries in any version's clock
	maxKB        float64 // largest estimated size of one key's sibling set, in KB
	maxSibHeal   float64 // most siblings any replica held after healing and anti-entropy
	maxSibFinal  float64 // after one client read-merge-write of every key
	readTimeouts float64 // keys whose R=3 read of the sibling set timed out (the client then used R=1)
}

// growthRun runs `writers` clients on a split cluster, half on each side,
// all writing 8 hot keys with R=W=1, then measures the sibling sets.
//
// With blind=true every client writes without reading first (empty
// context), so each write is concurrent with every earlier one. Otherwise
// it reads, merges by union and writes back under the read context.
func growthRun(writers int, blind bool, seed int64) growth {
	cfg := store.DefaultConfig()
	cfg.Seed = seed
	c := store.NewCluster(cfg)
	defer c.Close()
	const keys = 8
	var sideA, sideB []string
	sideA, sideB = []string{"n1", "n2"}, []string{"n3", "n4", "n5"}
	var clients []*store.Client
	for i := 0; i < writers; i++ {
		cl := c.NewClient(fmt.Sprintf("c%d", i), 1, 1)
		clients = append(clients, cl)
		if i%2 == 0 {
			sideA = append(sideA, cl.ID)
		} else {
			sideB = append(sideB, cl.ID)
		}
	}
	c.Net.Partition(sideA, sideB)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for n := 0; !stop.Load(); n++ {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				if blind {
					_, _ = cl.Put(key, fmt.Sprintf("%s-%d", cl.ID, n), nil)
					continue
				}
				g, err := cl.Get(key)
				if err != nil {
					continue
				}
				_, _ = cl.Put(key, store.UnionResolver(append(g.Values(), cl.ID)), g.Context)
			}
		}(cl, int64(i))
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	var g growth
	scan := func() (sib, clock, kb float64) {
		for k := 0; k < keys; k++ {
			key := fmt.Sprintf("k%d", k)
			for _, nd := range c.Nodes {
				vs := nd.Snapshot(key)
				sib = math.Max(sib, float64(len(vs)))
				bytes := 0
				for _, v := range vs {
					clock = math.Max(clock, float64(len(v.Clock())))
					bytes += len(v.Value) + 16 + 8*len(v.Clock())
				}
				kb = math.Max(kb, float64(bytes)/1024)
			}
		}
		return
	}
	g.maxSib, g.maxClock, g.maxKB = scan()
	c.Net.Heal()
	c.WaitHintsDrained(2 * time.Second)
	for i := 0; i < 3; i++ {
		c.AntiEntropy()
	}
	g.maxSibHeal, _, _ = scan()
	fin := c.NewClient("final", 3, 3)
	fin1 := c.NewClient("final1", 1, 3)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		r, err := fin.Get(key)
		if err != nil { // reading a huge sibling set from all 3 replicas timed out
			g.readTimeouts++
			if r, err = fin1.Get(key); err != nil {
				continue
			}
		}
		_, _ = fin.Put(key, store.UnionResolver(r.Values()), r.Context)
	}
	c.WaitHintsDrained(time.Second)
	for i := 0; i < 3; i++ {
		c.AntiEntropy()
	}
	g.maxSibFinal, _, _ = scan()
	return g
}

// x9Growth shows how sibling sets and clocks grow during a long partition.
func x9Growth() {
	out("\n## X9. Sibling and clock growth during a long partition\n\n")
	out("Cluster split {n1,n2} | {n3,n4,n5}, R=W=1 strict, 8 hot keys, writers split over both sides, %v of partition, then heal, three anti-entropy rounds, then one client reads and writes back every key under the merged context. Mean (min-max) of %d runs. \"Union\" clients read, merge and write back (the intended use); \"blind\" clients write without reading, so every write is concurrent with every earlier one (the worst case).\n\n", *dur, *reps)
	out("| writers | clients | max siblings per key | max clock entries | largest sibling set KB | siblings after heal + anti-entropy | after read-merge-write | keys (of 8) whose R=3 read timed out |\n|---|---|---|---|---|---|---|---|\n")
	counts := []int{2, 8, 32}
	var groups []string
	sib := [][]float64{{}, {}}
	clk := [][]float64{{}, {}}
	for _, n := range counts {
		groups = append(groups, fmt.Sprint(n))
		for bi, blind := range []bool{false, true} {
			name := "union"
			if blind {
				name = "blind"
			}
			var cols [6][]float64
			for r := 0; r < *reps; r++ {
				g := growthRun(n, blind, int64(r+1))
				for i, v := range []float64{g.maxSib, g.maxClock, g.maxKB, g.maxSibHeal, g.maxSibFinal, g.readTimeouts} {
					cols[i] = append(cols[i], v)
				}
			}
			sp := func(i int) spread { return summarize(cols[i]) }
			out("| %d | %s | %s | %s | %s | %s | %s | %s |\n", n, name, sp(0), sp(1), sp(2), sp(3), sp(4), sp(5))
			sib[bi] = append(sib[bi], sp(0).mean)
			clk[bi] = append(clk[bi], sp(1).mean)
		}
	}
	chart.Bar(chartPath("x9_siblings.svg"), "X9 siblings per key at the end of a partition", "writer clients", "max siblings on one replica", groups, []string{"read-merge-write", "blind writes"}, sib, true)
	chart.Bar(chartPath("x9_clock.svg"), "X9 clock entries in the largest version", "writer clients", "clock entries", groups, []string{"read-merge-write", "blind writes"}, clk, false)
	out("\n![siblings](charts/x9_siblings.svg)\n![clock](charts/x9_clock.svg)\n")
}

// summarize returns the mean and range of vs.
func summarize(vs []float64) spread {
	s := spread{lo: math.Inf(1), hi: math.Inf(-1)}
	for _, v := range vs {
		s.mean += v / float64(len(vs))
		s.lo, s.hi = math.Min(s.lo, v), math.Max(s.hi, v)
	}
	return s
}

// ---------------------------------------------------------------- X10

// mixedRun runs one cluster whose keys have different write quorums and
// whose readers have different read quorums, and returns the stale-read
// percentage for every (key W, reader R) pair, indexed [W-1][R-1].
func mixedRun(seed int64) [3][3]float64 {
	cfg := store.DefaultConfig()
	cfg.Seed = seed
	c := store.NewCluster(cfg)
	defer c.Close()
	const keys = 30
	or := newOracle()
	var reads, stales [3][3]atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", k)
			cl := c.NewClient(fmt.Sprintf("w%d", k), 1, 1+k%3) // this key is always written with W=1+k%3
			var ctx vclock.VC
			for i := 0; !stop.Load(); i++ {
				if v, err := cl.Put(key, fmt.Sprint(i), ctx); err == nil {
					ctx = v.Clock()
					or.ack(key, v)
				}
			}
		}(k)
	}
	for r := 1; r <= 3; r++ {
		for j := 0; j < 3; j++ {
			wg.Add(1)
			go func(r, j int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(r*10 + j)))
				cl := c.NewClient(fmt.Sprintf("r%d-%d", r, j), r, 1)
				for !stop.Load() {
					k := rng.Intn(keys)
					key := fmt.Sprintf("k%d", k)
					before := or.snapshot(key)
					res, err := cl.Get(key)
					if err != nil {
						continue
					}
					reads[k%3][r-1].Add(1)
					if stale(before, res.Versions) {
						stales[k%3][r-1].Add(1)
					}
				}
			}(r, j)
		}
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	var out [3][3]float64
	for w := 0; w < 3; w++ {
		for r := 0; r < 3; r++ {
			out[w][r] = 100 * float64(stales[w][r].Load()) / math.Max(1, float64(reads[w][r].Load()))
		}
	}
	return out
}

// x10Mixed checks that a client's freshness depends only on its own R and
// the W the key was written with, not on what other clients are doing.
func x10Mixed() {
	out("\n## X10. Clients with different R and W in one run\n\n")
	out("One cluster, 30 keys. Key k is always written with W = 1 + k mod 3; nine readers use R=1, 2 or 3 (three each), all at the same time. Stale reads %% per (key W, reader R) pair, mean (min-max) of %d runs. The theory says a pair is never stale when R + W > 3; those cells are marked *.\n\n| key written with | R=1 | R=2 | R=3 |\n|---|---|---|---|\n", *reps)
	var runs [][3][3]float64
	for r := 0; r < *reps; r++ {
		runs = append(runs, mixedRun(int64(r+1)))
	}
	cell := func(w, r int) spread {
		var vs []float64
		for _, m := range runs {
			vs = append(vs, m[w][r])
		}
		return summarize(vs)
	}
	series := [][]float64{{}, {}, {}}
	var groups []string
	violated := 0.0
	for w := 0; w < 3; w++ {
		groups = append(groups, fmt.Sprintf("W=%d", w+1))
		row := []string{fmt.Sprintf("W=%d", w+1)}
		for r := 0; r < 3; r++ {
			s := cell(w, r)
			mark := ""
			if r+1+w+1 > 3 {
				mark = " *"
				violated = math.Max(violated, s.hi)
			}
			row = append(row, s.String()+mark)
			series[r] = append(series[r], s.mean)
		}
		out("| %s |\n", strings.Join(row, " | "))
	}
	out("\nLargest stale rate among the * cells (R + W > 3): %.2f%%.\n", violated)
	chart.Bar(chartPath("x10_mixed.svg"), "X10 stale reads when R and W differ per client", "key's write quorum", "stale reads (%)", groups, []string{"R=1", "R=2", "R=3"}, series, false)
	out("\n![mixed](charts/x10_mixed.svg)\n")
}

// ---------------------------------------------------------------- X11

// joinResult is what one X11 run measured.
type joinResult struct {
	stale        [3]float64 // stale read % before, during and after the join
	putFail      [3]float64 // failed put % in the same phases
	bootstrapMs  float64    // time until the new node held every static key it should
	lost         float64    // acknowledged writes missing from a current replica at the end
	copiesOnNew  float64    // percent of keys the new node ended up holding
	bootstrapped bool
}

// joinRun starts a 4-node cluster under load (R=W=2, strict), adds a fifth
// node, and measures the join.
func joinRun(interval time.Duration, seed int64) joinResult {
	cfg := store.DefaultConfig()
	cfg.Nodes = 4
	cfg.Seed = seed
	cfg.Node.AntiEntropyInterval = interval
	c := store.NewCluster(cfg)
	defer c.Close()
	const statics, hot = 400, 32
	loader := c.NewClient("loader", 3, 3)
	ackedStatic := map[string]store.Version{}
	for i := 0; i < statics; i++ {
		k := fmt.Sprintf("s%d", i)
		if v, err := loader.Put(k, "v", nil); err == nil {
			ackedStatic[k] = v
		}
	}
	or := newOracle()
	var phase atomic.Int32
	var sts [3]stats
	var stop atomic.Bool
	var wg sync.WaitGroup
	for k := 0; k < hot; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("h%d", k)
			cl := c.NewClient(fmt.Sprintf("w%d", k), 2, 2)
			var ctx vclock.VC
			for i := 0; !stop.Load(); i++ {
				ph := phase.Load()
				t0 := time.Now()
				v, err := cl.Put(key, fmt.Sprint(i), ctx)
				sts[ph].put(time.Since(t0), err == nil)
				if err == nil {
					ctx = v.Clock()
					or.ack(key, v)
				}
			}
		}(k)
	}
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			cl := c.NewClient(fmt.Sprintf("r%d", r), 2, 2)
			for !stop.Load() {
				ph := phase.Load()
				key := fmt.Sprintf("h%d", rng.Intn(hot))
				before := or.snapshot(key)
				t0 := time.Now()
				res, err := cl.Get(key)
				sts[ph].get(time.Since(t0), err == nil, stale(before, res.Versions), len(res.Versions))
			}
		}(r)
	}
	time.Sleep(*dur / 2)
	phase.Store(1)
	t0 := time.Now()
	id := c.AddNode()
	var res joinResult
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for k := range ackedStatic {
			for _, n := range c.PreferenceList(k) {
				if n == id && len(c.Nodes[id].Snapshot(k)) == 0 {
					done = false
				}
			}
		}
		if done {
			res.bootstrapped = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	res.bootstrapMs = float64(time.Since(t0)) / float64(time.Millisecond)
	phase.Store(2)
	time.Sleep(*dur / 2)
	stop.Store(true)
	wg.Wait()
	for i := 0; i < 3; i++ {
		c.AntiEntropy()
	}
	for i := range sts {
		st := &sts[i]
		res.stale[i] = 100 * float64(st.staleReads) / math.Max(1, float64(st.getOK))
		res.putFail[i] = 100 * float64(st.puts-st.putOK) / math.Max(1, float64(st.puts))
	}
	held, total := 0, 0
	for k, v := range ackedStatic {
		total++
		for _, n := range c.PreferenceList(k) {
			if n == id {
				held++
			}
			if !coversAny(c.Nodes[n].Snapshot(k), v) {
				res.lost++
			}
		}
	}
	res.copiesOnNew = 100 * float64(held) / math.Max(1, float64(total))
	return res
}

// coversAny reports whether some version in vs covers v.
func coversAny(vs []store.Version, v store.Version) bool {
	for _, x := range vs {
		if x.Covers(v) {
			return true
		}
	}
	return false
}

// x11Join measures a node joining a live cluster.
func x11Join() {
	out("\n## X11. A node joining a running cluster\n\n")
	out("4 nodes, 400 static keys written at W=3, then 32 hot keys with a writer each and 8 readers, all R=W=2 strict. After %v a fifth node joins (`Cluster.AddNode`); it starts empty and fills by anti-entropy. \"During\" is from the join until the new node holds every static key it should; \"after\" is the next %v. Mean (min-max) of %d runs.\n\n", *dur/2, *dur/2, *reps)
	out("| anti-entropy interval | stale %% before | stale %% during | stale %% after | put failures %% during | time until new node is filled (ms) | static copies lost | keys on new node %% |\n|---|---|---|---|---|---|---|---|\n")
	ivs := []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	var groups []string
	series := make([][]float64, len(ivs))
	groups = []string{"before join", "during join", "after join"}
	for ii, iv := range ivs {
		var runs []joinResult
		for r := 0; r < *reps; r++ {
			runs = append(runs, joinRun(iv, int64(r+1)))
		}
		col := func(f func(joinResult) float64) spread {
			var vs []float64
			for _, r := range runs {
				vs = append(vs, f(r))
			}
			return summarize(vs)
		}
		sb, sd, sa := col(func(r joinResult) float64 { return r.stale[0] }), col(func(r joinResult) float64 { return r.stale[1] }), col(func(r joinResult) float64 { return r.stale[2] })
		out("| %v | %s | %s | %s | %s | %s | %s | %s |\n", iv, sb, sd, sa,
			col(func(r joinResult) float64 { return r.putFail[1] }), col(func(r joinResult) float64 { return r.bootstrapMs }),
			col(func(r joinResult) float64 { return r.lost }), col(func(r joinResult) float64 { return r.copiesOnNew }))
		series[ii] = []float64{sb.mean, sd.mean, sa.mean}
	}
	names := make([]string, len(ivs))
	for i, iv := range ivs {
		names[i] = "interval " + iv.String()
	}
	chart.Bar(chartPath("x11_join.svg"), "X11 stale reads around a node join (R=W=2)", "phase", "stale reads (%)", groups, names, series, false)
	out("\n![join](charts/x11_join.svg)\n")
}
