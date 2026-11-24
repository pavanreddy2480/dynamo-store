// This file implements experiment A1b, the ablation behind the default of 64
// virtual nodes per physical node. A1 only measured balance at four counts on
// one particular set of node names. A1b sweeps 1 to 1024 vnodes over 20
// different placements (different node names give different token
// positions) and measures four things per count: how evenly replica copies
// spread, how many copies move when a fifth node joins, what a lookup costs
// and what building the ring costs.

package main

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
)

// vnodeCounts are the vnodes-per-node values swept by A1b.
var vnodeCounts = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

const (
	a1bPlacements = 20    // independent token placements averaged per count
	a1bKeys       = 20000 // keys placed per placement
)

// vnodeResult is the outcome of A1b for one vnode count.
type vnodeResult struct {
	tokens                  int     // ring tokens with 5 nodes
	cv, spread, worstSpread float64 // mean CV, mean and worst max-min share (points)
	moved, worstMoved       float64 // copies moved on join: mean and worst (%)
	lookupNs, buildUs       float64 // PreferenceList cost; NewRing cost
}

// nodeNames returns n node ids unique to placement p, so each placement
// hashes its tokens to different ring positions.
func nodeNames(p, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("p%d-n%d", p, i+1)
	}
	return ids
}

// ablateVnodes measures one vnode count over every placement.
func ablateVnodes(vn int) vnodeResult {
	var r vnodeResult
	r.tokens = 5 * vn
	for p := 0; p < a1bPlacements; p++ {
		ids5 := nodeNames(p, 5)
		ring5, ring4 := store.NewRing(ids5, vn), store.NewRing(ids5[:4], vn)
		cnt := map[string]float64{}
		moved := 0
		for k := 0; k < a1bKeys; k++ {
			key := "user:" + strconv.Itoa(k)
			pl5 := ring5.PreferenceList(key, 3)
			for _, n := range pl5 {
				cnt[n]++
			}
			had := map[string]bool{}
			for _, n := range ring4.PreferenceList(key, 3) {
				had[n] = true
			}
			for _, n := range pl5 {
				if !had[n] {
					moved++
				}
			}
		}
		mn, mx, sum, sq := math.Inf(1), 0.0, 0.0, 0.0
		for _, id := range ids5 {
			c := 100 * cnt[id] / a1bKeys
			mn, mx, sum, sq = math.Min(mn, c), math.Max(mx, c), sum+c, sq+c*c
		}
		mean := sum / 5
		r.cv += math.Sqrt(sq/5-mean*mean) / mean / a1bPlacements
		r.spread += (mx - mn) / a1bPlacements
		r.worstSpread = math.Max(r.worstSpread, mx-mn)
		m := 100 * float64(moved) / float64(3*a1bKeys)
		r.moved += m / a1bPlacements
		r.worstMoved = math.Max(r.worstMoved, m)
	}
	// Lookup and build cost on placement 0, repeated enough to time.
	ring := store.NewRing(nodeNames(0, 5), vn)
	const lookups = 50000
	r.lookupNs = math.Inf(1)
	for rep := 0; rep < 5; rep++ { // fastest of 5, to damp GC and scheduler noise
		t0 := time.Now()
		for k := 0; k < lookups; k++ {
			ring.PreferenceList("user:"+strconv.Itoa(k), 3)
		}
		r.lookupNs = math.Min(r.lookupNs, float64(time.Since(t0).Nanoseconds())/lookups)
	}
	t0 := time.Now()
	const builds = 20
	for i := 0; i < builds; i++ {
		store.NewRing(nodeNames(0, 5), vn)
	}
	r.buildUs = float64(time.Since(t0).Microseconds()) / builds
	return r
}

// vnodeAblation runs experiment A1b, appends its table to the report and
// draws results/charts/a1b_balance.svg, a1b_moved.svg and a1b_lookup.svg.
func vnodeAblation() {
	w("\n## A1b. Ablation: how many virtual nodes per node?\n\n")
	w("MD5 ring, 5 nodes, N=3, %d keys, averaged over %d placements (each placement uses different node names, so tokens land in different places). Spread = most-loaded minus least-loaded node's share of replica copies, in points (ideal 0; every node should hold 60%%). Moved = replica copies a node must receive when a 5th node joins a 4-node ring (ideal 20%%; leaving is the same pair of rings read backwards). \"Worst\" is the worst of the %d placements. Lookup is `PreferenceList` on one core (fastest of 5 runs).\n\n", a1bKeys, a1bPlacements, a1bPlacements)
	w("| vnodes/node | tokens | mean CV | mean spread | worst spread | mean moved on join | worst moved | lookup ns | build µs |\n|---|---|---|---|---|---|---|---|---|\n")
	var xs []string
	spread, worst, moved, wmoved, ideal, lookup := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
	for _, vn := range vnodeCounts {
		r := ablateVnodes(vn)
		w("| %d | %d | %.3f | %.1f | %.1f | %.1f%% | %.1f%% | %.0f | %.0f |\n", vn, r.tokens, r.cv, r.spread, r.worstSpread, r.moved, r.worstMoved, r.lookupNs, r.buildUs)
		xs = append(xs, strconv.Itoa(vn))
		spread, worst = append(spread, r.spread), append(worst, r.worstSpread)
		moved, wmoved, ideal = append(moved, r.moved), append(wmoved, r.worstMoved), append(ideal, 20)
		lookup = append(lookup, r.lookupNs)
	}
	chart.Line(*outDir+"/charts/a1b_balance.svg", "Load imbalance vs virtual nodes per node (lower is better)", "virtual nodes per node", "max share - min share (points)", xs, []string{"mean of 20", "worst of 20"}, [][]float64{spread, worst}, false)
	chart.Line(*outDir+"/charts/a1b_moved.svg", "Replica copies moved when a node joins (4 to 5 nodes)", "virtual nodes per node", "% of all copies", xs, []string{"mean of 20", "worst of 20", "ideal"}, [][]float64{moved, wmoved, ideal}, false)
	chart.Line(*outDir+"/charts/a1b_lookup.svg", "Cost of one preference-list lookup", "virtual nodes per node", "ns per lookup", xs, []string{"lookup"}, [][]float64{lookup}, false)
	w("\n![balance](charts/a1b_balance.svg)\n![moved](charts/a1b_moved.svg)\n![lookup](charts/a1b_lookup.svg)\n")
}
