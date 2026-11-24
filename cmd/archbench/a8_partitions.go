// This file implements experiment A8, which compares three ways of placing
// nodes on the ring: equally spaced single tokens (one piece per node),
// random virtual nodes (the store's default) and Q fixed equal partitions
// dealt to nodes (Dynamo's strategy 3). It reports how evenly each spreads
// replica load, and how many replica copies must move when a fifth node
// joins a four-node cluster or leaves a five-node one.

package main

import (
	"fmt"
	"math"

	"dynamostore/chart"
	"dynamostore/store"
)

// placement builds the rings of one scheme: the 4-node ring, the 5-node ring
// reached by a join, and the 4-node ring reached by that node leaving.
type placement struct {
	name              string
	four, five, leave *store.Ring
}

// copiesMoved returns the percentage of replica copies (key, node) that the
// new ring needs and the old ring did not have, over keys keys.
func copiesMoved(old, new *store.Ring, keys int) float64 {
	moved := 0
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("user:%d", k)
		had := map[string]bool{}
		for _, n := range old.PreferenceList(key, 3) {
			had[n] = true
		}
		for _, n := range new.PreferenceList(key, 3) {
			if !had[n] {
				moved++
			}
		}
	}
	return 100 * float64(moved) / float64(3*keys)
}

// primariesMoved returns the percentage of keys whose first replica differs
// between the two rings.
func primariesMoved(old, new *store.Ring, keys int) float64 {
	moved := 0
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("user:%d", k)
		if old.Walk(key)[0] != new.Walk(key)[0] {
			moved++
		}
	}
	return 100 * float64(moved) / float64(keys)
}

// balance returns the min and max node share of replica copies (ideal 60%
// with 5 nodes and N=3) and the coefficient of variation.
func balance(r *store.Ring, ids []string, keys int) (mn, mx, cv float64) {
	cnt := map[string]float64{}
	for k := 0; k < keys; k++ {
		for _, p := range r.PreferenceList(fmt.Sprintf("user:%d", k), 3) {
			cnt[p]++
		}
	}
	mn = math.Inf(1)
	var sum, sq float64
	for _, id := range ids {
		c := cnt[id] / float64(keys)
		mn, mx, sum, sq = math.Min(mn, c), math.Max(mx, c), sum+c, sq+c*c
	}
	mean := sum / float64(len(ids))
	return 100 * mn, 100 * mx, math.Sqrt(sq/float64(len(ids))-mean*mean) / mean
}

// partitionExperiment runs experiment A8, appends its tables to the report
// and draws results/a8_balance.svg and a8_moved.svg.
func partitionExperiment() {
	const keys = 100000
	ids4, ids5 := nodes(4), nodes(5)
	fixed5 := store.NewFixedRing(ids4, 256).WithNode("n5")
	schemes := []placement{
		{"equal spacing, 1 piece per node", store.NewFixedRing(ids4, 4), store.NewFixedRing(ids5, 5), store.NewFixedRing(ids4, 4)},
		{"random vnodes, 8 per node", store.NewRing(ids4, 8), store.NewRing(ids5, 8), store.NewRing(ids4, 8)},
		{"random vnodes, 64 per node (default)", store.NewRing(ids4, 64), store.NewRing(ids5, 64), store.NewRing(ids4, 64)},
		{"fixed partitions, Q=256", store.NewFixedRing(ids4, 256), fixed5, fixed5.WithoutNode("n5")},
	}
	w("\n## A8. Ring placement: equal spacing vs random vnodes vs fixed partitions\n\n")
	w("%d keys, N=3. With 5 nodes each node should hold 60%% of the replica copies. Moving a node in or out should ideally move 20%% of all copies (the joining node holds 3/5 of one copy-set; the leaving one held the same). Moved = copies a node must receive because it was not in the key's old preference list.\n\n", keys)
	w("| scheme | min share | max share | CV | keys whose first replica changes on join | copies moved on join 4→5 | copies moved on leave 5→4 |\n|---|---|---|---|---|---|---|\n")
	var cvs []float64
	joins, leaves := []float64{}, []float64{}
	var names []string
	for _, s := range schemes {
		mn, mx, cv := balance(s.five, ids5, keys)
		j, l := copiesMoved(s.four, s.five, keys), copiesMoved(s.five, s.leave, keys)
		w("| %s | %.1f%% | %.1f%% | %.3f | %.1f%% | %.1f%% | %.1f%% |\n", s.name, mn, mx, cv, primariesMoved(s.four, s.five, keys), j, l)
		names = append(names, s.name)
		cvs, joins, leaves = append(cvs, mx-mn), append(joins, j), append(leaves, l)
	}
	short := []string{"equal spacing", "vnodes x8", "vnodes x64", "fixed Q=256"}
	chart.Bar(*outDir+"/a8_balance.svg", "Load imbalance: most-loaded minus least-loaded node (points of share, ideal 0)", "placement scheme", "max share - min share (%)", short, []string{"5 nodes"}, [][]float64{cvs}, false)
	chart.Bar(*outDir+"/a8_moved.svg", "Replica copies moved when a node joins or leaves (ideal 20%)", "placement scheme", "% of all copies moved", short, []string{"join 4→5", "leave 5→4"}, [][]float64{joins, leaves}, false)
	w("\n![balance](a8_balance.svg)\n![moved](a8_moved.svg)\n")
}
