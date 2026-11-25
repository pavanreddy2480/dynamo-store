// This file implements experiment A9, which closes two gaps left by A8:
// how many fixed partitions Q to use, and how a plain modulo-hash placement
// (key hash mod N picks the first replica) compares. For each Q it builds a
// 4-node fixed ring, adds a fifth node with WithNode, and measures balance
// and the replica copies that move; modulo placement is measured the same
// way.

package main

import (
	"math"
	"strconv"

	"dynamostore/chart"
	"dynamostore/ring"
)

// moduloReplicas returns the 3 replicas modulo placement gives key on n
// nodes: node hash%n and the next two in id order.
func moduloReplicas(key string, n int) []int {
	h := int(ring.Hash64(key) % uint64(n))
	return []int{h, (h + 1) % n, (h + 2) % n}
}

// moduloStats measures modulo placement on 5 nodes and a 4 to 5 join.
func moduloStats(keys int) (spread, moved float64) {
	cnt := make([]float64, 5)
	m := 0
	for k := 0; k < keys; k++ {
		key := "user:" + strconv.Itoa(k)
		had := map[int]bool{}
		for _, n := range moduloReplicas(key, 4) {
			had[n] = true
		}
		for _, n := range moduloReplicas(key, 5) {
			cnt[n]++
			if !had[n] {
				m++
			}
		}
	}
	mn, mx := math.Inf(1), 0.0
	for _, c := range cnt {
		mn, mx = math.Min(mn, 100*c/float64(keys)), math.Max(mx, 100*c/float64(keys))
	}
	return mx - mn, 100 * float64(m) / float64(3*keys)
}

// ringQ runs experiment A9, appends its table to the report and draws
// results/charts/a9_q_balance.svg and a9_q_moved.svg.
func ringQ() {
	const keys = 100000
	ids4, ids5 := nodes(4), nodes(5)
	w("\n## A9. How many fixed partitions, and why not modulo hashing?\n\n")
	w("%d keys, N=3, 5 nodes after a join from 4. Spread = most-loaded minus least-loaded node's share of replica copies, in points (ideal 0). Moved = replica copies that must move on the join (ideal 20%%). The modulo baseline puts a key on node hash%%N and the next two nodes.\n\n", keys)
	w("| scheme | partitions per node | spread (pts) | copies moved on join |\n|---|---|---|---|\n")
	qs := []int{16, 32, 64, 128, 256, 512, 1024, 4096}
	var xs []string
	var spreads, moved, ideal, modMoved []float64
	ms, mm := moduloStats(keys)
	for _, q := range qs {
		four := ring.NewFixedRing(ids4, q)
		five := four.WithNode("n5")
		mn, mx, _ := balance(five, ids5, keys)
		j := copiesMoved(four, five, keys)
		w("| fixed, Q=%d | %.1f | %.1f | %.1f%% |\n", q, float64(q)/5, mx-mn, j)
		xs = append(xs, strconv.Itoa(q))
		spreads, moved, ideal, modMoved = append(spreads, mx-mn), append(moved, j), append(ideal, 20), append(modMoved, mm)
	}
	w("| modulo hash | - | %.1f | %.1f%% |\n", ms, mm)
	chart.Line(*outDir+"/charts/a9_q_balance.svg", "Load imbalance vs number of fixed partitions Q", "partitions Q", "max share - min share (points)", xs, []string{"fixed Q"}, [][]float64{spreads}, false)
	chart.Line(*outDir+"/charts/a9_q_moved.svg", "Replica copies moved on a join: fixed partitions vs modulo hashing", "partitions Q", "% of all copies", xs, []string{"fixed Q", "ideal", "modulo"}, [][]float64{moved, ideal, modMoved}, false)
	w("\n![balance](charts/a9_q_balance.svg)\n![moved](charts/a9_q_moved.svg)\n")
}
