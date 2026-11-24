// This file implements experiment A10: does walking a differing partition's
// Merkle tree one level per round trip pay off against fetching the whole
// tree in one round trip (MerkleOneShot)? It repeats A6's diverged-cluster
// round at 1% and 10% divergence for several tree depths and both modes.

package main

import (
	"fmt"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
)

// merkleWalk runs experiment A10, appends its table to the report and
// draws results/a10_roundtrips.svg and a10_kb.svg.
func merkleWalk() {
	w("\n## A10. Merkle walk: one level per round trip vs whole tree at once\n\n")
	w("A6's setup (20,000 keys, 256 partitions, one round over 10 node pairs). Level-by-level sends only the hashes under subtrees that differ but needs one round trip per level; one-shot sends every hash of a differing partition's tree after the roots.\n\n")
	w("| depth | divergence | walk | round trips | hashes sent | KB sent | time |\n|---|---|---|---|---|---|---|\n")
	var groups []string
	rt, kb := [][]float64{{}, {}}, [][]float64{{}, {}}
	for _, depth := range []int{2, 4, 6} {
		for _, d := range []float64{0.01, 0.10} {
			groups = append(groups, fmt.Sprintf("depth %d, %g%%", depth, 100*d))
			for mi, one := range []bool{false, true} {
				cfg := store.DefaultConfig()
				cfg.Node.MerkleDepth = depth
				cfg.Node.MerkleOneShot = one
				r := measureSync(cfg, d)
				name := map[bool]string{false: "level by level", true: "one shot"}[one]
				w("| %d | %g%% | %s | %d | %d | %.1f | %.0fms |\n", depth, 100*d, name, r.s.RoundTrips, r.s.Hashes, float64(r.s.Bytes)/1024, float64(r.elapsed)/float64(time.Millisecond))
				rt[mi], kb[mi] = append(rt[mi], float64(r.s.RoundTrips)), append(kb[mi], float64(r.s.Bytes)/1024)
			}
		}
	}
	names := []string{"level by level", "one shot"}
	chart.Bar(*outDir+"/a10_roundtrips.svg", "Round trips per anti-entropy round (10 pairs)", "tree depth and divergence", "round trips", groups, names, rt, false)
	chart.Bar(*outDir+"/a10_kb.svg", "KB sent per anti-entropy round", "tree depth and divergence", "KB sent", groups, names, kb, false)
	w("\n![round trips](a10_roundtrips.svg)\n![kb](a10_kb.svg)\n")
}
