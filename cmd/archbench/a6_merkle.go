// This file implements experiment A6, which justifies replacing full-state
// anti-entropy with Merkle trees and picks the tree depth. Replicas start
// identical on 20,000 keys; then a given fraction of keys gets a newer
// version on just one of its replicas. One anti-entropy round runs and the
// traffic it needed is counted.

package main

import (
	"fmt"
	"math/rand"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
)

// a6Keys is the number of keys in every A6 run.
const a6Keys = 20000

// syncCost holds what one anti-entropy round cost and whether it worked.
type syncCost struct {
	s         store.SyncStats
	elapsed   time.Duration
	divergent int // keys whose replicas still disagree afterwards
}

// divergedCluster builds a cluster whose replicas agree on a6Keys keys
// except for a fraction div of keys, which have a newer version on one
// replica only. The same seed gives the same state for any SyncMode.
func divergedCluster(cfg store.Config, div float64) *store.Cluster {
	cfg.Node.HandoffInterval = 0
	cfg.Node.AntiEntropyInterval = 0
	// Full-state messages carry the whole key space; give every exchange
	// time to finish so the comparison is about traffic, not timeouts.
	cfg.Node.ReplicaTimeout = 10 * time.Second
	c := store.NewCluster(cfg)
	rng := rand.New(rand.NewSource(42))
	value := "0123456789abcdef0123456789abcdef" // 32-byte payload
	for k := 0; k < a6Keys; k++ {
		key := fmt.Sprintf("user:%d", k)
		pl := c.PreferenceList(key)
		v0 := store.Version{Value: value, Dot: store.Dot{Node: pl[0], Counter: 1}, TS: 1, Origin: pl[0]}
		for _, id := range pl {
			c.Nodes[id].Inject(key, v0)
		}
		if rng.Float64() < div {
			v1 := store.Version{Value: value + "!", Dot: store.Dot{Node: pl[0], Counter: 2}, Ctx: v0.Clock(), TS: 2, Origin: pl[0]}
			c.Nodes[pl[rng.Intn(len(pl))]].Inject(key, v1)
		}
	}
	return c
}

// measureSync runs one anti-entropy round on a fresh diverged cluster.
func measureSync(cfg store.Config, div float64) syncCost {
	c := divergedCluster(cfg, div)
	defer c.Close()
	start := time.Now()
	c.AntiEntropy()
	r := syncCost{s: c.SyncStats(), elapsed: time.Since(start)}
	for k := 0; k < a6Keys; k++ {
		key := fmt.Sprintf("user:%d", k)
		var first string
		for i, id := range c.PreferenceList(key) {
			if s := fmt.Sprint(c.Nodes[id].Snapshot(key)); i == 0 {
				first = s
			} else if s != first {
				r.divergent++
				break
			}
		}
	}
	return r
}

// merkleVsFull runs experiment A6, appends its tables to the report and
// draws results/charts/a6_keys_sent.svg, a6_bytes_sent.svg and a6_depth.svg.
func merkleVsFull() {
	w("\n## A6. Anti-entropy: full-state exchange vs Merkle trees\n\n")
	w("%d keys, all 3 replicas identical, then a fraction of keys gets a newer version on one replica only. One anti-entropy round (10 node pairs). Traffic is both directions; bytes use a simple size model (key, value, dot, context entries; 8 bytes per hash).\n\n", a6Keys)
	w("| strategy | divergence | keys sent | KB sent | round trips | time | divergent keys after |\n|---|---|---|---|---|---|---|\n")
	divs := []float64{0, 0.01, 0.10}
	modes := []struct {
		name string
		m    store.SyncMode
	}{{"full-state", store.FullStateSync}, {"Merkle (per partition)", store.MerkleSync}}
	keys, kb := make([][]float64, len(modes)), make([][]float64, len(modes))
	for i, m := range modes {
		for _, d := range divs {
			cfg := store.DefaultConfig()
			cfg.Node.AntiEntropy = m.m
			r := measureSync(cfg, d)
			w("| %s | %g%% | %d | %.1f | %d | %.0fms | %d |\n", m.name, 100*d, r.s.Keys, float64(r.s.Bytes)/1024, r.s.RoundTrips, float64(r.elapsed)/float64(time.Millisecond), r.divergent)
			keys[i] = append(keys[i], float64(r.s.Keys))
			kb[i] = append(kb[i], float64(r.s.Bytes)/1024)
		}
	}
	groups := []string{"0% diverged", "1% diverged", "10% diverged"}
	names := []string{modes[0].name, modes[1].name}
	chart.Bar(*outDir+"/charts/a6_keys_sent.svg", "Keys sent in one anti-entropy round (20,000 keys, log scale)", "fraction of keys that differ on one replica", "keys sent", groups, names, keys, true)
	chart.Bar(*outDir+"/charts/a6_bytes_sent.svg", "KB sent in one anti-entropy round (20,000 keys, log scale)", "fraction of keys that differ on one replica", "KB sent", groups, names, kb, true)
	w("\n![keys sent](charts/a6_keys_sent.svg)\n![bytes sent](charts/a6_bytes_sent.svg)\n")

	w("\nDepth of each partition's tree (leaves = 2^depth) at 1%% and 10%% divergence:\n\n| depth | divergence | keys sent | hashes sent | KB sent | round trips |\n|---|---|---|---|---|---|\n")
	depths := []float64{0, 1, 2, 3, 4, 5, 6, 8}
	dkb := make([][]float64, 2)
	var depthLabels []string
	for _, d := range depths {
		depthLabels = append(depthLabels, fmt.Sprint(int(d)))
	}
	for i, d := range []float64{0.01, 0.10} {
		for _, depth := range depths {
			cfg := store.DefaultConfig()
			cfg.Node.MerkleDepth = int(depth)
			r := measureSync(cfg, d)
			w("| %d | %g%% | %d | %d | %.1f | %d |\n", int(depth), 100*d, r.s.Keys, r.s.Hashes, float64(r.s.Bytes)/1024, r.s.RoundTrips)
			dkb[i] = append(dkb[i], float64(r.s.Bytes)/1024)
		}
	}
	chart.Line(*outDir+"/charts/a6_depth.svg", "KB sent per round vs depth of each partition tree (20,000 keys, 256 partitions)", "tree depth per partition (2^depth leaves)", "KB sent", depthLabels, []string{"1% diverged", "10% diverged"}, dkb, false)
	w("\n![depth](charts/a6_depth.svg)\n")
}
