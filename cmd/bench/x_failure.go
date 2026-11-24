// This file implements ablation experiment X8: what happens when the
// failure detector is wrong (false suspicions) and when the partition has a
// different shape than E3's two-sided split.

package main

import (
	"fmt"
	"time"

	"dynamostore/chart"
	"dynamostore/store"
)

// x8Failure runs the failure-detector and partition-shape ablations.
func x8Failure() {
	out("\n## X8. Failure detector and partition shape\n\n")
	out("**False suspicions.** Healthy cluster, sloppy quorums, R=W=2. With probability p the coordinator's `Reachable` wrongly reports a live node as down (messages still flow), so it sends to fallback nodes needlessly. Mean (min-max) of %d runs.\n\n| p | stale reads %% | messages/op | hints parked at end | put failures %% |\n|---|---|---|---|---|\n", *reps)
	var labels []string
	var stale []spread
	for _, p := range []float64{0, .05, .20} {
		w := base(rw{2, 2})
		w.sloppy = true
		w.cfg.Seed = 1
		s := measureAllNet(w, p)
		out("| %.0f%% | %s | %s | %s | %s |\n", 100*p, s[0], s[1], s[2], s[3])
		labels, stale = append(labels, fmt.Sprintf("%.0f%%", 100*p)), append(stale, s[0])
	}
	m, lo, hi := column(stale)
	chart.BarErr(chartPath("x8_suspicion.svg"), "X8 stale reads with a wrong failure detector (sloppy R=W=2)", "false-suspicion probability", "stale reads (%)", labels, []string{"stale reads"}, [][]float64{m}, [][]float64{lo}, [][]float64{hi}, false)

	out("\n**Partition shapes.** 12 clients spread over the groups, R=W=2, 2s on 64 keys (flapping: split and heal every 200ms). After the end, heal, drain hints, run 2 anti-entropy rounds and check that every acknowledged write is still readable at R=3 and the replicas agree. One run per cell.\n\n| shape | quorum | availability %% | acked writes lost | replicas agree |\n|---|---|---|---|---|\n")
	shapes := []struct {
		name   string
		groups [][]string
		flap   bool
	}{
		{"1 vs 4 nodes", [][]string{{"n1"}, {"n2", "n3", "n4", "n5"}}, false},
		{"2 vs 3 nodes (E3)", sloppyPartition, false},
		{"2 vs 2 vs 1 nodes", [][]string{{"n1", "n2"}, {"n3", "n4"}, {"n5"}}, false},
		{"2 vs 3, flapping", sloppyPartition, true},
	}
	var sg []string
	avail := [][]float64{{}, {}}
	for _, sh := range shapes {
		sg = append(sg, sh.name)
		for mi, sloppy := range []bool{false, true} {
			cfg := store.DefaultConfig()
			pr := partitionRun(cfg, rw{2, 2}, sloppy, sh.groups, sh.flap, 2*time.Second)
			pr.c.Net.Heal()
			pr.c.WaitHintsDrained(2 * time.Second)
			pr.c.AntiEntropy()
			pr.c.AntiEntropy()
			lost := 0
			reader := pr.c.NewClient("reader", 3, 3)
			for key, acked := range pr.acked {
				g, err := reader.Get(key)
				if err != nil {
					lost += len(acked)
					continue
				}
				for _, a := range acked {
					seen := false
					for _, v := range g.Versions {
						seen = seen || v.Covers(a)
					}
					if !seen {
						lost++
					}
				}
			}
			av := 100 * float64(pr.ok) / float64(pr.attempted)
			mode := map[bool]string{false: "strict", true: "sloppy"}[sloppy]
			out("| %s | %s | %.1f | %d | %v |\n", sh.name, mode, av, lost, divergent(pr.c, pr.keys) == 0)
			avail[mi] = append(avail[mi], av)
			pr.c.Close()
		}
	}
	chart.Bar(chartPath("x8_shapes.svg"), "X8 availability by partition shape (R=W=2)", "partition shape", "successful operations (%)", sg, []string{"strict", "sloppy"}, avail, false)
	out("\n![suspicion](charts/x8_suspicion.svg)\n![shapes](charts/x8_shapes.svg)\n")
}

// measureAllNet runs w with a false-suspicion probability p on the failure
// detector and summarises stale %, messages/op, hints parked and put
// failures.
func measureAllNet(w workload, p float64) []spread {
	w.cfg.FalseSuspicion = p
	return measureAll(w, mStale, mMsgs, func(r result) float64 { return float64(r.hints) }, mPutErr)
}
