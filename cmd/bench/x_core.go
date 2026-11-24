// This file implements ablation experiments X1 (run-to-run noise), X2
// (sensitivity to the latency model and timeout), X4 (cluster size and N)
// and X5 (workload shape). They all run the E1 workload through `run` with
// one thing changed, repeat each cell -reps times and report mean and
// range, so a difference smaller than the range is not a difference.

package main

import (
	"fmt"
	"path/filepath"
	"time"

	"dynamostore/chart"
	"dynamostore/network"
)

// measureAll runs w -reps times and summarises every metric of each run.
func measureAll(w workload, ms ...metric) []spread {
	runs := make([][]float64, len(ms))
	for i := 0; i < *reps; i++ {
		w.cfg.Seed = int64(i + 1)
		r := run(w)
		for j, m := range ms {
			runs[j] = append(runs[j], m(r))
		}
	}
	out := make([]spread, len(ms))
	for j, vs := range runs {
		out[j] = spread{lo: vs[0], hi: vs[0]}
		for _, v := range vs {
			out[j].mean += v / float64(len(vs))
			if v < out[j].lo {
				out[j].lo = v
			}
			if v > out[j].hi {
				out[j].hi = v
			}
		}
	}
	return out
}

// Metrics shared by the experiments.
var (
	mStale  metric = result.staleRate
	mGet99  metric = func(r result) float64 { return p99(r.st.getLat) }
	mPut99  metric = func(r result) float64 { return p99(r.st.putLat) }
	mPutErr metric = result.putFail
	mGetErr metric = result.getFail
	mMsgs   metric = result.msgsPerOp
)

// chartPath returns where an ablation chart is written.
func chartPath(name string) string { return filepath.Join(*outDir, "charts", name) }

// x1Noise repeats E1 cells to show how much a single run varies.
func x1Noise() {
	out("\n## X1. How much does one run vary?\n\n")
	out("E1 workload (32 keys, 16 readers), each cell run %d times with different latency seeds. Cells are mean (min-max). A difference smaller than the range is not a measured difference.\n\n", *reps)
	out("| R,W | stale reads %% | get p99 ms | put p99 ms |\n|---|---|---|---|\n")
	var labels []string
	var stale []spread
	for _, q := range []rw{{1, 1}, {1, 2}, {2, 1}, {2, 2}} {
		s := measureAll(base(q), mStale, mGet99, mPut99)
		out("| %d,%d | %s | %s | %s |\n", q.R, q.W, s[0], s[1], s[2])
		labels, stale = append(labels, fmt.Sprintf("%d,%d", q.R, q.W)), append(stale, s[0])
	}
	m, lo, hi := column(stale)
	chart.BarErr(chartPath("x1_noise.svg"), "X1 stale reads by quorum: mean and min-max over repeated runs", "R,W", "stale reads (%)", labels, []string{"stale reads"}, [][]float64{m}, [][]float64{lo}, [][]float64{hi}, false)

	out("\nRead repair on vs off at R=W=1 (the claim in D13 that it barely changes staleness):\n\n| read repair | stale reads %% |\n|---|---|\n")
	var rr []spread
	for _, on := range []bool{true, false} {
		w := base(rw{1, 1})
		w.cfg.Node.ReadRepair = on
		s := measureAll(w, mStale)[0]
		out("| %v | %s |\n", on, s)
		rr = append(rr, s)
	}
	verdict := "the ranges do not overlap, so read repair has a measurable effect"
	if rr[0].lo <= rr[1].hi && rr[1].lo <= rr[0].hi {
		verdict = "the ranges overlap, so there is no measurable effect of read repair on staleness"
	}
	out("\nVerdict: %s.\n\n![noise](charts/x1_noise.svg)\n", verdict)
}

// latencyModel is a named pair of replica and client latency models.
type latencyModel struct {
	name    string
	replica network.LatencyModel
	client  network.LatencyModel
}

// latencyModels are a fast LAN, the default and a cross-region deployment.
var latencyModels = []latencyModel{
	{"LAN (0.1ms hop)", network.LatencyModel{Base: 100 * time.Microsecond, JitterMean: 200 * time.Microsecond, SlowProb: .03, SlowExtra: time.Millisecond}, network.LatencyModel{Base: 50 * time.Microsecond, JitterMean: 50 * time.Microsecond}},
	{"default (0.4ms hop)", network.DefaultLatency, network.DefaultClientLatency},
	{"cross-region (5ms hop)", network.LatencyModel{Base: 5 * time.Millisecond, JitterMean: 5 * time.Millisecond, SlowProb: .03, SlowExtra: 50 * time.Millisecond}, network.LatencyModel{Base: 500 * time.Microsecond, JitterMean: 500 * time.Microsecond}},
}

// x2Latency reruns E1 and the timeout experiment under three latency models.
func x2Latency() {
	out("\n## X2. Sensitivity to the latency model\n\n")
	out("Every latency, staleness and timeout number depends on the simulated network. Same E1 workload under three models (replica hop base + exponential jitter, 3%% of messages hit a slow path). Mean (min-max) of %d runs.\n\n", *reps)
	out("| model | R,W | stale reads %% | get p99 ms | put p99 ms | put failures %% (20ms timeout) |\n|---|---|---|---|---|---|\n")
	qs := []rw{{1, 1}, {1, 2}, {2, 2}}
	stale := make([][]spread, len(latencyModels))
	for mi, lm := range latencyModels {
		for _, q := range qs {
			w := base(q)
			w.cfg.Latency, w.cfg.Client = lm.replica, lm.client
			s := measureAll(w, mStale, mGet99, mPut99, mPutErr)
			out("| %s | %d,%d | %s | %s | %s | %s |\n", lm.name, q.R, q.W, s[0], s[1], s[2], s[3])
			stale[mi] = append(stale[mi], s[0])
		}
	}
	var groups, names []string
	for _, q := range qs {
		groups = append(groups, fmt.Sprintf("%d,%d", q.R, q.W))
	}
	var means [][]float64
	for mi, lm := range latencyModels {
		names = append(names, lm.name)
		m, _, _ := column(stale[mi])
		means = append(means, m)
	}
	chart.Bar(chartPath("x2_stale.svg"), "X2 stale reads under three latency models", "R,W", "stale reads (%)", groups, names, means, false)

	out("\nReplica timeout vs spurious put failures on a healthy cluster (R=W=2), same three models:\n\n| model | timeout | put failures %% |\n|---|---|---|\n")
	tos := []time.Duration{5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond, 0} // 0 = AutoTimeout
	var tg []string
	for _, to := range tos {
		tg = append(tg, toLabel(to))
	}
	fails := make([][]float64, len(latencyModels))
	for mi, lm := range latencyModels {
		for _, to := range tos {
			w := base(rw{2, 2})
			w.cfg.Latency, w.cfg.Client = lm.replica, lm.client
			w.cfg.Node.ReplicaTimeout = to
			s := measureAll(w, mPutErr)[0]
			out("| %s | %s | %s |\n", lm.name, toLabel(to), s)
			fails[mi] = append(fails[mi], s.mean)
		}
	}
	chart.Bar(chartPath("x2_timeout.svg"), "X2 spurious put failures vs replica timeout", "replica timeout", "puts failing (%)", tg, names, fails, false)
	out("\n![stale](charts/x2_stale.svg)\n![timeout](charts/x2_timeout.svg)\n")
}

// x4Cluster varies the node count and the replication factor N.
func x4Cluster() {
	out("\n## X4. Cluster size and replication factor\n\n")
	out("E1 workload. \"Majority\" is R=W=floor(N/2)+1 (R+W>N). Mean (min-max) of %d runs.\n\n", *reps)
	out("| nodes | N | R=W=1 stale %% | majority quorum | majority stale %% | majority get p99 ms | majority put p99 ms |\n|---|---|---|---|---|---|---|\n")
	var labels []string
	var one, maj []spread
	for _, c := range []struct{ nodes, n int }{{5, 2}, {5, 3}, {5, 5}, {7, 3}} {
		mq := c.n/2 + 1
		w1, wm := base(rw{1, 1}), base(rw{mq, mq})
		for _, w := range []*workload{&w1, &wm} {
			w.cfg.Nodes, w.cfg.Node.N = c.nodes, c.n
		}
		a := measureAll(w1, mStale)[0]
		b := measureAll(wm, mStale, mGet99, mPut99)
		out("| %d | %d | %s | %d,%d | %s | %s | %s |\n", c.nodes, c.n, a, mq, mq, b[0], b[1], b[2])
		labels, one, maj = append(labels, fmt.Sprintf("%d nodes, N=%d", c.nodes, c.n)), append(one, a), append(maj, b[0])
	}
	m1, l1, h1 := column(one)
	mm, lm, hm := column(maj)
	chart.BarErr(chartPath("x4_cluster.svg"), "X4 stale reads by cluster size and N", "cluster", "stale reads (%)", labels, []string{"R=W=1", "majority quorum"}, [][]float64{m1, mm}, [][]float64{l1, lm}, [][]float64{h1, hm}, false)
	out("\n![cluster](charts/x4_cluster.svg)\n")
}

// x5Workload varies the key count, reader count, write rate and key skew.
func x5Workload() {
	out("\n## X5. Workload shape\n\n")
	out("Stale reads depend on how often a read lands right after a write to the same key. Mean (min-max) of %d runs.\n\n", *reps)
	out("| workload | R=W=1 stale %% | R=1,W=2 stale %% |\n|---|---|---|\n")
	cases := []struct {
		name string
		f    func(*workload)
	}{
		{"baseline: 32 keys, 16 readers", func(*workload) {}},
		{"8 keys", func(w *workload) { w.keys = 8 }},
		{"256 keys", func(w *workload) { w.keys = 256 }},
		{"4 readers", func(w *workload) { w.readers = 4 }},
		{"64 readers", func(w *workload) { w.readers = 64 }},
		{"writers pause 2ms (read-heavy)", func(w *workload) { w.think = 2 * time.Millisecond }},
		{"Zipf-skewed reads (hot keys)", func(w *workload) { w.zipf = true }},
	}
	var labels []string
	var a, b []spread
	for _, c := range cases {
		w1, w2 := base(rw{1, 1}), base(rw{1, 2})
		c.f(&w1)
		c.f(&w2)
		sa, sb := measureAll(w1, mStale)[0], measureAll(w2, mStale)[0]
		out("| %s | %s | %s |\n", c.name, sa, sb)
		labels, a, b = append(labels, c.name), append(a, sa), append(b, sb)
	}
	short := []string{"baseline", "8 keys", "256 keys", "4 readers", "64 readers", "slow writers", "Zipf reads"}
	m1, l1, h1 := column(a)
	m2, l2, h2 := column(b)
	chart.BarErr(chartPath("x5_workload.svg"), "X5 stale reads by workload shape", "workload", "stale reads (%)", short, []string{"R=W=1", "R=1,W=2"}, [][]float64{m1, m2}, [][]float64{l1, l2}, [][]float64{h1, h2}, false)
	out("\n![workload](charts/x5_workload.svg)\n")
}

// toLabel names a replica timeout; zero is the latency-derived default.
func toLabel(to time.Duration) string {
	if to == 0 {
		return "auto"
	}
	return to.String()
}
