// This file holds the shared plumbing for the ablation experiments X1 to X8
// (run with `go run ./cmd/bench -ablate`, results in results/ablations.md):
// a generalised version of the E1 workload whose cluster, keys, readers and
// read pattern can all be varied, repetition helpers that report mean and
// range, and the report writer. The experiments themselves are in x*.go.

package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
	"dynamostore/vclock"
)

// workload describes one E1-style run: one writer per key, many readers.
type workload struct {
	cfg     store.Config  // cluster configuration (Seed is varied per repetition)
	q       rw            // read and write quorum of every client
	keys    int           // keys, each with its own writer
	readers int           // reader goroutines
	think   time.Duration // writer pause between writes
	zipf    bool          // readers pick keys with a Zipf(1.2) skew instead of uniformly
	slow    time.Duration // extra delay on every message of n3
	sloppy  bool          // clients use sloppy quorums
}

// base returns the E1 workload: 32 keys, 16 readers, default cluster.
func base(q rw) workload {
	return workload{cfg: store.DefaultConfig(), q: q, keys: 32, readers: 16}
}

// result is the outcome of one run.
type result struct {
	st     *stats
	msgs   int64 // network messages sent during the run
	hints  int   // hinted keys still parked at the end
	syncKB float64
}

// staleRate returns stale reads as a percentage of successful reads.
func (r result) staleRate() float64 {
	return 100 * float64(r.st.staleReads) / math.Max(1, float64(r.st.getOK))
}

// putFail returns failed writes as a percentage of attempted writes.
func (r result) putFail() float64 {
	return 100 * float64(r.st.puts-r.st.putOK) / math.Max(1, float64(r.st.puts))
}

// getFail returns failed reads as a percentage of attempted reads.
func (r result) getFail() float64 {
	return 100 * float64(r.st.gets-r.st.getOK) / math.Max(1, float64(r.st.gets))
}

// msgsPerOp returns network messages per attempted client operation.
func (r result) msgsPerOp() float64 {
	return float64(r.msgs) / math.Max(1, float64(r.st.gets+r.st.puts))
}

// p99 returns the 99th percentile latency in milliseconds of ds.
func p99(ds []time.Duration) float64 { return pctMs(ds, .99) }

// pctMs is pct as a number: the p-th percentile of ds in milliseconds, or 0.
func pctMs(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return 0
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return float64(c[int(p*float64(len(c)-1))]) / float64(time.Millisecond)
}

// run executes the workload for -duration and returns its measurements. The
// readers and writers are the E1 ones: writes to a key are totally ordered,
// so any staleness is caused by the configuration under test.
func run(w workload) result {
	c := store.NewCluster(w.cfg)
	defer c.Close()
	if w.slow > 0 {
		c.Net.SetSlow("n3", w.slow)
	}
	or := newOracle()
	st := &stats{}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for k := 0; k < w.keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", k)
			cl := c.NewClient(fmt.Sprintf("w%d", k), w.q.R, w.q.W)
			cl.Sloppy = w.sloppy
			var ctx vclock.VC
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				v, err := cl.Put(key, fmt.Sprint(i), ctx)
				st.put(time.Since(t0), err == nil)
				if err == nil {
					ctx = v.Clock()
					or.ack(key, v)
				}
				if w.think > 0 {
					time.Sleep(w.think)
				}
			}
		}(k)
	}
	for r := 0; r < w.readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			var z *rand.Zipf
			if w.zipf {
				z = rand.NewZipf(rng, 1.2, 1, uint64(w.keys-1))
			}
			cl := c.NewClient(fmt.Sprintf("r%d", r), w.q.R, w.q.W)
			cl.Sloppy = w.sloppy
			for !stop.Load() {
				k := rng.Intn(w.keys)
				if z != nil {
					k = int(z.Uint64())
				}
				key := fmt.Sprintf("k%d", k)
				before := or.snapshot(key)
				t0 := time.Now()
				res, err := cl.Get(key)
				st.get(time.Since(t0), err == nil, stale(before, res.Versions), len(res.Versions))
			}
		}(r)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	r := result{st: st, msgs: c.Net.Sent(), syncKB: float64(c.SyncStats().Bytes) / 1024}
	for _, nd := range c.Nodes {
		r.hints += nd.HintCount()
	}
	return r
}

// spread is mean, min and max of repeated measurements.
type spread struct{ mean, lo, hi float64 }

// String formats a spread as "mean (lo-hi)".
func (s spread) String() string { return fmt.Sprintf("%.1f (%.1f-%.1f)", s.mean, s.lo, s.hi) }

// repeat runs f reps times with seeds 1..reps and summarises its values.
func repeat(f func(seed int64) float64) spread {
	s := spread{lo: math.Inf(1), hi: math.Inf(-1)}
	for i := 0; i < *reps; i++ {
		v := f(int64(i + 1))
		s.mean += v / float64(*reps)
		s.lo, s.hi = math.Min(s.lo, v), math.Max(s.hi, v)
	}
	return s
}

// metric extracts the numbers of interest from a result.
type metric func(result) float64

// measure runs w reps times (different latency seeds) and summarises m.
func measure(w workload, m metric) spread {
	return repeat(func(seed int64) float64 {
		w.cfg.Seed = seed
		return m(run(w))
	})
}

// column turns a slice of spreads into the three series a BarErr chart needs.
func column(ss []spread) (mean, lo, hi []float64) {
	for _, s := range ss {
		mean, lo, hi = append(mean, s.mean), append(lo, s.lo), append(hi, s.hi)
	}
	return
}

// report accumulates ablations.md.
var report strings.Builder

// out prints and records report text.
func out(format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	fmt.Print(s)
	report.WriteString(s)
}
