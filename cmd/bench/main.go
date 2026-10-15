// Command bench measures latency, availability, returned versions and
// stale-read frequency for different quorum settings, with and without
// network partitions. Results are printed and written to -out as Markdown.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
	"dynamostore/vclock"
)

var (
	dur    = flag.Duration("duration", 2*time.Second, "load duration per configuration")
	outDir = flag.String("out", "results", "directory for Markdown results")
)

// rw is one quorum setting.
type rw struct{ R, W int }

func (q rw) String() string { return fmt.Sprintf("R=%d W=%d", q.R, q.W) }

// sweep lists the quorum settings measured in E1.
var sweep = []rw{{1, 1}, {1, 2}, {2, 1}, {1, 3}, {3, 1}, {2, 2}, {2, 3}, {3, 2}, {3, 3}}

// oracle remembers, per key, the latest acknowledged writes (those not yet
// superseded by another acknowledged write).
type oracle struct {
	mu    sync.Mutex
	acked map[string][]store.Version
}

func newOracle() *oracle { return &oracle{acked: map[string][]store.Version{}} }

// ack records an acknowledged write and forgets the writes it supersedes.
func (o *oracle) ack(key string, v store.Version) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var kept []store.Version
	for _, e := range o.acked[key] {
		if !v.Covers(e) {
			kept = append(kept, e)
		}
	}
	o.acked[key] = append(kept, v)
}

// snapshot returns the acknowledged writes a read starting now must see.
func (o *oracle) snapshot(key string) []store.Version {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]store.Version(nil), o.acked[key]...)
}

// stale reports whether a read missed a write acknowledged before it began:
// some acknowledged write is neither returned nor superseded by a returned
// version.
func stale(before []store.Version, got []store.Version) bool {
	for _, a := range before {
		seen := false
		for _, v := range got {
			if v.Covers(a) {
				seen = true
				break
			}
		}
		if !seen {
			return true
		}
	}
	return false
}

// stats accumulates per-operation results for one configuration. It is
// shared by all client goroutines, hence the mutex.
type stats struct {
	mu                sync.Mutex
	getLat, putLat    []time.Duration
	gets, getOK       int
	puts, putOK       int
	staleReads        int
	siblingSum, multi int
	maxSiblings       int
}

// get records one read: latency, success, staleness and siblings returned.
func (s *stats) get(d time.Duration, ok bool, isStale bool, siblings int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if ok {
		s.getOK++
		s.getLat = append(s.getLat, d)
		if isStale {
			s.staleReads++
		}
		s.siblingSum += siblings
		if siblings > 1 {
			s.multi++
		}
		if siblings > s.maxSiblings {
			s.maxSiblings = siblings
		}
	}
}

// put records one write.
func (s *stats) put(d time.Duration, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if ok {
		s.putOK++
		s.putLat = append(s.putLat, d)
	}
}

// pct returns the p-th percentile of ds in milliseconds, or "-" if empty.
func pct(ds []time.Duration, p float64) string {
	if len(ds) == 0 {
		return "-"
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	d := c[int(p*float64(len(c)-1))]
	return fmt.Sprintf("%.2f", float64(d)/float64(time.Millisecond))
}

// ratio formats a/b as a percentage, or "-" if b is zero.
func ratio(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(a)/float64(b))
}

// ---------------------------------------------------------------- E1

// quorumSweep: one writer per key (so writes per key are totally ordered),
// many readers on the same hot keys, no failures.
func quorumSweep(q rw, readRepair bool, slowNode time.Duration) *stats {
	cfg := store.DefaultConfig()
	cfg.Node.ReadRepair = readRepair
	c := store.NewCluster(cfg)
	defer c.Close()
	if slowNode > 0 {
		c.Net.SetSlow("n3", slowNode)
	}
	const keys, readers = 32, 16
	or := newOracle()
	st := &stats{}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", k)
			cl := c.NewClient(fmt.Sprintf("w%d", k), q.R, q.W)
			var ctx vclock.VC
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				v, err := cl.Put(key, fmt.Sprint(i), ctx)
				st.put(time.Since(t0), err == nil)
				if err == nil {
					ctx = v.Clock()
					or.ack(key, v)
				}
			}
		}(k)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			cl := c.NewClient(fmt.Sprintf("r%d", r), q.R, q.W)
			for !stop.Load() {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
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
	return st
}

// ---------------------------------------------------------------- E2

// concResult summarises one concurrent-update run (E2).
type concResult struct {
	st         *stats
	ackedItems int
	lostItems  int
	finalVers  int
}

// concurrentUpdates: several clients do read-modify-write on ONE key at the
// same time, each adding unique items to a set.
func concurrentUpdates(q rw, policy store.Policy, clocks store.ClockMode) concResult {
	cfg := store.DefaultConfig()
	cfg.Node.Policy = policy
	cfg.Node.Clocks = clocks
	c := store.NewCluster(cfg)
	defer c.Close()
	const key, writers = "hot", 8
	st := &stats{}
	var mu sync.Mutex
	var acked []string
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("w%d", w), q.R, q.W)
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				res, err := cl.Get(key)
				st.get(time.Since(t0), err == nil, false, len(res.Versions))
				if err != nil {
					continue
				}
				item := fmt.Sprintf("w%d.%d", w, i)
				val := store.UnionResolver(append(res.Values(), item))
				t0 = time.Now()
				_, err = cl.Put(key, val, res.Context)
				st.put(time.Since(t0), err == nil)
				if err == nil {
					mu.Lock()
					acked = append(acked, item)
					mu.Unlock()
				}
			}
		}(w)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	c.AntiEntropy()
	reader := c.NewClient("final", 3, 3)
	res, _ := reader.Get(key)
	final := store.UnionResolver(res.Values())
	have := map[string]bool{}
	for _, it := range strings.Split(final, ",") {
		have[it] = true
	}
	lost := 0
	for _, it := range acked {
		if !have[it] {
			lost++
		}
	}
	return concResult{st: st, ackedItems: len(acked), lostItems: lost, finalVers: len(res.Versions)}
}

// ---------------------------------------------------------------- E3

// sideStats holds per-side results of a partition run (E3).
type sideStats struct {
	a, b             *stats
	keysWithSibs     int  // after healing and reconciliation-free read
	converged        bool // every replica of every key identical after heal
	lostAfterResolve int  // acked writes missing after union reconciliation
}

// partitioned: 5 nodes split {n1,n2} | {n3,n4,n5} with clients on both
// sides doing reads and read-modify-writes on shared keys.
func partitioned(q rw, sloppy bool) sideStats {
	c := store.NewCluster(store.DefaultConfig())
	defer c.Close()
	const keys, perSide = 64, 6
	or := newOracle()
	res := sideStats{a: &stats{}, b: &stats{}}
	sideA, sideB := []string{"n1", "n2"}, []string{"n3", "n4", "n5"}
	var clientsA, clientsB []string
	for i := 0; i < perSide; i++ {
		clientsA = append(clientsA, fmt.Sprintf("a%d", i))
		clientsB = append(clientsB, fmt.Sprintf("b%d", i))
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	run := func(cl *store.Client, st *stats, seed int64) {
		defer wg.Done()
		id := cl.ID
		rng := rand.New(rand.NewSource(seed))
		cl.Sloppy = sloppy
		for !stop.Load() {
			key := fmt.Sprintf("k%d", rng.Intn(keys))
			before := or.snapshot(key)
			t0 := time.Now()
			g, err := cl.Get(key)
			st.get(time.Since(t0), err == nil, stale(before, g.Versions), len(g.Versions))
			if rng.Intn(2) == 0 {
				continue
			}
			ctx := g.Context // nil after a failed read: a blind write
			t0 = time.Now()
			v, err := cl.Put(key, store.UnionResolver(append(g.Values(), id)), ctx)
			st.put(time.Since(t0), err == nil)
			if err == nil {
				or.ack(key, v)
			}
		}
	}
	var ca, cb []*store.Client
	for i := range clientsA { // register before partitioning
		ca = append(ca, c.NewClient(clientsA[i], q.R, q.W))
		cb = append(cb, c.NewClient(clientsB[i], q.R, q.W))
	}
	c.Net.Partition(append(append([]string{}, sideA...), clientsA...), append(append([]string{}, sideB...), clientsB...))
	for i := 0; i < perSide; i++ {
		wg.Add(2)
		go run(ca[i], res.a, int64(i))
		go run(cb[i], res.b, int64(100+i))
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()

	// Reconnect: hinted handoff + one anti-entropy round, then inspect.
	time.Sleep(50 * time.Millisecond)
	c.Net.Heal()
	c.WaitHintsDrained(time.Second)
	c.AntiEntropy()
	res.converged = true
	reader := c.NewClient("reader", 3, 3)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		var first string
		for i, id := range c.PreferenceList(key) {
			snap := fmt.Sprint(c.Nodes[id].Snapshot(key))
			if i == 0 {
				first = snap
			} else if snap != first {
				res.converged = false
			}
		}
		g, err := reader.Get(key)
		if err != nil {
			res.converged = false
			continue
		}
		if len(g.Versions) > 1 {
			res.keysWithSibs++
		}
		// Every write acknowledged on either side must still be visible.
		for _, a := range or.snapshot(key) {
			seen := false
			for _, v := range g.Versions {
				seen = seen || v.Covers(a)
			}
			if !seen {
				res.lostAfterResolve++
			}
		}
	}
	return res
}

// ---------------------------------------------------------------- main

func main() {
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	var md strings.Builder
	w := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		fmt.Print(s)
		md.WriteString(s)
	}
	w("# Benchmark results\n\n")
	w("5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3%% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. %v of load per row.\n\n", *dur)

	for _, slow := range []time.Duration{0, 3 * time.Millisecond} {
		if slow == 0 {
			w("## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)\n\n")
		} else {
			w("\n## E1b. Same sweep with one slow replica (n3: +%v per message)\n\n", slow)
		}
		w("| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |\n")
		w("|---|---|---|---|---|---|---|---|---|\n")
		for _, q := range sweep {
			on := quorumSweep(q, true, slow)
			off := quorumSweep(q, false, slow)
			w("| %d,%d | %v | %s | %s | %s | %s | %d | %s | %s |\n", q.R, q.W, q.R+q.W > 3,
				pct(on.getLat, .5), pct(on.getLat, .99), pct(on.putLat, .5), pct(on.putLat, .99),
				on.getOK, ratio(on.staleReads, on.getOK), ratio(off.staleReads, off.getOK))
		}
	}

	w("\n## E2. Concurrent read-modify-write on one key (8 writers)\n\n")
	w("| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |\n")
	w("|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, v := range []struct {
		p store.Policy
		m store.ClockMode
	}{{store.KeepSiblings, store.DottedVV}, {store.KeepSiblings, store.PerNodeVC}, {store.LastWriterWins, store.DottedVV}} {
		p, actor := v.p, v.m.String()
		for _, q := range []rw{{1, 1}, {2, 2}, {3, 3}} {
			r := concurrentUpdates(q, p, v.m)
			avg := 0.0
			if r.st.getOK > 0 {
				avg = float64(r.st.siblingSum) / float64(r.st.getOK)
			}
			w("| %s | %s | %d,%d | %s | %s | %d | %.2f | %s | %d | %d | %d |\n", p, actor, q.R, q.W,
				pct(r.st.getLat, .5), pct(r.st.putLat, .5), r.st.getOK, avg,
				ratio(r.st.multi, r.st.getOK), r.st.maxSiblings, r.ackedItems, r.lostItems)
		}
	}

	w("\n## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys\n\n")
	w("Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.\n\n")
	w("| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |\n")
	w("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, sloppy := range []bool{false, true} {
		mode := "strict"
		if sloppy {
			mode = "sloppy+hinted handoff"
		}
		for _, q := range []rw{{1, 1}, {1, 3}, {2, 2}, {3, 1}, {3, 3}} {
			r := partitioned(q, sloppy)
			w("| %s | %d,%d | %s | %s | %s | %s | %s | %s | %s | %s | %d/%d | %d | %v |\n", mode, q.R, q.W,
				ratio(r.a.getOK, r.a.gets), ratio(r.a.putOK, r.a.puts),
				ratio(r.b.getOK, r.b.gets), ratio(r.b.putOK, r.b.puts),
				pct(r.a.getLat, .5), pct(r.b.getLat, .5),
				ratio(r.a.staleReads, r.a.getOK), ratio(r.b.staleReads, r.b.getOK),
				r.keysWithSibs, 64, r.lostAfterResolve, r.converged)
		}
	}
	path := filepath.Join(*outDir, "benchmark.md")
	if err := os.WriteFile(path, []byte(md.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
