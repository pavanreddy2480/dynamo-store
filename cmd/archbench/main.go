// Command archbench runs the experiments that justify individual
// architecture decisions (see ARCHITECTURE.md). Results go to
// results/architecture.md.
package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

var (
	dur    = flag.Duration("duration", 2*time.Second, "load duration per configuration")
	outDir = flag.String("out", "results", "output directory")
)

var md strings.Builder

func w(format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	fmt.Print(s)
	md.WriteString(s)
}

func nodes(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return ids
}

func fnv64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// ------------------------------------------------------------ A1 ring

// ringBalance places 100k keys and reports how evenly replicas spread.
func ringBalance() {
	w("## A1. Ring hash function and virtual nodes\n\n")
	w("100,000 keys, 5 nodes, N=3. Each node would hold 60%% of the keys if perfectly balanced. Max/min is the most-loaded node's share over the least-loaded node's share.\n\n")
	w("| hash | vnodes/node | min share | max share | max/min | coefficient of variation |\n|---|---|---|---|---|---|\n")
	const keys = 100000
	ids := nodes(5)
	for _, h := range []struct {
		name string
		f    func(string) uint64
	}{{"FNV-1a", fnv64}, {"MD5", nil}} {
		for _, vn := range []int{1, 8, 64, 256} {
			var r *store.Ring
			if h.f == nil {
				r = store.NewRing(ids, vn)
			} else {
				r = store.NewRingWithHash(ids, vn, h.f)
			}
			cnt := map[string]float64{}
			for k := 0; k < keys; k++ {
				for _, p := range r.PreferenceList(fmt.Sprintf("user:%d", k), 3) {
					cnt[p]++
				}
			}
			mn, mx, sum, sq := math.Inf(1), 0.0, 0.0, 0.0
			for _, id := range ids {
				c := cnt[id] / keys
				mn, mx = math.Min(mn, c), math.Max(mx, c)
				sum += c
				sq += c * c
			}
			mean := sum / 5
			cv := math.Sqrt(sq/5-mean*mean) / mean
			ratio := "∞"
			if mn > 0 {
				ratio = fmt.Sprintf("%.2f", mx/mn)
			}
			w("| %s | %d | %.1f%% | %.1f%% | %s | %.3f |\n", h.name, vn, 100*mn, 100*mx, ratio, cv)
		}
	}
}

// ------------------------------------------------------------ A2 replication

type rmwResult struct {
	acked, lost, siblings int
}

// rmw runs 8 concurrent read-modify-write clients on one key and counts
// acknowledged items missing from the final union.
func rmw(cfg store.Config, r, wq int) rmwResult {
	c := store.NewCluster(cfg)
	defer c.Close()
	var mu sync.Mutex
	var acked []string
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("w%d", i), r, wq)
			for j := 0; !stop.Load(); j++ {
				res, err := cl.Get("hot")
				if err != nil {
					continue
				}
				item := fmt.Sprintf("w%d.%d", i, j)
				if _, err := cl.Put("hot", store.UnionResolver(append(res.Values(), item)), res.Context); err == nil {
					mu.Lock()
					acked = append(acked, item)
					mu.Unlock()
				}
			}
		}(i)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	c.AntiEntropy()
	res, _ := c.NewClient("final", 3, 3).Get("hot")
	have := map[string]bool{}
	for _, it := range strings.Split(store.UnionResolver(res.Values()), ",") {
		have[it] = true
	}
	out := rmwResult{acked: len(acked), siblings: len(res.Versions)}
	for _, it := range acked {
		if !have[it] {
			out.lost++
		}
	}
	return out
}

func replication() {
	w("\n## A2. What a coordinator replicates, and how versions are compared\n\n")
	w("8 clients do concurrent read-modify-write on one key for %v, adding a unique item each time. \"Lost\" counts acknowledged items missing after a final union merge. Each row is the sum over 3 runs.\n\n", *dur)
	w("| clocks | replication payload | R,W | acked items | lost items |\n|---|---|---|---|---|\n")
	for _, v := range []struct {
		clocks  store.ClockMode
		newOnly bool
	}{{store.DottedVV, false}, {store.DottedVV, true}, {store.PerNodeVC, false}} {
		payload := "whole sibling set"
		if v.newOnly {
			payload = "new version only"
		}
		for _, q := range [][2]int{{1, 1}, {2, 2}} {
			var tot rmwResult
			for run := 0; run < 3; run++ {
				cfg := store.DefaultConfig()
				cfg.Seed = int64(run + 1)
				cfg.Node.Clocks = v.clocks
				cfg.Node.ReplicateNewOnly = v.newOnly
				r := rmw(cfg, q[0], q[1])
				tot.acked += r.acked
				tot.lost += r.lost
			}
			w("| %s | %s | %d,%d | %d | %d (%.2f%%) |\n", v.clocks, payload, q[0], q[1], tot.acked, tot.lost, 100*float64(tot.lost)/float64(tot.acked))
		}
	}
}

// ------------------------------------------------------------ A3 repair

type repairMode struct {
	name                 string
	handoff, rr, entropy bool
}

// convergence: write through a partition with sloppy quorums, heal, apply
// only some repair mechanisms, then count acknowledged writes that are not
// yet on all three preferred replicas.
func convergence(m repairMode) (acked, under int, divergent int) {
	cfg := store.DefaultConfig()
	if !m.handoff {
		cfg.Node.HandoffInterval = 0
	}
	cfg.Node.ReadRepair = m.rr
	c := store.NewCluster(cfg)
	defer c.Close()
	const keys = 64
	var mu sync.Mutex
	var versions = map[string][]store.Version{}
	var clients []*store.Client
	var sideA, sideB []string
	for i := 0; i < 4; i++ {
		a := c.NewClient(fmt.Sprintf("a%d", i), 1, 2)
		b := c.NewClient(fmt.Sprintf("b%d", i), 1, 2)
		a.Sloppy, b.Sloppy = true, true
		clients = append(clients, a, b)
		sideA, sideB = append(sideA, a.ID), append(sideB, b.ID)
	}
	c.Net.Partition(append([]string{"n1", "n2"}, sideA...), append([]string{"n3", "n4", "n5"}, sideB...))
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				g, _ := cl.Get(key)
				v, err := cl.Put(key, store.UnionResolver(append(g.Values(), cl.ID)), g.Context)
				if err == nil {
					mu.Lock()
					versions[key] = append(versions[key], v)
					mu.Unlock()
				}
			}
		}(cl, int64(i))
	}
	time.Sleep(*dur / 2)
	stop.Store(true)
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	c.Net.Heal()
	if m.handoff {
		c.WaitHintsDrained(time.Second)
	}
	if m.rr {
		reader := c.NewClient("reader", 3, 3)
		for k := 0; k < keys; k++ {
			reader.Get(fmt.Sprintf("k%d", k))
		}
		time.Sleep(100 * time.Millisecond) // let repairs land
	}
	if m.entropy {
		c.AntiEntropy()
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		var first string
		div := false
		snaps := map[string][]store.Version{}
		for i, id := range c.PreferenceList(key) {
			snaps[id] = c.Nodes[id].Snapshot(key)
			s := fmt.Sprint(snaps[id])
			if i == 0 {
				first = s
			} else if s != first {
				div = true
			}
		}
		if div {
			divergent++
		}
		for _, a := range versions[key] {
			acked++
			for _, snap := range snaps {
				ok := false
				for _, s := range snap {
					ok = ok || s.Covers(a)
				}
				if !ok {
					under++
					break
				}
			}
		}
	}
	return
}

func repair() {
	w("\n## A3. Repair mechanisms after a partition heals\n\n")
	w("Sloppy R=1 W=2 writes on both sides of a {n1,n2} | {n3,n4,n5} partition for %v, then heal. \"Under-replicated\" means an acknowledged write not yet on all 3 preferred replicas. \"Divergent\" means the 3 replicas of a key disagree.\n\n", *dur/2)
	w("| repair after heal | acked writes | under-replicated | divergent keys (of 64) |\n|---|---|---|---|\n")
	for _, m := range []repairMode{
		{"none", false, false, false},
		{"hinted handoff only", true, false, false},
		{"read repair only (one R=3 read per key)", false, true, false},
		{"anti-entropy only (one round)", false, false, true},
		{"hinted handoff + anti-entropy (default)", true, false, true},
	} {
		a, u, d := convergence(m)
		w("| %s | %d | %d (%.1f%%) | %d |\n", m.name, a, u, 100*float64(u)/float64(a), d)
	}
}

// ------------------------------------------------------------ A4 coordinator

func coordinatorChoice() {
	w("\n## A4. Choosing the coordinator\n\n")
	w("Same workload as E1 (32 keys, one writer each, 16 readers), R=1 W=1, for %v. Load share is each node's fraction of coordinated requests; perfect balance is 20%%.\n\n", *dur)
	w("| coordinator | stale reads | coordinator load share n1..n5 | busiest node |\n|---|---|---|---|\n")
	for _, first := range []bool{true, false} {
		c := store.NewCluster(store.DefaultConfig())
		const keys = 32
		last := make([]atomic.Pointer[store.Version], keys)
		var stop atomic.Bool
		var wg sync.WaitGroup
		var reads, stale atomic.Int64
		for k := 0; k < keys; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				cl := c.NewClient(fmt.Sprintf("w%d", k), 1, 1)
				cl.FirstCoordinator = first
				var ctx map[string]uint64
				for !stop.Load() {
					v, err := cl.Put(fmt.Sprintf("k%d", k), "x", ctx)
					if err == nil {
						ctx = v.Clock()
						last[k].Store(&v)
					}
				}
			}(k)
		}
		for r := 0; r < 16; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(r)))
				cl := c.NewClient(fmt.Sprintf("r%d", r), 1, 1)
				cl.FirstCoordinator = first
				for !stop.Load() {
					k := rng.Intn(keys)
					before := last[k].Load()
					g, err := cl.Get(fmt.Sprintf("k%d", k))
					if err != nil {
						continue
					}
					reads.Add(1)
					if before != nil {
						seen := false
						for _, v := range g.Versions {
							seen = seen || v.Covers(*before)
						}
						if !seen {
							stale.Add(1)
						}
					}
				}
			}(r)
		}
		time.Sleep(*dur)
		stop.Store(true)
		wg.Wait()
		var total int64
		for _, id := range c.IDs {
			total += c.Nodes[id].Coordinated()
		}
		var shares []string
		busiest := 0.0
		for _, id := range c.IDs {
			s := float64(c.Nodes[id].Coordinated()) / float64(total)
			busiest = math.Max(busiest, s)
			shares = append(shares, fmt.Sprintf("%.0f%%", 100*s))
		}
		c.Close()
		name := "random preferred replica (default)"
		if first {
			name = "always first preferred replica"
		}
		w("| %s | %.1f%% | %s | %.0f%% |\n", name, 100*float64(stale.Load())/float64(reads.Load()), strings.Join(shares, " / "), 100*busiest)
	}
}

// ------------------------------------------------------------ A5 timeout

func timeouts() {
	w("\n## A5. Replica timeout\n\n")
	w("How long a coordinator waits for replicas. Strict R=W=2, 64 keys, partition {n1,n2} | {n3,n4,n5}, 6 clients per side, %v. A failed operation costs the full timeout.\n\n", *dur/2)
	w("| replica timeout | no partition: put p99 ms | no partition: put failures | partitioned: failed-op latency p50 ms | partitioned: put availability |\n|---|---|---|---|---|\n")
	for _, to := range []time.Duration{2 * time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond} {
		healthy := loadWith(to, false)
		part := loadWith(to, true)
		w("| %v | %.2f | %.2f%% | %.2f | %.1f%% |\n", to, healthy.p99ok, 100*healthy.failRate, part.p50fail, 100*(1-part.failRate))
	}
}

type loadStats struct{ p99ok, p50fail, failRate float64 }

func loadWith(timeout time.Duration, partition bool) loadStats {
	cfg := store.DefaultConfig()
	cfg.Node.ReplicaTimeout = timeout
	c := store.NewCluster(cfg)
	defer c.Close()
	var clients []*store.Client
	var a, b []string
	for i := 0; i < 6; i++ {
		ca, cb := c.NewClient(fmt.Sprintf("a%d", i), 2, 2), c.NewClient(fmt.Sprintf("b%d", i), 2, 2)
		clients = append(clients, ca, cb)
		a, b = append(a, ca.ID), append(b, cb.ID)
	}
	if partition {
		c.Net.Partition(append([]string{"n1", "n2"}, a...), append([]string{"n3", "n4", "n5"}, b...))
	}
	var mu sync.Mutex
	var ok, fail []time.Duration
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				t0 := time.Now()
				_, err := cl.Put(fmt.Sprintf("k%d", rng.Intn(64)), "v", nil)
				d := time.Since(t0)
				mu.Lock()
				if err == nil {
					ok = append(ok, d)
				} else {
					fail = append(fail, d)
				}
				mu.Unlock()
			}
		}(cl, int64(i))
	}
	time.Sleep(*dur / 2)
	stop.Store(true)
	wg.Wait()
	return loadStats{
		p99ok:    pctMs(ok, .99),
		p50fail:  pctMs(fail, .5),
		failRate: float64(len(fail)) / float64(len(ok)+len(fail)),
	}
}

func pctMs(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return math.NaN()
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return float64(c[int(p*float64(len(c)-1))]) / float64(time.Millisecond)
}

func main() {
	flag.Parse()
	w("# Architecture experiments\n\n")
	w("Default cluster unless stated: 5 nodes, N=3, replica-to-replica latency 0.4ms + Exp(0.8ms) with 3%% of messages +5ms, client hop 0.1ms + Exp(0.1ms), replica timeout 20ms. Regenerate with `go run ./cmd/archbench`.\n\n")
	ringBalance()
	replication()
	repair()
	coordinatorChoice()
	timeouts()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	path := filepath.Join(*outDir, "architecture.md")
	if err := os.WriteFile(path, []byte(md.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
