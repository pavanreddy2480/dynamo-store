package store

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dynamostore/vclock"
)

// merkleModes lists every Merkle-based SyncMode; each property below must
// hold for all of them.
var merkleModes = []SyncMode{MerkleSync}

// randomReplicas plays a random write history for one key the way the
// store does, but without a network, and returns each replica's final
// sibling set plus every version written.
//
// Each write: a client reads one random replica (or nothing) and uses its
// clock as context; a random replica coordinates, issuing a dot above any
// counter it has seen; it then copies its whole sibling set to a random
// subset of the other replicas, as coordPut does. Lost or missing copies
// leave the replicas diverged, with stale values and concurrent siblings.
func randomReplicas(rng *rand.Rand, nodes []string) (map[string][]Version, []Version) {
	r := rules{}
	sets := map[string][]Version{}
	issued := map[string]uint64{}
	var all []Version
	for w := 0; w < 1+rng.Intn(6); w++ {
		ctx := vclock.VC{}
		if rd := rng.Intn(len(nodes) + 1); rd < len(nodes) {
			for _, v := range sets[nodes[rd]] {
				ctx = vclock.Merge(ctx, v.Clock())
			}
		}
		c := nodes[rng.Intn(len(nodes))]
		floor := issued[c]
		for _, v := range append(sets[c], Version{Ctx: ctx}) {
			if n := v.Clock()[c]; n > floor {
				floor = n
			}
		}
		issued[c] = floor + 1
		v := Version{Value: fmt.Sprintf("v%d", w), Dot: Dot{c, floor + 1}, Ctx: ctx, TS: int64(w)}
		all = append(all, v)
		sets[c] = r.merge(sets[c], v)
		for _, o := range nodes {
			if o != c && rng.Intn(2) == 0 {
				sets[o] = r.merge(sets[o], cloneVersions(sets[c])...)
			}
		}
	}
	return sets, all
}

// seedDivergent writes the same random divergent state into every given
// cluster, straight into replica state (no network).
//
// Returns:
//   - map[string][]Version: per key, the merge of every replica's set,
//     i.e. the state every replica must reach. It is checked to cover
//     every version ever written, so reaching it loses no write.
func seedDivergent(t *testing.T, seed int64, keys int, clusters ...*Cluster) map[string][]Version {
	rng := rand.New(rand.NewSource(seed))
	want := map[string][]Version{}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		sets, all := randomReplicas(rng, clusters[0].PreferenceList(key))
		for id, vs := range sets {
			want[key] = rules{}.merge(want[key], cloneVersions(vs)...)
			for _, c := range clusters {
				nd := c.Nodes[id]
				nd.mu.Lock()
				nd.put(key, cloneVersions(vs))
				nd.mu.Unlock()
			}
		}
		for _, v := range all {
			if !(rules{}).covers(want[key], v) {
				t.Fatalf("seed %d: generator lost write %v of %s", seed, v.Dot, key)
			}
		}
	}
	return want
}

// checkConverged fails t unless every replica of every key holds exactly
// want[key].
func checkConverged(t *testing.T, c *Cluster, want map[string][]Version, label string) {
	t.Helper()
	for key, w := range want {
		for _, id := range c.PreferenceList(key) {
			if got := c.Nodes[id].Snapshot(key); fmt.Sprint(got) != fmt.Sprint(w) {
				t.Fatalf("%s: %s on %s = %v, want %v", label, key, id, got, w)
			}
		}
	}
}

// checkTreesMatchData fails t unless every node's per-partition Merkle
// trees equal trees rebuilt from scratch from its data, i.e. incremental
// updates never drift.
func checkTreesMatchData(t *testing.T, c *Cluster) {
	t.Helper()
	for _, nd := range c.Nodes {
		nd.mu.Lock()
		fresh := map[int]*merkle{}
		for k, vs := range nd.data {
			p, h := nd.ring.Partition(k)
			if !contains(nd.ring.PartitionList(p, nd.cfg.N), nd.ID) {
				continue
			}
			if fresh[p] == nil {
				fresh[p] = newMerkle(nd.cfg.MerkleDepth)
			}
			fresh[p].set(k, h, hashVersions(k, vs))
		}
		for p, tr := range nd.trees {
			if tr.h[1] != fresh[p].hash(1) {
				t.Errorf("%s: tree of partition %d drifted from its data", nd.ID, p)
			}
		}
		nd.mu.Unlock()
	}
}

// fastConfig is DefaultConfig with a quiet network (no heavy tail) and no
// background handoff, so anti-entropy is the only repair that runs.
func fastConfig(mode SyncMode) Config {
	cfg := DefaultConfig()
	cfg.Node.AntiEntropy = mode
	cfg.Node.HandoffInterval = 0
	cfg.Latency = LatencyModel{Base: 50 * time.Microsecond}
	cfg.Client = cfg.Latency
	// Anti-entropy is the only traffic, but -race can stall a goroutine for
	// tens of milliseconds; a timed-out exchange would look like a bug.
	cfg.Node.ReplicaTimeout = 5 * time.Second
	return cfg
}

// syncRound runs one anti-entropy round like Cluster.AntiEntropy but fails
// t on any exchange error instead of ignoring it.
func syncRound(t *testing.T, c *Cluster) {
	t.Helper()
	for i, a := range c.IDs {
		for _, b := range c.IDs[i+1:] {
			if err := c.Nodes[a].syncWith(b); err != nil {
				t.Fatalf("sync %s-%s: %v", a, b, err)
			}
		}
	}
}

// Property: from any random divergent state, one Merkle anti-entropy round
// leaves every replica with the merge of all versions ever written, which
// is exactly what full-state sync produces from the same state. A second
// round then finds nothing to do and sends no keys.
func TestMerkleSyncMatchesFullStateSync(t *testing.T) {
	for _, mode := range merkleModes {
		for seed := int64(1); seed <= 25; seed++ {
			full, mk := NewCluster(fastConfig(FullStateSync)), NewCluster(fastConfig(mode))
			want := seedDivergent(t, seed, 300, full, mk)
			checkTreesMatchData(t, mk)
			syncRound(t, full)
			syncRound(t, mk)
			label := fmt.Sprintf("mode %d seed %d", mode, seed)
			checkConverged(t, full, want, label+" full-state")
			checkConverged(t, mk, want, label+" merkle")
			checkTreesMatchData(t, mk)
			before := mk.SyncStats()
			syncRound(t, mk)
			after := mk.SyncStats()
			if after.Keys != before.Keys || after.RoundTrips-before.RoundTrips != 10 {
				t.Fatalf("%s: converged round sent %d keys in %d round trips, want 0 keys in 10 (one per pair)",
					label, after.Keys-before.Keys, after.RoundTrips-before.RoundTrips)
			}
			full.Close()
			mk.Close()
		}
	}
}

// End to end: clients write on both sides of a partition through sloppy
// quorums with hinted handoff off, so after the heal only Merkle
// anti-entropy can repair the replicas. Every replica must end equal, and
// every acknowledged write must be on all three replicas of its key.
func TestMerkleRepairsPartition(t *testing.T) {
	for _, mode := range merkleModes {
		cfg := DefaultConfig()
		cfg.Node.AntiEntropy = mode
		cfg.Node.HandoffInterval = 0
		c := NewCluster(cfg)
		var mu sync.Mutex
		acked := map[string][]Version{}
		var stop atomic.Bool
		var wg sync.WaitGroup
		sides := [][]string{{"n1", "n2"}, {"n3", "n4", "n5"}}
		for i := 0; i < 6; i++ {
			cl := c.NewClient(fmt.Sprintf("c%d", i), 1, 2)
			cl.Sloppy = true
			sides[i%2] = append(sides[i%2], cl.ID)
			wg.Add(1)
			go func(cl *Client, rng *rand.Rand) {
				defer wg.Done()
				for !stop.Load() {
					key := fmt.Sprintf("k%d", rng.Intn(100))
					g, _ := cl.Get(key)
					if v, err := cl.Put(key, cl.ID, g.Context); err == nil {
						mu.Lock()
						acked[key] = append(acked[key], v)
						mu.Unlock()
					}
				}
			}(cl, rand.New(rand.NewSource(int64(i))))
		}
		c.Net.Partition(sides...)
		time.Sleep(300 * time.Millisecond)
		stop.Store(true)
		wg.Wait()
		time.Sleep(50 * time.Millisecond)
		c.Net.Heal()
		// Hints parked on fallbacks are not part of the owners' trees;
		// deliver them once by hand so the test isolates anti-entropy
		// between owners (the background loop is off).
		for _, nd := range c.Nodes {
			nd.handoffHints()
		}
		for round := 0; round < 3; round++ {
			c.AntiEntropy()
		}
		want := map[string][]Version{}
		for key := range acked {
			var all []Version
			for _, id := range c.PreferenceList(key) {
				all = append(all, c.Nodes[id].Snapshot(key)...)
			}
			want[key] = rules{}.merge(nil, all...)
			for _, a := range acked[key] {
				if !(rules{}).covers(want[key], a) {
					t.Fatalf("mode %d: acknowledged write %v of %s lost", mode, a.Dot, key)
				}
			}
		}
		checkConverged(t, c, want, fmt.Sprintf("mode %d", mode))
		c.Close()
	}
}

// The one-shot tree fetch must converge exactly like the level-by-level
// walk, and the periodic loop must converge a diverged cluster by itself.
func TestMerkleOneShotAndPeriodic(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		cfg := fastConfig(MerkleSync)
		cfg.Node.MerkleOneShot = true
		c := NewCluster(cfg)
		want := seedDivergent(t, seed, 300, c)
		syncRound(t, c)
		checkConverged(t, c, want, fmt.Sprintf("one-shot seed %d", seed))
		c.Close()
	}
	cfg := fastConfig(MerkleSync)
	cfg.Node.AntiEntropyInterval = 5 * time.Millisecond
	c := NewCluster(cfg)
	defer c.Close()
	want := seedDivergent(t, 9, 100, c)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for key, w := range want {
			for _, id := range c.PreferenceList(key) {
				ok = ok && fmt.Sprint(c.Nodes[id].Snapshot(key)) == fmt.Sprint(w)
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic anti-entropy did not converge in 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
