package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// A fixed ring gives every node an equal share of partitions; a join moves
// partitions only to the new node, and the node that leaves hands its
// partitions to the others, never reshuffling what stays.
func TestFixedRingJoinAndLeave(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4"}
	four := NewFixedRing(ids, 256)
	five := four.WithNode("n5")
	if len(five.tokens) != 256 {
		t.Fatalf("partition count changed: %d", len(five.tokens))
	}
	own := map[string]int{}
	for _, tok := range five.tokens {
		own[five.owner[tok]]++
		if o, n := four.owner[tok], five.owner[tok]; o != n && n != "n5" {
			t.Fatalf("partition moved between old nodes: %s -> %s", o, n)
		}
	}
	for id, c := range own {
		if c < 51 || c > 52 {
			t.Errorf("%s owns %d of 256 partitions, want 51 or 52", id, c)
		}
	}
	back := five.WithoutNode("n5")
	for _, tok := range back.tokens {
		if back.owner[tok] == "n5" {
			t.Fatal("n5 still owns a partition after leaving")
		}
		if five.owner[tok] != "n5" && five.owner[tok] != back.owner[tok] {
			t.Fatal("a partition not owned by the leaver changed owner")
		}
	}
}

// Merkle anti-entropy converges on the older random-vnode ring exactly as
// it does on the default fixed-partition ring (each token gap is a
// partition with its own tree).
func TestMerkleOnRandomVnodes(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		cfg := fastConfig(MerkleSync)
		cfg.Partitions = 0
		c := NewCluster(cfg)
		want := seedDivergent(t, seed, 300, c)
		syncRound(t, c)
		checkConverged(t, c, want, fmt.Sprintf("random vnodes seed %d", seed))
		checkTreesMatchData(t, c)
		c.Close()
	}
}

// A join changes only who owns some partitions; the partition a key falls
// in never changes, so per-partition trees stay valid across it.
func TestPartitionOfKeyStableAcrossJoin(t *testing.T) {
	four := NewFixedRing([]string{"n1", "n2", "n3", "n4"}, 256)
	five := four.WithNode("n5")
	for k := 0; k < 2000; k++ {
		key := fmt.Sprintf("user:%d", k)
		a, _ := four.Partition(key)
		b, _ := five.Partition(key)
		if a != b {
			t.Fatalf("%s moved from partition %d to %d", key, a, b)
		}
	}
}

// A node joining a running cluster takes its share (about 60% of the keys),
// gets
// its data by anti-entropy, and no acknowledged write is lost while clients
// keep writing during the join.
func TestAddNodeJoinsLive(t *testing.T) {
	cfg := fastConfig(MerkleSync)
	cfg.Nodes = 4
	cfg.Node.AntiEntropyInterval = 20 * time.Millisecond
	c := NewCluster(cfg)
	defer c.Close()
	cl := c.NewClient("w", 3, 3)
	acked := map[string]Version{}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%d", i)
		v, err := cl.Put(k, "v0", nil)
		if err != nil {
			t.Fatal(err)
		}
		acked[k] = v
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var mu sync.Mutex
	go func() { // keep writing through the join
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := fmt.Sprintf("k%d", i%200)
			g, _ := cl.Get(k)
			if v, err := cl.Put(k, fmt.Sprint("w", i), g.Context); err == nil {
				mu.Lock()
				acked[k] = v
				mu.Unlock()
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	id := c.AddNode()
	time.Sleep(600 * time.Millisecond)
	close(stop)
	<-done
	for r := 0; r < 3; r++ {
		syncRound(t, c)
	}
	held := 0
	for k, v := range acked {
		for _, n := range c.PreferenceList(k) {
			if !(rules{}).covers(c.Nodes[n].Snapshot(k), v) {
				t.Fatalf("%s lost on %s after join", k, n)
			}
			if n == id {
				held++
			}
		}
	}
	if held < 100 || held > 140 { // each of 5 nodes holds about 60% of the 200 keys
		t.Fatalf("new node holds %d of 200 keys, want about 120", held)
	}
}
