package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

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

// A node joining a running cluster takes its share (about 60% of the keys),
// gets its data by anti-entropy, and no acknowledged write is lost while clients
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
