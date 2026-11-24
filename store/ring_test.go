package store

import (
	"fmt"
	"testing"
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
