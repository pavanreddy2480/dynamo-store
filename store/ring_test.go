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

// Merkle anti-entropy converges on a fixed-partition ring exactly as it
// does on random vnodes.
func TestMerkleOnFixedRing(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		cfg := fastConfig(MerkleSync)
		cfg.Partitions = 256
		c := NewCluster(cfg)
		want := seedDivergent(t, seed, 300, c)
		c.AntiEntropy()
		checkConverged(t, c, want, fmt.Sprintf("fixed ring seed %d", seed))
		c.Close()
	}
}
