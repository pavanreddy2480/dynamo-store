package store

import (
	"fmt"
	"testing"
	"time"

	"dynamostore/vclock"
)

func TestMergeVersionsDropsDominated(t *testing.T) {
	r := rules{policy: KeepSiblings}
	v1 := Version{Value: "a", Dot: Dot{"n1", 1}, Ctx: vclock.VC{}}
	v2 := Version{Value: "b", Dot: Dot{"n1", 2}, Ctx: v1.Clock()}
	v3 := Version{Value: "c", Dot: Dot{"n2", 1}, Ctx: v1.Clock()}
	got := r.merge(nil, v1, v2, v3)
	if len(got) != 2 {
		t.Fatalf("want 2 siblings (b,c), got %v", got)
	}
	got = r.merge(got, v1) // stale replay is ignored
	if len(got) != 2 {
		t.Fatalf("stale version resurrected: %v", got)
	}
	lww := rules{policy: LastWriterWins}.merge(nil, Version{Value: "x", Dot: v2.Dot, Ctx: v2.Ctx, TS: 5}, Version{Value: "y", Dot: v3.Dot, Ctx: v3.Ctx, TS: 9})
	if len(lww) != 1 || lww[0].Value != "y" {
		t.Fatalf("lww kept %v", lww)
	}
}

// Two writes coordinated by the same node from the same (stale) context
// must stay concurrent with dotted version vectors, while plain per-node
// vector clocks wrongly order them.
func TestSameCoordinatorConcurrentWrites(t *testing.T) {
	base := vclock.VC{"n1": 1}
	a := Version{Value: "a", Dot: Dot{"n1", 2}, Ctx: base}
	b := Version{Value: "b", Dot: Dot{"n1", 3}, Ctx: base}
	if got := (rules{clocks: DottedVV}).merge(nil, a, b); len(got) != 2 {
		t.Fatalf("dotted VV lost a concurrent write: %v", got)
	}
	pa := Version{Value: "a", Dot: a.Dot, Ctx: a.Clock()}
	pb := Version{Value: "b", Dot: b.Dot, Ctx: b.Clock()}
	if got := (rules{clocks: PerNodeVC}).merge(nil, pa, pb); len(got) != 1 {
		t.Fatalf("expected per-node VC to (wrongly) order the writes, got %v", got)
	}
}

func TestReadYourWriteWithStrictQuorum(t *testing.T) {
	c := NewCluster(DefaultConfig())
	defer c.Close()
	cl := c.NewClient("c", 2, 2)
	var ctx vclock.VC
	for i := 0; i < 50; i++ {
		v, err := cl.Put("k", string(rune('a'+i%26)), ctx)
		if err != nil {
			t.Fatal(err)
		}
		ctx = v.Clock()
		got, err := cl.Get("k")
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Versions) != 1 || got.Versions[0].Value != v.Value {
			t.Fatalf("iteration %d: R+W>N read returned %v, want %q", i, got.Values(), v.Value)
		}
	}
}

func TestStrictQuorumUnavailableOnMinoritySide(t *testing.T) {
	c := NewCluster(DefaultConfig())
	defer c.Close()
	pref := c.PreferenceList("k")
	cl := c.NewClient("c", 2, 2)
	c.Net.Partition([]string{pref[0], "c"})
	if _, err := cl.Put("k", "v", nil); err == nil {
		t.Fatal("W=2 write with one reachable replica should fail")
	}
	// With a sloppy quorum the same side can borrow a fallback node.
	c.Net.Partition([]string{pref[0], c.Ring.Walk("k")[3], "c"})
	cl.Sloppy = true
	if _, err := cl.Put("k", "v", nil); err != nil {
		t.Fatalf("sloppy W=2 write with a live fallback should succeed: %v", err)
	}
}

// Hinted handoff must deliver any number of hints to a recovered owner. The
// old loop shared one 20ms deadline across every key of an owner, so with
// enough hinted keys the later sends always timed out and the hints were
// never pruned.
func TestHandoffDeliversManyHints(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Node.HandoffInterval = 5 * time.Millisecond
	c := NewCluster(cfg)
	defer c.Close()
	holder := c.Nodes["n5"]
	holder.mu.Lock()
	holder.hints["n1"] = map[string][]Version{}
	for i := 0; i < 150; i++ {
		holder.hints["n1"][fmt.Sprintf("k%d", i)] = []Version{{Value: "v", Dot: Dot{"n5", uint64(i + 1)}, Origin: "n5"}}
	}
	holder.mu.Unlock()
	if !c.WaitHintsDrained(5 * time.Second) {
		t.Fatalf("%d hints still undelivered after 5s", holder.HintCount())
	}
	if got := c.Nodes["n1"].Snapshot("k149"); len(got) != 1 {
		t.Fatalf("owner did not receive the hint: %v", got)
	}
}
