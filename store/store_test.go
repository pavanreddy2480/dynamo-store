package store

import (
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

func TestPartitionScenarios(t *testing.T) {
	for _, o := range []ScenarioOptions{
		{Policy: KeepSiblings, R: 1, W: 1},
		{Policy: KeepSiblings, R: 2, W: 2},
		{Policy: KeepSiblings, R: 2, W: 2, Sloppy: true},
		{Policy: LastWriterWins, R: 1, W: 1},
		{Policy: LastWriterWins, R: 1, W: 1, SkewMinority: 50 * time.Millisecond},
	} {
		r := RunPartitionScenario(o)
		if !r.RuleHolds {
			t.Errorf("%+v: rule violated\n%v", o, r.Log)
		}
		if o.Policy == KeepSiblings && len(r.Siblings) != 2 {
			t.Errorf("%+v: want 2 siblings after heal, got %d", o, len(r.Siblings))
		}
		if o.Policy == KeepSiblings && !r.AckedKept {
			t.Errorf("%+v: acknowledged write lost", o)
		}
		if o.Policy == LastWriterWins && r.AckedKept {
			t.Errorf("%+v: expected LWW to drop one acknowledged write", o)
		}
	}
}
