package store

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// UnionResolver treats values as comma-separated sets (a shopping cart) and
// returns their union. It is the explicit, declared resolution rule for the
// sibling policy: no acknowledged item is ever lost.
func UnionResolver(values []string) string {
	set := map[string]bool{}
	for _, v := range values {
		for _, it := range strings.Split(v, ",") {
			if it != "" {
				set[it] = true
			}
		}
	}
	items := make([]string, 0, len(set))
	for it := range set {
		items = append(items, it)
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

// ScenarioOptions configures RunPartitionScenario.
type ScenarioOptions struct {
	Policy Policy
	R, W   int
	Sloppy bool
	// SkewMinority shifts the minority side coordinator's clock; with LWW a
	// positive skew lets the earlier write win.
	SkewMinority time.Duration
	Seed         int64
}

// ScenarioResult reports what happened and whether the rule held.
type ScenarioResult struct {
	Opts         ScenarioOptions
	Key          string
	PrefList     []string
	SideA, SideB []string
	WriteA       string
	WriteB       string
	ErrA, ErrB   error
	Siblings     []Version // returned by an R=N read after healing
	Final        string    // value after applying the resolution rule
	Expected     string    // what the declared rule says the value must be
	Converged    bool      // every replica holds exactly one, identical version
	RuleHolds    bool
	AckedKept    bool // every acknowledged item survives in the final value
	Log          []string
}

// logf appends one line to the scenario's narrative log.
func (r *ScenarioResult) logf(format string, a ...any) {
	r.Log = append(r.Log, fmt.Sprintf(format, a...))
}

// RunPartitionScenario partitions the cluster so the key's replicas are
// split 1 | 2, updates the same key on both sides, heals the partition and
// checks the declared resolution rule.
//
// The steps are: write {milk} to all three replicas; have both clients read
// it; partition; write {eggs,milk} on side A and {bread,milk} on side B;
// heal and run hinted handoff plus anti-entropy; read with R=N; apply the
// rule; and finally check every replica holds one identical version.
func RunPartitionScenario(o ScenarioOptions) ScenarioResult {
	cfg := DefaultConfig()
	cfg.Node.Policy = o.Policy
	if o.Seed != 0 {
		cfg.Seed = o.Seed
	}
	c := NewCluster(cfg)
	defer c.Close()

	const key = "cart:alice"
	res := ScenarioResult{Opts: o, Key: key}
	walk := c.Ring.Walk(key)
	pref, others := walk[:3], walk[3:]
	res.PrefList = pref

	admin := c.NewClient("admin", 3, 3)
	a := c.NewClient("clientA", o.R, o.W)
	b := c.NewClient("clientB", o.R, o.W)
	a.Sloppy, b.Sloppy = o.Sloppy, o.Sloppy

	if _, err := admin.Put(key, "milk", nil); err != nil {
		res.logf("initial write failed: %v", err)
		return res
	}
	res.logf("initial write {milk} acknowledged by all of %v", pref)

	// Both clients read before the partition, so both writes below descend
	// from {milk} and are concurrent with each other.
	ra, _ := a.Get(key)
	rb, _ := b.Get(key)

	res.SideA = []string{pref[0], others[0], "clientA"}
	res.SideB = []string{pref[1], pref[2], others[1], "clientB"}
	c.Net.Partition(res.SideA, res.SideB)
	res.logf("partition: A=%v | B=%v", res.SideA, res.SideB)
	if o.SkewMinority != 0 {
		c.SetSkew(pref[0], o.SkewMinority)
		c.SetSkew(others[0], o.SkewMinority)
		res.logf("minority side clocks skewed by %v", o.SkewMinority)
	}

	res.WriteA = UnionResolver([]string{ra.Values()[0], "eggs"})
	verA, errA := a.Put(key, res.WriteA, ra.Context)
	res.ErrA = errA
	time.Sleep(2 * time.Millisecond) // B writes strictly later in real time
	res.WriteB = UnionResolver([]string{rb.Values()[0], "bread"})
	verB, errB := b.Put(key, res.WriteB, rb.Context)
	res.ErrB = errB
	res.logf("side A put {%s}: %s", res.WriteA, errStr(res.ErrA))
	res.logf("side B put {%s}: %s", res.WriteB, errStr(res.ErrB))

	time.Sleep(3 * cfg.Node.ReplicaTimeout) // let in-flight replication finish
	c.Net.Heal()
	hinted := c.WaitHintsDrained(500 * time.Millisecond)
	c.AntiEntropy()
	res.logf("healed; hinted handoff drained=%v; anti-entropy round done", hinted)

	reader := c.NewClient("reader", 3, 3)
	got, err := reader.Get(key)
	if err != nil {
		res.logf("post-heal read failed: %v", err)
		return res
	}
	res.Siblings = got.Versions
	for _, v := range got.Versions {
		res.logf("  returned version {%s} clock=%v", v.Value, v.Clock())
	}

	// The declared rule.
	var ackedVals []string
	switch o.Policy {
	case KeepSiblings:
		// Rule: the reconciled value is the set union of every sibling
		// returned, and it must contain every acknowledged write.
		res.Expected = UnionResolver(got.Values())
		acked := []string{"milk"}
		for _, w := range []struct {
			val string
			err error
		}{{res.WriteA, res.ErrA}, {res.WriteB, res.ErrB}} {
			inSiblings := false
			for _, v := range got.Values() {
				inSiblings = inSiblings || v == w.val
			}
			if w.err == nil {
				acked = append(acked, w.val)
			} else if inSiblings {
				res.logf("note: put {%s} reported failure but reached a replica; quorum failure is not a rollback, so it resurfaces as a sibling", w.val)
			}
		}
		res.AckedKept = isSubset(UnionResolver(acked), res.Expected)
		final, n, err := reader.GetResolved(key, UnionResolver)
		if err != nil {
			res.logf("reconcile write failed: %v", err)
			return res
		}
		res.logf("client reconciled %d siblings by set union -> {%s}", n, final)
	case LastWriterWins:
		// The acknowledged write with the highest (timestamp, origin) wins.
		var acked []Version
		ackedVals = []string{"milk"}
		if errA == nil {
			acked = append(acked, verA)
			ackedVals = append(ackedVals, verA.Value)
		}
		if errB == nil {
			acked = append(acked, verB)
			ackedVals = append(ackedVals, verB.Value)
		}
		res.Expected = "milk"
		if len(acked) > 0 {
			win := acked[0]
			for _, v := range acked[1:] {
				if (rules{policy: LastWriterWins}).lwwBeats(v, win) {
					win = v
				}
			}
			res.Expected = win.Value
			for _, v := range acked {
				if v.Value != win.Value {
					res.logf("LWW discarded acknowledged write {%s} (ts %d < %d)", v.Value, v.TS, win.TS)
				}
			}
		}
	}

	time.Sleep(3 * cfg.Node.ReplicaTimeout)
	c.AntiEntropy()
	final, _ := reader.Get(key)
	if len(final.Versions) == 1 {
		res.Final = final.Versions[0].Value
	}
	res.Converged = true
	for _, id := range pref {
		snap := c.Nodes[id].Snapshot(key)
		if len(snap) != 1 || snap[0].Value != res.Final {
			res.Converged = false
		}
		res.logf("  replica %s holds %s", id, describe(snap))
	}
	res.RuleHolds = res.Converged && res.Final == res.Expected
	if o.Policy == KeepSiblings {
		res.RuleHolds = res.RuleHolds && res.AckedKept
	} else {
		res.AckedKept = isSubset(UnionResolver(ackedVals), res.Final)
	}
	res.logf("final {%s}, expected by rule {%s}, converged=%v -> rule holds=%v",
		res.Final, res.Expected, res.Converged, res.RuleHolds)
	return res
}

// errStr renders a put outcome for the log.
func errStr(err error) string {
	if err == nil {
		return "ok"
	}
	return "FAILED (" + err.Error() + ")"
}

// describe renders a sibling set as {value}[clock] pairs.
func describe(vs []Version) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprintf("{%s}%v", v.Value, v.Clock())
	}
	return strings.Join(parts, " ")
}

// isSubset reports whether every item of comma-separated set a is in b.
func isSubset(a, b string) bool {
	have := map[string]bool{}
	for _, it := range strings.Split(b, ",") {
		have[it] = true
	}
	for _, it := range strings.Split(a, ",") {
		if it != "" && !have[it] {
			return false
		}
	}
	return true
}
