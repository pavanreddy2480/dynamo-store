package store

import (
	"sort"

	"dynamostore/vclock"
)

// Dot names one update: the Counter-th write coordinated by Node for a key.
type Dot struct {
	Node    string
	Counter uint64
}

// Version is one value of a key together with its causal history.
//
// Causality is tracked with a dotted version vector: Dot identifies this
// write and Ctx is the vector clock of everything the writer had seen when
// it wrote (the context returned by its last Get). Clock() = Ctx ∪ Dot is
// the plain vector clock of the version.
type Version struct {
	Value  string
	Dot    Dot
	Ctx    vclock.VC
	TS     int64  // coordinator wall-clock time in ns; used only by LWW
	Origin string // coordinating node; LWW tie-breaker
}

// Clock returns the version's vector clock.
func (v Version) Clock() vclock.VC {
	c := v.Ctx.Copy()
	if c[v.Dot.Node] < v.Dot.Counter {
		c[v.Dot.Node] = v.Dot.Counter
	}
	return c
}

// Covers reports whether v's writer had seen w (or v is w): w's dot is in
// v's causal context.
func (v Version) Covers(w Version) bool {
	return v.Dot == w.Dot || v.Ctx[w.Dot.Node] >= w.Dot.Counter
}

// Policy decides what a replica keeps when versions are concurrent.
type Policy int

const (
	// KeepSiblings keeps every concurrent version and returns them all to
	// the client, which must reconcile them explicitly (Dynamo's approach).
	KeepSiblings Policy = iota
	// LastWriterWins keeps only the version with the highest timestamp
	// (Cassandra-style); concurrent updates are silently discarded.
	LastWriterWins
)

func (p Policy) String() string {
	if p == LastWriterWins {
		return "last-writer-wins"
	}
	return "siblings+client-reconcile"
}

// ClockMode selects how versions are compared.
type ClockMode int

const (
	// DottedVV compares versions by dot-in-context. Two writes coordinated
	// by the same node from the same stale context stay concurrent.
	DottedVV ClockMode = iota
	// PerNodeVC is the scheme in the Dynamo paper: the coordinator bumps its
	// own entry and versions are ordered by plain vector-clock comparison.
	// Concurrent writes through one coordinator then look ordered, so one
	// silently overwrites the other.
	PerNodeVC
)

func (m ClockMode) String() string {
	if m == PerNodeVC {
		return "per-node vector clock"
	}
	return "dotted version vector"
}

type rules struct {
	policy Policy
	clocks ClockMode
}

// obsoletes reports whether a supersedes b.
func (r rules) obsoletes(a, b Version) bool {
	if a.Dot == b.Dot {
		return false
	}
	if r.clocks == PerNodeVC {
		return vclock.Compare(a.Clock(), b.Clock()) == vclock.After
	}
	return a.Covers(b)
}

// same reports whether a and b are indistinguishable to the store.
func (r rules) same(a, b Version) bool {
	if r.clocks == PerNodeVC {
		return vclock.Compare(a.Clock(), b.Clock()) == vclock.Equal
	}
	return a.Dot == b.Dot
}

// lwwBeats reports whether a wins over b under last-writer-wins.
func (r rules) lwwBeats(a, b Version) bool {
	switch {
	case r.same(a, b) || r.obsoletes(b, a):
		return false
	case r.obsoletes(a, b):
		return true
	case a.TS != b.TS:
		return a.TS > b.TS
	}
	return a.Origin > b.Origin
}

// merge folds incoming into set and returns the new set. Versions
// superseded by another version are dropped.
func (r rules) merge(set []Version, incoming ...Version) []Version {
	for _, v := range incoming {
		if r.policy == LastWriterWins {
			if len(set) == 0 || r.lwwBeats(v, set[0]) {
				set = []Version{v}
			}
			continue
		}
		dominated := false
		kept := set[:0:0]
		for _, e := range set {
			switch {
			case r.same(e, v) || r.obsoletes(e, v):
				dominated = true
				kept = append(kept, e)
			case r.obsoletes(v, e):
				// e is obsolete; drop it
			default:
				kept = append(kept, e)
			}
		}
		if !dominated {
			kept = append(kept, v)
		}
		set = kept
	}
	sortVersions(set)
	return set
}

// covers reports whether set already holds v or something superseding it.
func (r rules) covers(set []Version, v Version) bool {
	for _, e := range set {
		if r.same(e, v) || r.obsoletes(e, v) {
			return true
		}
	}
	return false
}

func sortVersions(vs []Version) {
	sort.Slice(vs, func(i, j int) bool { return vs[i].Clock().String() < vs[j].Clock().String() })
}

func cloneVersions(vs []Version) []Version {
	out := make([]Version, len(vs))
	for i, v := range vs {
		v.Ctx = v.Ctx.Copy()
		out[i] = v
	}
	return out
}
