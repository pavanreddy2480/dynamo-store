// This file defines how a stored value and its causal history are
// represented: Dot (the identity of one write), Version (a value plus its
// dotted version vector) and the two settings that change how versions are
// compared, Policy (siblings or last-writer-wins) and ClockMode (dotted
// version vectors or per-node vector clocks). The merge rules that use them
// are in merge.go.

package store

import "dynamostore/vclock"

// Dot names one update: the Counter-th write coordinated by Node for a key.
//
// Dots are unique per key: a coordinator never issues the same counter
// twice for one key (see coordPut).
type Dot struct {
	Node    string // id of the coordinating node
	Counter uint64 // per-key write counter on that node, starting at 1
}

// Version is one value of a key together with its causal history.
//
// Causality is tracked with a dotted version vector: Dot identifies this
// write and Ctx is the vector clock of everything the writer had seen when
// it wrote (the context returned by its last Get). Clock() = Ctx ∪ Dot is
// the plain vector clock of the version.
type Version struct {
	Value  string    // the stored value
	Dot    Dot       // identity of the write that produced this version
	Ctx    vclock.VC // causal context the writer had seen
	TS     int64     // coordinator wall-clock time in ns; used only by LWW
	Origin string    // coordinating node; LWW tie-breaker
}

// Clock returns the version's vector clock.
//
// Returns:
//   - vclock.VC: a new clock equal to Ctx with the Dot's entry raised to
//     its counter; the caller may modify it.
func (v Version) Clock() vclock.VC {
	c := v.Ctx.Copy()
	if c[v.Dot.Node] < v.Dot.Counter {
		c[v.Dot.Node] = v.Dot.Counter
	}
	return c
}

// Covers reports whether v's writer had seen w (or v is w): w's dot is in
// v's causal context.
//
// Parameters:
//   - w: the version to test.
//
// Returns:
//   - bool: true if v is w or causally descends from it.
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

// String returns the policy's name as used in benchmark tables.
//
// Returns:
//   - string: "last-writer-wins" or "siblings+client-reconcile".
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

// String returns the clock mode's name as used in benchmark tables.
//
// Returns:
//   - string: "per-node vector clock" or "dotted version vector".
func (m ClockMode) String() string {
	if m == PerNodeVC {
		return "per-node vector clock"
	}
	return "dotted version vector"
}
