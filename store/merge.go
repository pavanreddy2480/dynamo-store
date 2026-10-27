// This file implements the version-ordering rules shared by every replica,
// coordinator and repair path: whether one version supersedes another,
// which version wins under last-writer-wins, and how incoming versions are
// folded into a sibling set. It also has the helpers that keep sibling sets
// in a deterministic order and copy them safely across locks.

package store

import (
	"sort"

	"dynamostore/vclock"
)

// rules bundles the two settings that decide how versions compare. Every
// merge and comparison goes through it, so replicas, coordinators and repair
// all apply the same ordering.
type rules struct {
	policy Policy    // what to keep when versions are concurrent
	clocks ClockMode // how causal order between versions is decided
}

// obsoletes reports whether a supersedes b.
//
// Parameters:
//   - a: the candidate newer version.
//   - b: the candidate older version.
//
// Returns:
//   - bool: true if a causally descends from b and is a different write;
//     false for the same write or for concurrent versions.
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
//
// Under DottedVV that means the same dot; under PerNodeVC, equal vector
// clocks, which is how that scheme conflates distinct concurrent writes.
//
// Parameters:
//   - a, b: the versions to compare.
//
// Returns:
//   - bool: true if the store treats a and b as one version.
func (r rules) same(a, b Version) bool {
	if r.clocks == PerNodeVC {
		return vclock.Compare(a.Clock(), b.Clock()) == vclock.Equal
	}
	return a.Dot == b.Dot
}

// lwwBeats reports whether a wins over b under last-writer-wins. Causal
// order is respected first; only concurrent versions fall back to the
// timestamp, and equal timestamps to the coordinating node's id, so every
// replica picks the same winner.
//
// Parameters:
//   - a: the challenger.
//   - b: the current winner.
//
// Returns:
//   - bool: true if a should replace b.
func (r rules) lwwBeats(a, b Version) bool {
	switch {
	case r.same(a, b) || r.obsoletes(b, a):
		return false
	case r.obsoletes(a, b):
		return true
	case a.TS != b.TS:
		return a.TS > b.TS
	}
	// Equal timestamps from different coordinators: the higher node id
	// wins. Any fixed total order works; it only has to be the same on
	// every replica so they converge on one value.
	return a.Origin > b.Origin
}

// merge folds incoming into set and returns the new set. Versions
// superseded by another version are dropped. Under KeepSiblings the result
// holds only mutually concurrent versions; under LastWriterWins it holds at
// most one. Merge is commutative and idempotent, so replicas that receive
// the same versions in any order, or more than once, end up identical.
//
// set must not be shared with another goroutine: merge may reuse and
// reorder its backing array. Versions are not deep-copied; callers clone
// them first when they come from another lock's domain.
//
// Parameters:
//   - set: the current sibling set (may be nil).
//   - incoming: versions to fold in.
//
// Returns:
//   - []Version: the new sibling set, sorted deterministically.
func (r rules) merge(set []Version, incoming ...Version) []Version {
	for _, v := range incoming {
		if r.policy == LastWriterWins {
			if len(set) == 0 || r.lwwBeats(v, set[0]) {
				set = []Version{v}
			}
			continue
		}
		dominated := false
		// Build into a fresh slice (cap 0) so appending never overwrites
		// elements of set that the loop has not read yet.
		kept := set[:0:0]
		for _, e := range set {
			switch {
			case r.same(e, v) || r.obsoletes(e, v):
				// v is already present or superseded: keep e, drop v.
				dominated = true
				kept = append(kept, e)
			case r.obsoletes(v, e):
				// e is obsolete; drop it
			default:
				// Concurrent: both stay as siblings.
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
//
// Read repair and hinted handoff use it to decide what a replica still
// lacks.
//
// Parameters:
//   - set: the sibling set to search.
//   - v: the version to look for.
//
// Returns:
//   - bool: true if some version in set is v or supersedes it.
func (r rules) covers(set []Version, v Version) bool {
	for _, e := range set {
		if r.same(e, v) || r.obsoletes(e, v) {
			return true
		}
	}
	return false
}

// sortVersions orders a sibling set deterministically, so replicas holding
// the same versions also return them in the same order.
//
// The slice is sorted in place by the string form of each version's clock.
//
// Parameters:
//   - vs: the sibling set to sort.
func sortVersions(vs []Version) {
	sort.Slice(vs, func(i, j int) bool { return vs[i].Clock().String() < vs[j].Clock().String() })
}

// cloneVersions deep-copies versions (including their context maps) so the
// copy can leave the lock that guards the original.
//
// Parameters:
//   - vs: the versions to copy (may be nil).
//
// Returns:
//   - []Version: a new slice of the same length whose Ctx maps are
//     independent copies.
func cloneVersions(vs []Version) []Version {
	out := make([]Version, len(vs))
	for i, v := range vs {
		v.Ctx = v.Ctx.Copy()
		out[i] = v
	}
	return out
}
