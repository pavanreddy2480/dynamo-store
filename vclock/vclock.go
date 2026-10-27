// Package vclock implements version vectors (vector clocks) used to order
// versions of a key and to detect concurrent updates.
//
// A clock maps each node id to a counter. Clock a descends from b when every
// entry of a is at least the matching entry of b; if neither descends from
// the other, the two histories are concurrent and neither may replace the
// other.
//
// Clocks are plain maps. Functions in this package never modify their
// arguments: Increment and Merge return new clocks, so a clock that has been
// attached to a stored version can be shared safely as long as callers treat
// it as read-only.
package vclock

import (
	"fmt"
	"sort"
	"strings"
)

// Order is the causal relationship between two clocks, as returned by
// Compare.
type Order int

// Possible results of Compare(a, b).
const (
	// Equal means a and b record identical histories.
	Equal Order = iota
	// Before means a happened-before b: b descends from a and is newer.
	Before
	// After means b happened-before a: a descends from b and is newer.
	After
	// Concurrent means neither clock descends from the other; the histories
	// diverged and both versions must be kept or explicitly resolved.
	Concurrent
)

// String returns the order's name ("Equal", "Before", "After" or
// "Concurrent").
//
// Returns:
//   - string: the name of o; it panics if o is not one of the defined
//     constants.
func (o Order) String() string {
	return [...]string{"Equal", "Before", "After", "Concurrent"}[o]
}

// VC is a version vector: it maps a node id to the number of updates that
// node coordinated. Missing entries are zero, so a nil or empty VC is the
// clock of a key that has never been written.
type VC map[string]uint64

// New returns an empty, non-nil clock.
//
// Returns:
//   - VC: a clock with no entries, ready to be written to.
func New() VC { return VC{} }

// Copy returns a deep copy of the clock.
//
// Returns:
//   - VC: a new map with the same entries as v; changing it does not affect
//     v. Copying a nil clock yields an empty, non-nil clock.
func (v VC) Copy() VC {
	c := make(VC, len(v))
	for k, n := range v {
		c[k] = n
	}
	return c
}

// Increment returns a copy of v with node's counter advanced past both its
// current value and floor.
//
// The new counter is max(v[node], floor) + 1. floor lets a coordinator
// guarantee it never reuses a counter value it already issued for this key,
// even when the client's context v is older than the coordinator's own
// history (reusing a value would make two different writes look identical).
//
// Parameters:
//   - node: the id of the coordinating node whose counter is advanced.
//   - floor: the highest counter value node has already issued for this
//     key; zero if none.
//
// Returns:
//   - VC: a new clock; v is not modified.
func (v VC) Increment(node string, floor uint64) VC {
	c := v.Copy()
	n := c[node]
	if floor > n {
		n = floor
	}
	c[node] = n + 1
	return c
}

// Merge returns the pointwise maximum of the given clocks: a clock that has
// seen every update any of them has seen.
//
// Parameters:
//   - clocks: the clocks to combine; nil or empty clocks are allowed.
//
// Returns:
//   - VC: a new, non-nil clock; the inputs are not modified.
func Merge(clocks ...VC) VC {
	out := VC{}
	for _, c := range clocks {
		for k, n := range c {
			if n > out[k] {
				out[k] = n
			}
		}
	}
	return out
}

// Compare reports how clock a relates causally to clock b.
//
// It scans both clocks once, noting whether a is behind b on some entry and
// whether b is behind a on some entry. Behind on neither means the clocks
// are equal; behind on exactly one side means that side happened before the
// other; behind on both means the histories are concurrent.
//
// Parameters:
//   - a: the first clock; nil is treated as empty.
//   - b: the second clock; nil is treated as empty.
//
// Returns:
//   - Order: Equal, Before (a < b), After (a > b) or Concurrent.
func Compare(a, b VC) Order {
	aLess, bLess := false, false // a has some entry < b, b has some entry < a
	for k, an := range a {
		bn := b[k]
		if an < bn {
			aLess = true
		} else if an > bn {
			bLess = true
		}
	}
	// Entries present only in b are zero in a. Only a positive counter makes
	// a lag behind; an explicit zero entry is the same as a missing one.
	for k, bn := range b {
		if _, ok := a[k]; !ok && bn > 0 {
			aLess = true
		}
	}
	switch {
	case !aLess && !bLess:
		return Equal
	case aLess && !bLess:
		return Before
	case !aLess && bLess:
		return After
	default:
		return Concurrent
	}
}

// Descends reports whether a ≥ b, i.e. a has seen every update in b.
//
// A clock descends from itself, so a version may safely replace another
// whenever its clock descends from the other's.
//
// Parameters:
//   - a: the candidate successor clock.
//   - b: the clock it is checked against.
//
// Returns:
//   - bool: true if Compare(a, b) is Equal or After; false if a is older
//     than or concurrent with b.
func Descends(a, b VC) bool {
	o := Compare(a, b)
	return o == Equal || o == After
}

// String renders the clock deterministically, e.g. [n1:2 n3:1].
//
// Entries are sorted by node id so equal clocks always print identically,
// which keeps logs and test output stable despite random map iteration.
//
// Returns:
//   - string: the entries as "id:counter" pairs, space-separated and
//     enclosed in brackets; "[]" for an empty clock.
func (v VC) String() string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s:%d", k, v[k])
	}
	return "[" + strings.Join(parts, " ") + "]"
}
