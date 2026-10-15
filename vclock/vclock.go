// Package vclock implements version vectors (vector clocks) used to order
// versions of a key and to detect concurrent updates.
//
// A clock maps each node id to a counter. Clock a descends from b when every
// entry of a is at least the matching entry of b; if neither descends from
// the other, the two histories are concurrent and neither may replace the
// other.
package vclock

import (
	"fmt"
	"sort"
	"strings"
)

// Order is the causal relationship between two clocks.
type Order int

// Possible results of Compare.
const (
	Equal      Order = iota // identical histories
	Before                  // a happened-before b (b descends from a)
	After                   // b happened-before a (a descends from b)
	Concurrent              // neither descends from the other
)

// String returns the order's name.
func (o Order) String() string {
	return [...]string{"Equal", "Before", "After", "Concurrent"}[o]
}

// VC maps a node id to the number of updates that node coordinated.
// Missing entries are zero.
type VC map[string]uint64

// New returns an empty clock.
func New() VC { return VC{} }

// Copy returns a deep copy.
func (v VC) Copy() VC {
	c := make(VC, len(v))
	for k, n := range v {
		c[k] = n
	}
	return c
}

// Increment returns a copy of v with node's counter set to at least floor+1
// and at least v[node]+1. floor lets a coordinator guarantee it never reuses
// a counter value it already issued for this key.
func (v VC) Increment(node string, floor uint64) VC {
	c := v.Copy()
	n := c[node]
	if floor > n {
		n = floor
	}
	c[node] = n + 1
	return c
}

// Merge returns the pointwise maximum of the clocks.
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

// Compare reports how a relates to b.
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
	// Entries present only in b are zero in a.
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
func Descends(a, b VC) bool {
	o := Compare(a, b)
	return o == Equal || o == After
}

// String renders the clock deterministically, e.g. [n1:2 n3:1].
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
