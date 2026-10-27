// This file holds the application-level conflict resolution rule used with
// the KeepSiblings policy: UnionResolver, which treats values as
// comma-separated sets and merges siblings by set union, and the isSubset
// helper used to check that no acknowledged item was lost.

package store

import (
	"sort"
	"strings"
)

// UnionResolver treats values as comma-separated sets (a shopping cart) and
// returns their union. It is the explicit, declared resolution rule for the
// sibling policy: no acknowledged item is ever lost.
//
// It satisfies Resolver. Empty items are ignored and the result is sorted,
// so the same siblings always resolve to the same string regardless of
// order.
//
// Parameters:
//   - values: the sibling values to merge.
//
// Returns:
//   - string: the sorted, de-duplicated union of all items, comma-separated.
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

// isSubset reports whether every item of comma-separated set a is in b.
//
// Parameters:
//   - a: the comma-separated items that must be present.
//   - b: the comma-separated set to search.
//
// Returns:
//   - bool: true if every non-empty item of a appears in b.
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
