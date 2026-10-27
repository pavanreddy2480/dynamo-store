// This file holds the correctness oracle shared by the benchmark experiments.
// Clients report every acknowledged write to the oracle; before each read
// they take a snapshot of what the oracle knows for the key, and after the
// read the stale function compares that snapshot against the versions the
// store actually returned. A read is stale when it misses a write that had
// already been acknowledged to some client before the read began, which is
// exactly the guarantee a strict R+W>N quorum is supposed to provide.

package main

import (
	"sync"

	"dynamostore/store"
)

// oracle remembers, per key, the latest acknowledged writes: those that have
// not yet been superseded by another acknowledged write.
//
// Keeping only the non-superseded frontier (rather than every write ever
// acknowledged) keeps snapshots small and makes the staleness check cheap:
// returning a version that covers the frontier implies all older writes were
// seen too. Under concurrent writers the frontier can hold several mutually
// concurrent versions.
//
// An oracle is safe for concurrent use by multiple goroutines.
type oracle struct {
	// mu guards acked; writers and readers run on many goroutines.
	mu sync.Mutex
	// acked maps a key to its frontier of acknowledged, not-yet-superseded
	// versions.
	acked map[string][]store.Version
}

// newOracle creates an empty oracle that knows of no acknowledged writes.
//
// Returns:
//   - *oracle: a ready-to-use oracle with an initialised key map.
func newOracle() *oracle { return &oracle{acked: map[string][]store.Version{}} }

// ack records an acknowledged write and forgets the writes it supersedes.
//
// Any previously recorded version that v covers (v's writer had seen it) is
// dropped from the key's frontier; versions concurrent with v are kept,
// because a later read must still return or supersede them.
//
// Parameters:
//   - key: the key that was written.
//   - v: the version the store returned when it acknowledged the write.
func (o *oracle) ack(key string, v store.Version) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var kept []store.Version
	for _, e := range o.acked[key] {
		if !v.Covers(e) {
			kept = append(kept, e)
		}
	}
	o.acked[key] = append(kept, v)
}

// snapshot returns the acknowledged writes a read starting now must see.
//
// The result is a copy, so the caller may hold it while other goroutines
// keep acknowledging writes for the same key.
//
// Parameters:
//   - key: the key about to be read.
//
// Returns:
//   - []store.Version: the key's current frontier of acknowledged writes
//     (nil if nothing has been acknowledged for the key yet).
func (o *oracle) snapshot(key string) []store.Version {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]store.Version(nil), o.acked[key]...)
}

// stale reports whether a read missed a write acknowledged before it began:
// some acknowledged write is neither returned nor superseded by a returned
// version.
//
// Parameters:
//   - before: the oracle snapshot taken immediately before the read started.
//   - got: the versions (siblings) the read returned.
//
// Returns:
//   - bool: true if at least one write in before is not covered by any
//     version in got; false if every acknowledged write was observed.
func stale(before []store.Version, got []store.Version) bool {
	for _, a := range before {
		// A write counts as seen if any returned version covers it, i.e. the
		// returned version is that write or was written with it in context.
		seen := false
		for _, v := range got {
			if v.Covers(a) {
				seen = true
				break
			}
		}
		if !seen {
			return true
		}
	}
	return false
}
