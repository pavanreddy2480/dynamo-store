// This file defines the measurement plumbing used by every experiment: the
// rw quorum setting and the list of settings swept in E1, the stats
// accumulator that client goroutines report each operation into, and the
// pct and ratio helpers that format latency percentiles and percentages for
// the Markdown result tables.

package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// rw is one quorum setting: how many replicas must answer a read (R) and
// how many must acknowledge a write (W) before the coordinator replies to
// the client. With N=3 replicas, R+W>N gives overlapping read and write
// quorums.
type rw struct {
	R, W int // read quorum and write quorum, each in [1, N]
}

// String formats the quorum setting for display.
//
// Returns:
//   - string: the setting in the form "R=<r> W=<w>".
func (q rw) String() string { return fmt.Sprintf("R=%d W=%d", q.R, q.W) }

// sweep lists the quorum settings measured in E1, covering both weak
// (R+W<=N) and strict (R+W>N) combinations for N=3.
var sweep = []rw{{1, 1}, {1, 2}, {2, 1}, {1, 3}, {3, 1}, {2, 2}, {2, 3}, {3, 2}, {3, 3}}

// stats accumulates per-operation results for one configuration. It is
// shared by all client goroutines, hence the mutex.
//
// Latencies and the stale/sibling counters are recorded only for successful
// operations; gets and puts count every attempt, so getOK/gets and
// putOK/puts give availability.
type stats struct {
	// mu guards every field below.
	mu sync.Mutex
	// getLat and putLat hold the latency of each successful read and write.
	getLat, putLat []time.Duration
	// gets counts attempted reads; getOK counts the ones that succeeded.
	gets, getOK int
	// puts counts attempted writes; putOK counts the ones that succeeded.
	puts, putOK int
	// staleReads counts successful reads that missed an acknowledged write.
	staleReads int
	// siblingSum totals the versions returned over all successful reads;
	// multi counts successful reads that returned more than one version.
	siblingSum, multi int
	// maxSiblings is the largest number of versions any single read returned.
	maxSiblings int
}

// get records one read: latency, success, staleness and siblings returned.
//
// Parameters:
//   - d: how long the read took.
//   - ok: whether the read succeeded; failed reads only increment gets.
//   - isStale: whether the read missed an acknowledged write (see stale).
//   - siblings: the number of versions the read returned.
func (s *stats) get(d time.Duration, ok bool, isStale bool, siblings int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if ok {
		s.getOK++
		s.getLat = append(s.getLat, d)
		if isStale {
			s.staleReads++
		}
		s.siblingSum += siblings
		if siblings > 1 {
			s.multi++
		}
		if siblings > s.maxSiblings {
			s.maxSiblings = siblings
		}
	}
}

// put records one write.
//
// Parameters:
//   - d: how long the write took.
//   - ok: whether the write was acknowledged; failed writes only increment
//     puts.
func (s *stats) put(d time.Duration, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if ok {
		s.putOK++
		s.putLat = append(s.putLat, d)
	}
}

// pct returns the p-th percentile of ds in milliseconds, or "-" if empty.
//
// It sorts a copy, so ds itself is left untouched, and uses the
// nearest-rank-below method (index floor(p*(n-1))).
//
// Parameters:
//   - ds: the latency samples.
//   - p: the percentile as a fraction in [0, 1], e.g. 0.5 or 0.99.
//
// Returns:
//   - string: the percentile in milliseconds with two decimals, or "-" when
//     ds is empty.
func pct(ds []time.Duration, p float64) string {
	if len(ds) == 0 {
		return "-"
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	d := c[int(p*float64(len(c)-1))]
	return fmt.Sprintf("%.2f", float64(d)/float64(time.Millisecond))
}

// ratio formats a/b as a percentage, or "-" if b is zero.
//
// Parameters:
//   - a: the numerator (e.g. successful or stale operations).
//   - b: the denominator (e.g. attempted or successful operations).
//
// Returns:
//   - string: the percentage with one decimal and a trailing "%", or "-"
//     when b is zero.
func ratio(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(a)/float64(b))
}
