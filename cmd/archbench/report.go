// This file holds the report plumbing shared by every archbench experiment:
// the in-memory Markdown builder that becomes results/architecture.md, the w
// helper that echoes each line to stdout while appending it to that report,
// and small utilities (node id generation and latency percentiles) used by
// more than one experiment.

package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// md accumulates the Markdown report written to results/architecture.md.
//
// Every experiment appends to it through w; main writes its contents to disk
// once all experiments have finished. It is only touched from the main
// goroutine, so it needs no locking.
var md strings.Builder

// w prints a formatted line to stdout and appends the same text to the report.
//
// Writing through a single helper keeps the console output and the Markdown
// file byte-for-byte identical, so a run can be followed live and the file is
// exactly what was printed.
//
// Parameters:
//   - format: a fmt.Sprintf format string; it must include any trailing
//     newline, and a literal percent sign must be written as %%.
//   - a: the values substituted into format.
func w(format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	fmt.Print(s)
	md.WriteString(s)
}

// nodes returns the node ids "n1" through "nN", in order.
//
// These match the ids store.NewCluster assigns, so experiments that build a
// ring directly (A1) use the same names as those that build a full cluster.
//
// Parameters:
//   - n: the number of ids to generate; zero yields an empty slice.
//
// Returns:
//   - []string: a new slice of length n holding "n1".."nN".
func nodes(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return ids
}

// pctMs returns the p-th percentile of a set of durations, in milliseconds.
//
// It uses the nearest-rank method on a sorted copy: the element at index
// floor(p*(len-1)). The input slice is not reordered.
//
// Parameters:
//   - ds: the observed durations; may be empty.
//   - p: the percentile as a fraction in [0, 1], e.g. 0.99 for p99.
//
// Returns:
//   - float64: the percentile in milliseconds, or NaN when ds is empty (so
//     the report shows "NaN" rather than a misleading zero).
func pctMs(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return math.NaN()
	}
	// Sort a copy so callers can keep appending to or reusing ds.
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return float64(c[int(p*float64(len(c)-1))]) / float64(time.Millisecond)
}
