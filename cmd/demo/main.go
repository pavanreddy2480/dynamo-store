// Command demo runs the partition / concurrent-update / reconnect scenario
// under several quorum and resolution settings and prints what happened.
//
// Each case calls store.RunPartitionScenario, which writes a key, partitions
// the cluster so the key's replicas are split 1 | 2, updates the key on both
// sides, heals, and checks the declared resolution rule. The cases cover:
//
//   - keeping siblings at R=W=1 and R=W=2 with strict quorums;
//   - keeping siblings with sloppy quorums and hinted handoff;
//   - last-writer-wins, with and without a 50ms clock skew on the minority
//     side (which lets the earlier write win).
//
// For every case the step-by-step log, the number of siblings returned after
// the heal, and whether every acknowledged item survived are printed.
//
// Usage:
//
//	go run ./cmd/demo
//
// The command takes no flags. It exits with status 1 and prints
// "RESOLUTION RULE VIOLATED" if any case breaks its declared resolution
// rule, and status 0 otherwise.
package main

import (
	"fmt"
	"os"
	"time"

	"dynamostore/store"
)

// main runs every scenario case in order, prints its log and summary, and
// sets the exit status according to whether all resolution rules held.
func main() {
	// cases pairs a human-readable label with the scenario options it runs.
	cases := []struct {
		name string
		o    store.ScenarioOptions
	}{
		{"siblings, R=1 W=1, strict quorum", store.ScenarioOptions{Policy: store.KeepSiblings, R: 1, W: 1}},
		{"siblings, R=2 W=2, strict quorum", store.ScenarioOptions{Policy: store.KeepSiblings, R: 2, W: 2}},
		{"siblings, R=2 W=2, sloppy quorum + hinted handoff", store.ScenarioOptions{Policy: store.KeepSiblings, R: 2, W: 2, Sloppy: true}},
		{"last-writer-wins, R=1 W=1", store.ScenarioOptions{Policy: store.LastWriterWins, R: 1, W: 1}},
		{"last-writer-wins, R=1 W=1, minority clock +50ms", store.ScenarioOptions{Policy: store.LastWriterWins, R: 1, W: 1, SkewMinority: 50 * time.Millisecond}},
	}
	// allOK stays true only if every case converged to the value its
	// declared rule predicts. Losing acknowledged items counts as a violation
	// only under KeepSiblings; last-writer-wins explicitly permits it, so for
	// those cases AckedKept is printed for information only.
	allOK := true
	for i, tc := range cases {
		r := store.RunPartitionScenario(tc.o)
		fmt.Printf("=== %d. %s (key %s, replicas %v)\n", i+1, tc.name, r.Key, r.PrefList)
		for _, l := range r.Log {
			fmt.Println("  " + l)
		}
		fmt.Printf("  siblings returned after heal: %d; every acknowledged item kept: %v\n\n", len(r.Siblings), r.AckedKept)
		allOK = allOK && r.RuleHolds
	}
	if !allOK {
		fmt.Println("RESOLUTION RULE VIOLATED")
		os.Exit(1)
	}
	fmt.Println("all scenarios: declared resolution rule verified")
}
