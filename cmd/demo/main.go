// Command demo runs the partition / concurrent-update / reconnect scenario
// under several quorum and resolution settings and prints what happened.
package main

import (
	"fmt"
	"os"
	"time"

	"dynamostore/store"
)

func main() {
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
