// This file drives the ablation experiments: it runs X1 to X11 (or those
// named by -only) and writes results/ablations.md.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// runAblations runs the selected ablation experiments in order and writes
// the accumulated report to <out>/ablations.md.
func runAblations() {
	out("# Ablations\n\nEach experiment changes one design choice and reports mean (min-max) over %d runs of %v. Regenerate with `go run ./cmd/bench -ablate`.\n", *reps, *dur)
	for _, e := range []struct {
		id  string
		run func()
	}{
		{"X1", x1Noise}, {"X2", x2Latency}, {"X3", x3Periodic}, {"X4", x4Cluster},
		{"X5", x5Workload}, {"X6", x6Handoff}, {"X7", x7ReadPath}, {"X8", x8Failure},
		{"X9", x9Growth}, {"X10", x10Mixed}, {"X11", x11Join},
	} {
		if *only == "" || strings.Contains(","+*only+",", ","+e.id+",") {
			e.run()
		}
	}
	if *only != "" {
		return
	}
	path := filepath.Join(*outDir, "ablations.md")
	if err := os.WriteFile(path, []byte(report.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
