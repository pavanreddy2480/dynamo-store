// Command archbench runs the experiments that justify individual
// architecture decisions (see ARCHITECTURE.md). Results go to
// results/architecture.md.
//
// Each experiment builds an in-process store.Cluster with simulated network
// latency, drives a workload against it, and prints a Markdown table:
//
//   - A1 (a1_ring.go): ring hash function and virtual nodes per node.
//   - A2 (a2_replication.go): replication payload and version comparison.
//   - A3 (a3_repair.go): repair mechanisms after a partition heals.
//   - A4 (a4_coordinator.go): how a client chooses its coordinator.
//   - A5 (a5_timeout.go): the coordinator's replica timeout.
//   - A6 (a6_merkle.go): full-state vs Merkle anti-entropy, tree depth.
//   - A8 (a8_partitions.go): equal spacing vs random vnodes vs fixed partitions.
//
// Everything printed to stdout is also collected and written, unchanged, to
// <out>/architecture.md once all experiments have finished.
//
// Usage:
//
//	go run ./cmd/archbench [-duration 2s] [-out results] [-only A6]
//
// Flags:
//
//	-duration  load duration per configuration (default 2s); A3 and A5 run
//	           their load phases for half of it
//	-out       output directory for architecture.md (default "results")
//	-only      run only these experiments (comma-separated, e.g. A6,A7);
//	           charts are still written but architecture.md is not
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	// dur is how long each configuration's load phase runs (-duration).
	dur = flag.Duration("duration", 2*time.Second, "load duration per configuration")
	// outDir is the directory architecture.md is written to (-out).
	outDir = flag.String("out", "results", "output directory")
	// only restricts the run to some experiments (-only).
	only = flag.String("only", "", "comma-separated experiments to run, e.g. A6,A7")
)

// experiments lists every experiment in report order.
var experiments = []struct {
	id  string
	run func()
}{
	{"A1", ringBalance}, {"A2", replication}, {"A3", repair}, {"A4", coordinatorChoice},
	{"A5", timeouts}, {"A6", merkleVsFull}, {"A8", partitionExperiment},
}

// main parses flags, runs the experiments in order, and writes the
// accumulated report to <out>/architecture.md.
//
// It panics if the output directory or file cannot be written, since the
// report is the command's only product.
func main() {
	flag.Parse()
	w("# Architecture experiments\n\n")
	w("Default cluster unless stated: 5 nodes, N=3, replica-to-replica latency 0.4ms + Exp(0.8ms) with 3%% of messages +5ms, client hop 0.1ms + Exp(0.1ms), replica timeout 20ms. Regenerate with `go run ./cmd/archbench`.\n\n")
	for _, e := range experiments {
		if *only == "" || strings.Contains(","+*only+",", ","+e.id+",") {
			e.run()
		}
	}
	if *only != "" {
		return
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	path := filepath.Join(*outDir, "architecture.md")
	if err := os.WriteFile(path, []byte(md.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
