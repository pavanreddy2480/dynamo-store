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
//
// Everything printed to stdout is also collected and written, unchanged, to
// <out>/architecture.md once all experiments have finished.
//
// Usage:
//
//	go run ./cmd/archbench [-duration 2s] [-out results]
//
// Flags:
//
//	-duration  load duration per configuration (default 2s); A3 and A5 run
//	           their load phases for half of it
//	-out       output directory for architecture.md (default "results")
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	// dur is how long each configuration's load phase runs (-duration).
	dur = flag.Duration("duration", 2*time.Second, "load duration per configuration")
	// outDir is the directory architecture.md is written to (-out).
	outDir = flag.String("out", "results", "output directory")
)

// main parses flags, runs experiments A1 through A5 in order, and writes the
// accumulated report to <out>/architecture.md.
//
// It panics if the output directory or file cannot be written, since the
// report is the command's only product.
func main() {
	flag.Parse()
	w("# Architecture experiments\n\n")
	w("Default cluster unless stated: 5 nodes, N=3, replica-to-replica latency 0.4ms + Exp(0.8ms) with 3%% of messages +5ms, client hop 0.1ms + Exp(0.1ms), replica timeout 20ms. Regenerate with `go run ./cmd/archbench`.\n\n")
	ringBalance()
	replication()
	repair()
	coordinatorChoice()
	timeouts()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	path := filepath.Join(*outDir, "architecture.md")
	if err := os.WriteFile(path, []byte(md.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
