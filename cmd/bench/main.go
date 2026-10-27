// Command bench measures latency, availability, returned versions and
// stale-read frequency for different quorum settings, with and without
// network partitions. Results are printed and written to -out as Markdown.
//
// Every run uses an in-process simulated cluster (store.DefaultConfig: five
// nodes, N=3 replicas per key) with randomised message latency, so no
// external services are needed. Three experiments are run in order:
//
//   - E1 / E1b (e1_quorum.go): a sweep over read/write quorums on a healthy
//     cluster, with read repair on and off, and then again with one slow
//     replica. Reports latency percentiles and the share of stale reads.
//   - E2 (e2_concurrency.go): eight clients doing concurrent
//     read-modify-write on one key, for each conflict policy and clock
//     scheme. Reports siblings per read and acknowledged items lost.
//   - E3 (e3_partition.go): a {n1,n2} | {n3,n4,n5} partition with clients on
//     both sides, using strict and then sloppy quorums. Reports per-side
//     availability and stale reads, and post-heal convergence and lost
//     writes.
//
// Usage:
//
//	go run ./cmd/bench [-duration 2s] [-out results]
//
// Flags:
//
//	-duration  load duration per configuration (default 2s). Each table row
//	           runs for this long, so total run time grows with it.
//	-out       directory for Markdown results (default "results"). It is
//	           created if missing; the report is written to benchmark.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dynamostore/store"
)

// Command-line flags. They are read after flag.Parse in main and consulted
// by the experiment functions.
var (
	// dur is how long each configuration (table row) applies load.
	dur = flag.Duration("duration", 2*time.Second, "load duration per configuration")
	// outDir is the directory benchmark.md is written to.
	outDir = flag.String("out", "results", "directory for Markdown results")
)

// main parses flags, runs experiments E1, E1b, E2 and E3 in sequence and
// writes their results both to standard output and, as one Markdown
// document, to <out>/benchmark.md.
//
// It panics if the output directory cannot be created or the report cannot
// be written, since there is nothing useful to do without them.
func main() {
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	// w prints a formatted fragment to stdout and also appends it to the
	// Markdown report, so the console output and the file stay identical.
	var md strings.Builder
	w := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		fmt.Print(s)
		md.WriteString(s)
	}
	w("# Benchmark results\n\n")
	w("5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3%% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. %v of load per row.\n\n", *dur)

	// E1 runs with no slow node; E1b repeats the sweep with n3 slowed down.
	// Each setting runs twice: read repair on (the latency columns come from
	// this run) and off, to show how much staleness read repair hides.
	for _, slow := range []time.Duration{0, 3 * time.Millisecond} {
		if slow == 0 {
			w("## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)\n\n")
		} else {
			w("\n## E1b. Same sweep with one slow replica (n3: +%v per message)\n\n", slow)
		}
		w("| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |\n")
		w("|---|---|---|---|---|---|---|---|---|\n")
		for _, q := range sweep {
			on := quorumSweep(q, true, slow)
			off := quorumSweep(q, false, slow)
			w("| %d,%d | %v | %s | %s | %s | %s | %d | %s | %s |\n", q.R, q.W, q.R+q.W > 3,
				pct(on.getLat, .5), pct(on.getLat, .99), pct(on.putLat, .5), pct(on.putLat, .99),
				on.getOK, ratio(on.staleReads, on.getOK), ratio(off.staleReads, off.getOK))
		}
	}

	// E2 compares three policy/clock combinations: siblings with dotted
	// version vectors, siblings with per-node vector clocks (the Dynamo
	// paper's scheme) and last-writer-wins.
	w("\n## E2. Concurrent read-modify-write on one key (8 writers)\n\n")
	w("| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |\n")
	w("|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, v := range []struct {
		p store.Policy
		m store.ClockMode
	}{{store.KeepSiblings, store.DottedVV}, {store.KeepSiblings, store.PerNodeVC}, {store.LastWriterWins, store.DottedVV}} {
		p, actor := v.p, v.m.String()
		for _, q := range []rw{{1, 1}, {2, 2}, {3, 3}} {
			r := concurrentUpdates(q, p, v.m)
			// Average versions per successful read, guarded against a run
			// with no successful reads.
			avg := 0.0
			if r.st.getOK > 0 {
				avg = float64(r.st.siblingSum) / float64(r.st.getOK)
			}
			w("| %s | %s | %d,%d | %s | %s | %d | %.2f | %s | %d | %d | %d |\n", p, actor, q.R, q.W,
				pct(r.st.getLat, .5), pct(r.st.putLat, .5), r.st.getOK, avg,
				ratio(r.st.multi, r.st.getOK), r.st.maxSiblings, r.ackedItems, r.lostItems)
		}
	}

	// E3: availability is successful/attempted ops per side; stale reads
	// are a share of successful reads. The "/64" denominator is the key
	// count used by partitioned.
	w("\n## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys\n\n")
	w("Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.\n\n")
	w("| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |\n")
	w("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, sloppy := range []bool{false, true} {
		mode := "strict"
		if sloppy {
			mode = "sloppy+hinted handoff"
		}
		for _, q := range []rw{{1, 1}, {1, 3}, {2, 2}, {3, 1}, {3, 3}} {
			r := partitioned(q, sloppy)
			w("| %s | %d,%d | %s | %s | %s | %s | %s | %s | %s | %s | %d/%d | %d | %v |\n", mode, q.R, q.W,
				ratio(r.a.getOK, r.a.gets), ratio(r.a.putOK, r.a.puts),
				ratio(r.b.getOK, r.b.gets), ratio(r.b.putOK, r.b.puts),
				pct(r.a.getLat, .5), pct(r.b.getLat, .5),
				ratio(r.a.staleReads, r.a.getOK), ratio(r.b.staleReads, r.b.getOK),
				r.keysWithSibs, 64, r.lostAfterResolve, r.converged)
		}
	}
	path := filepath.Join(*outDir, "benchmark.md")
	if err := os.WriteFile(path, []byte(md.String()), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("\nwrote", path)
}
