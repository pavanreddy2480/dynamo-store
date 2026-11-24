// Command plots draws the SVG charts for every experiment in the project
// from the Markdown tables that cmd/bench and cmd/archbench commit to
// results/benchmark.md and results/architecture.md. Reading the committed
// tables (instead of re-running anything) means each chart shows exactly
// the numbers in the report next to it.
//
// A1b's charts are drawn by archbench itself, since they come from
// data that is not in a table. Everything lands in the -out directory.
//
// Usage:
//
//	go run ./cmd/plots [-dir results]
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"dynamostore/chart"
)

var dir = flag.String("dir", "results", "directory holding benchmark.md and architecture.md; charts are written there")

// table is one Markdown table: its column headers and string cells.
type table struct {
	head []string
	rows [][]string
}

// tables maps a section heading (the text after "## ", up to the first
// period) to its first table.
type tables map[string]table

// load parses every "## X. title" section of a Markdown report.
func load(path string) tables {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	out := tables{}
	var id string
	var t *table
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "## "):
			id, t = strings.SplitN(line[3:], ".", 2)[0], nil
		case id != "" && strings.HasPrefix(line, "|"):
			cells := strings.Split(strings.Trim(line, "| "), " | ")
			for i := range cells {
				cells[i] = strings.TrimSpace(cells[i])
			}
			switch {
			case t == nil:
				if _, seen := out[id]; !seen {
					out[id] = table{head: cells}
					v := out[id]
					t = &v
				}
			case strings.HasPrefix(cells[0], "---"):
			default:
				t.rows = append(t.rows, cells)
				out[id] = *t
			}
		}
	}
	return out
}

var numRe = regexp.MustCompile(`-?\d+(?:\.\d+)?`)

// num returns the i-th number found in a cell ("2180 (19.58%)" has 2180 at
// 0 and 19.58 at 1); a cell with no number, such as "-", gives 0.
func num(cell string, i int) float64 {
	m := numRe.FindAllString(cell, -1)
	if i >= len(m) {
		return 0
	}
	v, _ := strconv.ParseFloat(m[i], 64)
	return v
}

// col returns the index of the first header that starts with prefix.
func (t table) col(prefix string) int {
	for i, h := range t.head {
		if strings.HasPrefix(h, prefix) {
			return i
		}
	}
	panic(fmt.Sprintf("no column %q in %v", prefix, t.head))
}

// series collects, for each row accepted by keep, num(cell of column c, idx).
func (t table) series(keep func(r []string) bool, c string, idx int) []float64 {
	var out []float64
	ci := t.col(c)
	for _, r := range t.rows {
		if keep(r) {
			out = append(out, num(r[ci], idx))
		}
	}
	return out
}

// labels collects the text of column c for rows accepted by keep.
func (t table) labels(keep func(r []string) bool, c string) []string {
	var out []string
	ci := t.col(c)
	for _, r := range t.rows {
		if keep(r) {
			out = append(out, r[ci])
		}
	}
	return out
}

func all([]string) bool { return true }

// where returns a filter accepting rows whose column ci equals v.
func where(ci int, v string) func([]string) bool {
	return func(r []string) bool { return r[ci] == v }
}

func main() {
	flag.Parse()
	out := func(name string) string { return filepath.Join(*dir, name) }
	b := load(filepath.Join(*dir, "benchmark.md"))
	a := load(filepath.Join(*dir, "architecture.md"))

	// E1 and E1b: latency and staleness by quorum.
	for _, e := range []struct{ id, file, title string }{
		{"E1", "e1_latency.svg", "E1 tail latency by quorum (healthy cluster)"},
		{"E1b", "e1b_latency.svg", "E1b tail latency by quorum (one slow replica, +3ms)"},
	} {
		t := b[e.id]
		chart.Bar(out(e.file), e.title, "R,W", "p99 latency (ms)", t.labels(all, "R,W"), []string{"get p99", "put p99"},
			[][]float64{t.series(all, "get p99", 0), t.series(all, "put p99", 0)}, false)
	}
	t := b["E1"]
	chart.Bar(out("e1_stale.svg"), "E1 stale reads by quorum (R+W>3 never stale)", "R,W", "stale reads (%)", t.labels(all, "R,W"),
		[]string{"read repair on", "read repair off"}, [][]float64{t.series(all, "stale reads (read repair on)", 0), t.series(all, "stale reads (read repair off)", 0)}, false)

	// E2: acknowledged items lost, by policy/clock and quorum.
	t = b["E2"]
	var groups []string
	for _, rw := range []string{"1,1", "2,2", "3,3"} {
		groups = append(groups, rw)
	}
	var e2 [][]float64
	names := []string{"siblings + dotted VV", "siblings + per-node VC", "last-writer-wins"}
	pol, clk, rw := t.col("policy"), t.col("clocks"), t.col("R,W")
	for si := range names {
		var v []float64
		for _, g := range groups {
			for _, r := range t.rows {
				if r[rw] == g && ((si == 0 && strings.HasPrefix(r[pol], "siblings") && strings.HasPrefix(r[clk], "dotted")) ||
					(si == 1 && strings.HasPrefix(r[pol], "siblings") && strings.HasPrefix(r[clk], "per-node")) ||
					(si == 2 && strings.HasPrefix(r[pol], "last-writer"))) {
					v = append(v, 100*num(r[t.col("acked items lost")], 0)/num(r[t.col("acked items")], 0))
				}
			}
		}
		e2 = append(e2, v)
	}
	chart.Bar(out("e2_lost.svg"), "E2 acknowledged writes lost under concurrent updates", "R,W", "acknowledged items lost (%)", groups, names, e2, false)

	// E3: availability on each side of the partition.
	t = b["E3"]
	for _, kind := range []string{"strict", "sloppy+hinted handoff"} {
		keep := func(r []string) bool { return r[0] == kind }
		chart.Bar(out("e3_"+strings.Split(kind, "+")[0]+".svg"), "E3 availability during partition, "+kind+" quorum", "R,W", "successful ops (%)", t.labels(keep, "R,W"),
			[]string{"minority get", "minority put", "majority get", "majority put"},
			[][]float64{t.series(keep, "A get avail", 0), t.series(keep, "A put avail", 0), t.series(keep, "B get avail", 0), t.series(keep, "B put avail", 0)}, false)
	}

	// A1: ring balance by hash function.
	t = a["A1"]
	xs := []string{"1", "8", "64", "256"}
	var a1 [][]float64
	for _, h := range []string{"FNV-1a", "MD5"} {
		a1 = append(a1, t.series(where(0, h), "coefficient", 0))
	}
	chart.Line(out("a1_hash.svg"), "A1 load imbalance by hash function (lower is better)", "virtual nodes per node", "coefficient of variation", xs, []string{"FNV-1a", "MD5"}, a1, false)

	// A2: acknowledged writes lost, by clock scheme and replication payload.
	t = a["A2"]
	clk, pay, rw := t.col("clocks"), t.col("replication payload"), t.col("R,W")
	n2 := []struct{ name, clock, payload string }{
		{"dotted VV, whole sibling set", "dotted", "whole"},
		{"dotted VV, new version only", "dotted", "new"},
		{"per-node VC, whole sibling set", "per-node", "whole"},
	}
	var a2 [][]float64
	var n2names []string
	for _, n := range n2 {
		var v []float64
		for _, g := range []string{"1,1", "2,2"} {
			for _, r := range t.rows {
				if r[rw] == g && strings.HasPrefix(r[clk], n.clock) && strings.HasPrefix(r[pay], n.payload) {
					v = append(v, num(r[t.col("lost items")], 1))
				}
			}
		}
		a2 = append(a2, v)
		n2names = append(n2names, n.name)
	}
	chart.Bar(out("a2_lost.svg"), "A2 acknowledged writes lost by clock scheme and replication payload", "R,W", "lost items (%)", []string{"1,1", "2,2"}, n2names, a2, false)

	// A3: what is left unrepaired after the partition heals.
	t = a["A3"]
	var short []string
	for _, l := range t.labels(all, "repair after heal") {
		short = append(short, strings.TrimSpace(strings.SplitN(strings.SplitN(l, " (", 2)[0], " only", 2)[0]))
	}
	div := t.series(all, "divergent", 0)
	for i := range div {
		div[i] = 100 * div[i] / 64
	}
	chart.Bar(out("a3_repair.svg"), "A3 what stays broken after a partition heals", "repair applied", "% (of writes / of keys)", short,
		[]string{"under-replicated writes", "keys with disagreeing replicas"}, [][]float64{t.series(all, "under-replicated", 1), div}, false)

	// A4: coordinator choice.
	t = a["A4"]
	var load [][]float64
	loadCol := t.col("coordinator load")
	for _, r := range t.rows {
		var v []float64
		for i := 0; i < 5; i++ {
			v = append(v, num(r[loadCol], i))
		}
		load = append(load, v)
	}
	chart.Bar(out("a4_load.svg"), "A4 coordinator load per node (ideal 20%)", "node", "share of coordinated requests (%)", []string{"n1", "n2", "n3", "n4", "n5"},
		[]string{"always first replica", "random replica (default)"}, load, false)
	chart.Bar(out("a4_stale.svg"), "A4 stale reads by coordinator choice (R=W=1)", "coordinator", "stale reads (%)", []string{"always first replica", "random replica (default)"},
		[]string{"stale reads"}, [][]float64{t.series(all, "stale reads", 0)}, false)

	// A5: replica timeout.
	t = a["A5"]
	g := t.labels(all, "replica timeout")
	chart.Bar(out("a5_timeout.svg"), "A5 replica timeout: failures vs availability", "replica timeout", "%", g,
		[]string{"puts failing, no partition", "put availability, partitioned"}, [][]float64{t.series(all, "no partition: put failures", 0), t.series(all, "partitioned: put availability", 0)}, false)
	chart.Bar(out("a5_latency.svg"), "A5 replica timeout: latency", "replica timeout", "ms", g,
		[]string{"put p99, no partition", "failed-op p50, partitioned"}, [][]float64{t.series(all, "no partition: put p99", 0), t.series(all, "partitioned: failed-op", 0)}, false)
	fmt.Println("charts written to", *dir)
}
