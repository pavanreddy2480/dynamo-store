# Graphs for every experiment

Charts live in `results/` as SVG (open in a browser; hover a bar or point for its exact value). `go run ./cmd/plots` redraws all of them from the committed tables in `results/benchmark.md` and `results/architecture.md`, so a chart always matches the numbers next to it. A1b's charts are drawn by `go run ./cmd/archbench` itself.

| Experiment | What it tests | Chart(s) |
|---|---|---|
| E1 | latency and stale reads vs (R, W), healthy cluster | `e1_latency.svg`, `e1_stale.svg` |
| E1b | the same with one slow replica | `e1b_latency.svg` |
| E2 | acknowledged writes lost under concurrent updates | `e2_lost.svg` |
| E3 | availability on each side of a partition | `e3_strict.svg`, `e3_sloppy.svg` |
| A1 | hash function and vnode count: balance | `a1_hash.svg` |
| A1b | vnode ablation: balance, data moved, lookup cost | `a1b_balance.svg`, `a1b_moved.svg`, `a1b_lookup.svg` |
| A2 | clock scheme and replication payload: lost writes | `a2_lost.svg` |
| A3 | repair mechanisms after a partition heals | `a3_repair.svg` |
| A4 | coordinator choice: load balance and staleness | `a4_load.svg`, `a4_stale.svg` |
| A5 | replica timeout: failures, availability, latency | `a5_timeout.svg`, `a5_latency.svg` |

`demo.txt` (the partition scenario) is a transcript, not a measurement, so it has no chart. The Merkle, lazy-tree and fixed-partition experiments (A6 to A8) are on separate branches and have their own charts there.

Chart code is the `chart` package (about 190 lines, standard library only).
