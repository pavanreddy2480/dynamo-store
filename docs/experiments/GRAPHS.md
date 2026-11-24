# Graphs for every experiment

Charts live in `results/charts/` as SVG (open in a browser; hover a bar or point for its exact value). `go run ./cmd/plots` redraws all of them from the committed tables in `results/benchmark.md` and `results/architecture.md`, so a chart always matches the numbers next to it. A1b's charts are drawn by `go run ./cmd/archbench` itself.

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

Ablation charts (see [ABLATIONS.md](ABLATIONS.md)): X1 `x1_noise`, X2 `x2_stale` `x2_timeout`, X3 `x3_converge` `x3_idle`, X4 `x4_cluster`, X5 `x5_workload`, X6 `x6_handoff`, X7 `x7_repair` `x7_fanout`, X8 `x8_suspicion` `x8_shapes`, A9 `a9_q_balance` `a9_q_moved`, A10 `a10_roundtrips` `a10_kb`. A6 and A8 charts (Merkle, ring placement) are drawn by archbench too. X and A charts come from `cmd/bench -ablate` and `cmd/archbench`; E and A1 to A5 charts from `cmd/plots`.

`demo.txt` (the partition scenario) is a transcript, not a measurement, so it has no chart.

Chart code is the `chart` package (about 190 lines, standard library only).
