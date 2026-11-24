# Ablations: closing the gaps in the decision audit

Branch `ablations` (on top of `fixed-merkle`). Each experiment changes one design choice and measures it. Runs are repeated 3 times with different latency seeds and reported as mean (min-max). Regenerate with `go run ./cmd/bench -ablate` (X1 to X8, writes `results/ablations.md`) and `go run ./cmd/archbench` (A9, A10). Every chart is in `results/` and listed in [GRAPHS.md](GRAPHS.md). This answers [DECISIONS_AUDIT.md](../reference/DECISIONS_AUDIT.md).

## Headline: the ablations found a real bug

**Hinted handoff never finished once an owner had many hints.** The handoff loop gave one 20 ms deadline to *all* of an owner's keys and kept every hint unless every send succeeded. With about 15 or more hinted keys the later sends always timed out, so nothing was ever pruned and the same hints were retried forever. X6 first showed "10 s, never drained". Fix: one timeout per send, keep what was delivered, stop the round at the first failure. Regression test `TestHandoffDeliversManyHints` (150 hints) failed before and passes now. Since this is in `main` too, the fix is also on its own branch `fix-handoff-timeout` off `main`, not merged.

Earlier results that depended on hints draining (A3's "hinted handoff only" row, E3 after heal) were regenerated; the conclusions held because anti-entropy repaired what handoff missed, which is exactly the kind of masking an ablation catches.

## Results and what each means for your decisions

| # | Question | Result | Recommendation |
|---|---|---|---|
| X1 | How noisy is one run? | Very little: stale % at R=W=1 is 26.2 (26.1 to 26.4). The earlier worry was too pessimistic. Read repair on vs off: 26.2 vs 26.1, ranges overlap | Single 2 s runs were adequate. Drop any claim that read repair improves freshness. |
| X2 | Does the latency model matter? | Yes, a lot. R=W=1 stale reads: 8.3% (LAN), 26.2% (default), 40.7% (cross-region). Conclusions that do not change: R+W>N is 0% stale in all three. A fixed 20 ms timeout fails 23% of puts at 5 ms hops and 5 ms fails 100% | Report the stale numbers as a range across latency models. Make `ReplicaTimeout` scale with the hop time (about 4x the tail), not a constant. |
| X3 | How often should anti-entropy run? | 20 ms: repairs in 126 ms, costs 192 msgs/s idle. **100 ms: 404 ms, 37 msgs/s.** 500 ms: 2.3 s, 9 msgs/s. 2 s: 6 s, 3 msgs/s | Add the loop with a 100 ms default in the sim (not yet default: it changes every benchmark). Real clusters would use seconds to minutes. |
| X4 | Does node count or N matter? | Node count no: 5 vs 7 nodes at N=3 both 26.4%. N yes: R=W=1 stale is 15.2% at N=2, 26.4% at N=3, 23.4% at N=5. Majority quorum is 0% stale at every N | N=3 is fine. Quote staleness per N. |
| X5 | Does workload shape matter? | Yes: R=W=1 stale reads range 9.8% (256 keys) to 26.5% (32 keys) and 12.2% when writers are slow. R=1,W=2 stays between 0.1% and 1.2% | Report stale reads as a range with the workload stated. |
| X6 | Handoff interval | Time to deliver all hints after heal: 115 ms at 1, 5 and 20 ms, 151 ms at 100 ms, 563 ms at 500 ms (after the bug fix) | Keep 5 ms. Anything up to 20 ms is the same. |
| X7 | What does read repair cost, and is querying all N replicas worth it? | Read repair adds 0.3 messages per operation (3.0 to 3.3, +10%) and changes no staleness. Asking only R replicas saves 15% of messages (2.8 vs 3.3) but doubles read p99 (5.1 to 10.7 ms), and with a slow replica 9.8 to 20.0 ms | Keep querying all N. Keep read repair (cheap, helps keys that are read) but do not call it a freshness feature. |
| X8 | Wrong failure detector, other partition shapes | At 20% false suspicion: 0.2% stale, +6% messages, about 150 parked hints, **0 failed puts**. Shapes (R=W=2), strict vs sloppy availability: 1 vs 4 nodes 72.8 / 82.0%; 2 vs 3 48.5 / 100%; 2 vs 2 vs 1 26.0 / 88.9%; flapping 85.7 / 99.1%. No acknowledged write lost and replicas agreed after heal in every case | The exact detector flatters nothing: a bad one costs a little traffic, not correctness. Strict is very unavailable in 3-way splits. |
| A9 | How many fixed partitions Q, and why not modulo? | Spread (max-min share, points): Q=16, 32, 64 about 18.9; Q=128 2.5; **Q=256 1.0**; Q=512 1.4; Q=4096 0.5. Modulo hashing is perfectly balanced (0.2) but moves **40%** of copies on a join, twice the ideal 20% | Q=256 is the smallest Q with balance near 1 point. Modulo rejection is now measured. |
| A10 | Walk the Merkle tree level by level or fetch it at once? | At depth 4: 70 round trips level by level, 40 one-shot, but one-shot sends 52% more bytes at 1% divergence (229 vs 151 KB) and 80% more hashes | Keep level by level (bytes are the scarce resource in anti-entropy). One-shot only if round trips cost far more than bandwidth. |

Charts: `x1_noise`, `x2_stale`, `x2_timeout`, `x3_converge`, `x3_idle`, `x4_cluster`, `x5_workload`, `x6_handoff`, `x7_repair`, `x7_fanout`, `x8_suspicion`, `x8_shapes`, `a9_q_balance`, `a9_q_moved`, `a10_roundtrips`, `a10_kb` (all `.svg` in `results/charts/`).

## Details worth knowing

* **Q and WithNode.** Balance after a join depends on how the new node's evenly spaced partitions interleave with the old round-robin owners, not only on Q. At Q=16 to 64 the stride skips a donor and leaves an 18.9-point spread. I tried a version that takes an equal number from every donor; it was worse (6 to 46 points), because pieces taken from different donors landed side by side. A smarter join (choose partitions to balance each node's *neighbours*, not its count) would remove the Q sensitivity. Not built.
* **Idle cost of periodic anti-entropy** is dominated by sending one root hash for every shared partition (about 77 per pair) each round. Sending a single combined hash first would cut idle traffic by roughly that factor.
* **Round trips versus bytes.** The simulator charges wall-clock time per round trip, so one-shot looks faster (171 vs 281 ms at depth 4); on a real network the extra bytes may cost more than the saved latency.

## Still not tested

* Sibling and vector-clock growth with many writers during a long partition (no cap exists).
* Mixed R,W settings across clients in one run (D6 claims this works).
* Real-network behaviour (only on the `real-network-and-wal` branch).
* Hash-collision risk of the 64-bit XOR leaf hashes (about 2^-64, argued not measured).
* Dynamic membership: a node joining a running cluster. Join and leave are measured on the ring only.

## Code added

`Network.SetFalseSuspicion` and `Sent`; `NodeConfig.AntiEntropyInterval`, `ReadFromROnly`, `MerkleOneShot`; `Config.FalseSuspicion`; `chart.BarErr` (whiskers); the `-ablate` mode of `cmd/bench`; A9 and A10 in `cmd/archbench`. All off by default, so the default behaviour is unchanged except for the handoff fix.
