# Decision audit: what is proven, what is not

Every design choice in the project, rated by how much evidence stands behind it today. Written 2026-11-24 after the Merkle, fixed-partition and vnode experiments. "Run" means an experiment that exists in the repo and was executed; "reasoned" means an argument in the docs but no measurement.

**Ratings:** PROVEN (an experiment isolates the choice and the result is clear), PARTIAL (evidence exists but is thin or confounded), NONE (reasoned or arbitrary, nothing measured).

## 1. The five that matter most for your report

These could change a headline number or a conclusion, so decide on them first.

| # | Decision | Rating | Why it matters | Cost to prove |
|---|---|---|---|---|
| 1 | **The latency model** (replica hop 0.4 ms + Exp(0.8 ms), 3% of messages +5 ms; client hop 0.1 ms + Exp(0.1 ms)) | NONE | Every latency, stale-read %, availability and timeout result is a function of these hand-picked numbers. D15 says they were chosen so stale reads look realistic. They were never varied. | Small: rerun E1 and A5 with 3 latency models (fast LAN, default, cross-region) and plot stale % and timeout against them. |
| 2 | **One run per table row, no repeats** | NONE | E1 to E3 and A1 to A5 are single 2-second runs (A2 sums 3). Goroutine scheduling is not deterministic, so small differences are noise. Example: "read repair barely changes staleness" rests on 21.5% vs 21.8%, one run each. | Small: repeat each row 5 times, report mean and min/max, draw error bars. |
| 3 | **Anti-entropy is never triggered by the system** | NONE | D13 says "triggered after heal", but nothing does that. The only background loop is hinted handoff. Anti-entropy runs only when a benchmark or test calls `Cluster.AntiEntropy()`. So the store as built does not converge by itself after a partition without hints. The interval, who starts it, and which peer to pick are all undecided. | Medium: add a periodic loop (pick a random peer every T ms), then sweep T against time-to-converge and traffic. |
| 4 | **5 nodes, N=3** | NONE | Reasoned from "need fallbacks for sloppy quorums" (D4). No run varies N or the node count. N=3 is the paper's default. The stale-read and availability numbers are specific to it. | Small: run E1 and E3 at N=2, 3, 5 on 5 and 7 nodes. |
| 5 | **The workloads** (32 keys with one writer each and 16 readers; 64 keys; 8 clients on one key; no hot-key skew; no mixed read/write ratios; 2 s per row) | NONE | Staleness depends on how often a read lands right after a write to the same key. A different key count or read/write ratio changes the percentages. | Small: sweep key count (8, 32, 256) and write fraction for E1 at two R,W settings. |

## 2. Decision by decision

### Proven or mostly proven

| Decision | Rating | Evidence | Weakness |
|---|---|---|---|
| Dotted version vectors over plain vector clocks (D9) | PROVEN | A2/E2: 0 lost vs 19.6% / 25.0% lost; `TestSameCoordinatorConcurrentWrites` | The second alternative (clocks keyed by client id, "3 items lost") exists only as an anecdote. It is not in the code and cannot be reproduced. Drop it from the report or rebuild it. |
| Replicate whole sibling set (D10) | PARTIAL | A2: 0 vs 1 and 3 lost items | Counts of 1 and 3 over thousands of writes, 3 runs. Real but statistically thin. Repeat 10x to confirm. The correctness argument is the stronger evidence. |
| Siblings + client union beats LWW (D11) | PROVEN | E2: 0 vs 84% lost; scenario tests | Union is the only resolver tried, which suits a cart; other data types need other rules. |
| Hinted handoff + anti-entropy together (D13) | PROVEN | A3: only the pair reaches 0% unrepaired | Measured on one partition shape ({n1,n2} vs {n3,n4,n5}), one write rate. |
| Strict quorums as default (D8) | PROVEN | E3: 0% stale strict vs 87-92% sloppy at R=W=2 | A design choice about semantics more than a finding. |
| Wait for W then reply, rest async (D7) | PROVEN | E1/E1b latency vs W | Not compared against waiting for all N in the same harness. |
| Random coordinator vs sticky (D5) | PROVEN, but judgement | A4: 25.7% vs 0.1% stale; load 23% vs 31% | The trade-off is real, and which side to default to is your call (see section 3). |
| Failed quorum write is not rolled back (D12) | PARTIAL | Demo case 2 | One scenario. |
| 20 ms replica timeout (D14) | PARTIAL | A5: 4 timeouts tried; 20 ms is the first with 0% spurious failures | Depends on the latency model (decision 1 above). The client timeout of 3x was never tested. |
| MD5 vs FNV-1a hash (D3) | PARTIAL | A1: MD5 CV 0.056 vs FNV 0.401 at 64 vnodes | The cause ("FNV mixes short similar strings poorly") is asserted, not tested. The claim that FNV with 16 vnodes "left a node with no keys" is not in any committed experiment (A1 shows 27.9% minimum for FNV at 64). |
| 64 virtual nodes (D3) | PARTIAL | A1 (4 counts, 1 placement), now A1b (11 counts, 20 placements) | A1b shows 64 is defensible, not optimal: imbalance falls like 1/sqrt(vnodes), 128-256 is better at no measured cost. See [VNODE_ABLATION.md](VNODE_ABLATION.md). |
| Merkle anti-entropy over full-state (A6, branch) | PROVEN | 97% fewer bytes at 1% divergence; randomized equivalence test | Costs 13 round trips per pair; wall-clock was slower than full-state at 1-10% divergence in the sim. |
| Eager vs lazy trees (A7, branch) | PARTIAL | CPU microbenchmark via `Inject` | Excludes the network and lock contention. Chosen eager for simplicity, which is fine. |
| Fixed partitions beat random vnodes (A8, branch) | PROVEN for balance and data moved | 59.8-60.8% shares; 19.9% moved on join | Q=256 was your pick, not swept. No Q sweep exists. |

### No proof

| Decision | Where | Rating | What is unknown |
|---|---|---|---|
| Exact failure detector (`Reachable`) | D2, network.go | NONE | Real detectors give false suspicions and take time to notice. Sloppy-quorum availability (100%) is best case. Flapping nodes were never tested. |
| Modulo hashing rejected | D3 | NONE | Argued ("remaps almost every key"), never measured. Cheap to measure: same A8 join test with `hash % N`. |
| Hinted handoff every 5 ms | cluster.go | NONE | Interval never swept. Hints have no size cap and no expiry. |
| Hint owner = first preferred node | coordinator.go | NONE | When a sloppy coordinator is itself not a replica, it files the hint under the first preferred node regardless of which replica is actually down. Untested; probably harmless but unproven. |
| Read repair on for every read | NodeConfig | PARTIAL | A3 shows it helps convergence only when you deliberately read every key at R=3. E1 shows no freshness gain (21.5% vs 21.8%). Its traffic cost was never measured. |
| Requests go to all N replicas, answer after the first R/W | coordinator.go | NONE | Reading from only R replicas would cut load; never compared. |
| Client timeout = 3 x replica timeout | cluster.go | NONE | Arbitrary. |
| Sibling and clock growth | version.go | NONE | No cap and no clock truncation (the Dynamo paper truncates). Max observed was 11 siblings with 8 writers. Many writers during a long partition is untested. |
| Single partition shape | E3, A3 | NONE | Only {n1,n2} vs {n3,n4,n5}. No asymmetric, partial or repeated partitions, and no node crashes on `main`. |
| Same R,W for every client in a run | E1-E3 | NONE | D6 argues mixed R,W per client is a feature, but no experiment mixes them. |
| LWW tie-break (timestamp, then node id) | merge.go | NONE | Arbitrary but deterministic; only matters for the LWW comparison. |
| Per-peer vs per-partition Merkle trees, depth, hash function (branch) | antientropy.go | PARTIAL | Depth was swept for per-peer trees only. The 64-bit XOR leaf hash and FNV key hash were not tested for collisions (negligible at 2^-64 but unargued). |
| Tree walks one level per round trip | antientropy.go | NONE | Sending several levels per request would cut the 13 round trips; never tried. |

### Not decided at all (gaps, not choices)

* No deletes or tombstones.
* No persistence on `main` (the WAL exists on `real-network-and-wal`).
* No dynamic membership: the cluster cannot add or remove a node at runtime. Join and leave costs are measured offline on rings only.
* Real-network results (Docker/TCP, iptables partitions) exist only on `real-network-and-wal`; none of them back the `main` numbers.
* Value sizes are tiny (32 bytes); bandwidth conclusions from A6 scale with them.

## 3. Calls that are yours

These are judgement calls where more data will not settle it:

1. **Default coordinator: random (25.7% stale at R=W=1, even load) or sticky (0.1% stale, 31% on the busiest node, freshness lost when that node fails).** I would keep random and tell users to set R+W>N for freshness, as the docs do.
2. **Strict or sloppy quorums by default.** Strict is the only mode where R+W>N means fresh. Sloppy gives 99%+ availability and 87-92% stale reads at R=W=2. Keep strict.
3. **Number of vnodes, if you keep random ones: 64 or 256.** 256 halves imbalance and costs nothing measurable in the simulation. If you take fixed partitions, this goes away.
4. **Eager vs lazy trees.** Eager (your pick) is simpler; lazy only wins when keys are rewritten often.

## 4. What I would run next, in order

| Order | Experiment | Closes | Effort |
|---|---|---|---|
| 1 | Repeat every row 5x with different seeds; add error bars | decision 2 | small |
| 2 | Latency-model sensitivity (3 models) on E1 and A5 | decision 1 | small |
| 3 | Add periodic anti-entropy; sweep interval vs time-to-converge | decision 3 | medium |
| 4 | N and node-count sweep on E1 and E3 | decision 4 | small |
| 5 | Workload sweep (keys, read/write ratio, hot key) | decision 5 | small |
| 6 | Q sweep (64, 128, 256, 1024) for fixed partitions; modulo hashing baseline | Q=256, modulo | small |
| 7 | Handoff interval sweep; read repair on/off with traffic counted; read from R vs all N | handoff, read repair, fan-out | medium |
| 8 | False-suspicion failure detector; partial and flapping partitions | exact detector, partition shape | larger |

I can do any of these on a branch off the current design. 1, 2, 3 and 6 are the ones I would do before you write the report.
