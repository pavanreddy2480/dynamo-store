# Dynamo-inspired store with configurable consistency: report

The store runs in two ways, from the same node code:

* **Simulation** (sections 2–5): all nodes are in one process with a simulated network. Numbers come from `go run ./cmd/bench -duration 2s` ([results/benchmark.md](results/benchmark.md)) and `go run ./cmd/demo` ([results/demo.txt](results/demo.txt)). Latencies are simulated delays plus Go scheduling, so treat them as relative.
* **Real processes** (sections 6–7): each node is its own process in a Docker container talking TCP, partitions are iptables rules, crashes are `kill -9`, and every node has a write-ahead log on its own volume. Numbers come from `go run ./cmd/realbench -duration 4s` ([results/realnet.md](results/realnet.md)) on a 4-CPU Linux host shared by all containers.

## 1. What was built

| Requirement | Implementation |
|---|---|
| Replicated KV store, 3 replicas per key | 5 nodes on an MD5 consistent-hash ring (64 vnodes each). Each key is stored on the first N=3 distinct nodes clockwise (its preference list). |
| R and W as client settings | `cluster.NewClient(id, R, W)`. R and W travel with every request, so two clients can use different quorums against the same data. |
| Track concurrent versions | Each write gets a **dotted version vector**: a dot `(node, counter)` naming the write, plus the vector clock of what the writer had read. A replica drops a version only when another version's context contains its dot. Otherwise both are kept as siblings. |
| Return conflicting versions | `Get` returns every sibling plus a merged context. Passing that context to `Put` supersedes all of them. |
| Explicit resolution policy | `siblings+client-reconcile` (default): the client merges siblings with a declared rule, here **set union** (`UnionResolver`). `last-writer-wins` is included for comparison: the replica keeps only the highest (timestamp, node). |
| Partitions | An in-memory network delivers each message after a sampled delay. A partition silently drops messages between groups, so senders only learn about it through timeouts. |
| Reconnect | Hinted handoff (for sloppy quorums), read repair, and a pairwise anti-entropy round (a full-state exchange standing in for Merkle trees). |
| Real deployment | The same node code runs as separate processes (`cmd/dynode`) over TCP, one per Docker container, with a heartbeat failure detector. |
| Crash recovery | A write-ahead log per node (fsync, async or none), replayed on restart. |

The request path follows Dynamo. The client sends the request to a random reachable replica in the key's preference list, and that replica coordinates. It sends to all N replicas and answers once W acks (or R replies) arrive. The remaining replicas keep receiving the write in the background, and that lag is what produces stale reads. With `Sloppy` set, the coordinator replaces unreachable preferred replicas with the next live nodes on the ring. Those nodes store the write as a hint and hand it back when the owner is reachable again.

## 2. Partition, update both sides, reconnect: verifying the rule

Scenario: key `cart:alice` = {milk} is written to all 3 replicas. Two clients read it, then the network is partitioned so the replicas split 1 | 2. Client A adds `eggs` on the minority side and client B adds `bread` on the majority side. The partition then heals, hints drain, one anti-entropy round runs, and an R=3 read follows.

| Variant | A's put | B's put | Versions returned after heal | Final value on all 3 replicas | Rule holds | Acked items kept |
|---|---|---|---|---|---|---|
| siblings, R=1 W=1 strict | ok | ok | 2 | {bread, eggs, milk} | yes | yes |
| siblings, R=2 W=2 strict | **fails** (1/2 acks) | ok | 2 | {bread, eggs, milk} | yes | yes |
| siblings, R=2 W=2 sloppy | ok (via a fallback node) | ok | 2 | {bread, eggs, milk} | yes | yes |
| LWW, R=1 W=1 | ok | ok | 1 | {bread, milk} | yes | **no**: eggs lost |
| LWW, minority clock +50ms | ok | ok | 1 | {eggs, milk} | yes | **no**: bread lost, though it was written later |

The vector clocks correctly mark the two partition-side writes as concurrent. Neither write supersedes the other, both come back as siblings, and the client's union merge followed by a write under the merged context collapses them into one version on every replica. `TestPartitionScenarios` asserts all five rows.

Two behaviours worth noting:
* **A failed quorum write is not a rollback.** In the strict W=2 case A's put was rejected, but it still landed on the one replica A could reach, and it reappears as a sibling after healing. Dynamo has the same behaviour: a write that times out has an unknown outcome, not a failed one. The union rule handles this safely because it never removes data.
* **LWW resolves quickly but loses acknowledged writes**, and with clock skew it loses the write that actually came last.

## 3. Stale-read frequency vs quorum (no failures)

A read counts as stale if some write acknowledged before the read started is neither returned nor superseded by a returned version. The workload is 32 keys with one writer each and 16 readers.

| R,W | R+W>N | stale reads | get p50 / p99 ms | put p50 / p99 ms |
|---|---|---|---|---|
| 1,1 | no | **21.5%** | 0.69 / 6.16 | 0.67 / 5.75 |
| 1,2 | no | 0.4% | 1.04 / 4.92 | 3.92 / 9.94 |
| 2,1 | no | 0.8% | 3.29 / 9.49 | 0.68 / 5.42 |
| 1,3 | yes | 0% | 1.25 / 4.40 | 5.41 / 12.58 |
| 3,1 | yes | 0% | 5.31 / 12.76 | 0.74 / 4.68 |
| 2,2 | yes | 0% | 4.29 / 7.81 | 4.29 / 7.88 |
| 3,3 | yes | 0% | 5.66 / 12.17 | 5.65 / 12.32 |

* Every configuration with R+W>N had **zero** stale reads across about 50k reads. Read and write quorums must overlap, so every read reaches at least one replica with the latest acknowledged write.
* R+W≤N gives stale reads. R=W=1 is stale about a fifth of the time: a read often lands on a replica that the write hasn't reached yet. Raising either quorum to 2 cuts this to under 1%, because only one replica can then be behind.
* Latency is the cost. Each step up in R or W adds roughly one replica round trip and, more importantly, ties the operation to the slower replicas. With one slow node (E1b in the results file), p99 at R=3 or W=3 rises to about 22ms, versus about 9ms at R=1 or W=1.
* Read repair barely changes these numbers. It fixes a replica only after a stale read has already happened, and with continuous writes the next write makes the replica stale again.

## 4. Concurrent updates: returned versions

Eight clients read-modify-write one key at the same time. Each one reads, merges any siblings by union, adds a unique item, and writes back.

| Policy / clocks | R,W | avg versions per read | reads with siblings | max siblings | acked items lost |
|---|---|---|---|---|---|
| siblings, dotted version vectors | 1,1 | 5.65 | 99.2% | 11 | **0** of 2528 |
| siblings, dotted version vectors | 2,2 | 4.51 | 97.7% | 8 | **0** of 1727 |
| siblings, dotted version vectors | 3,3 | 3.58 | 91.3% | 8 | **0** of 1283 |
| siblings, per-node vector clocks (Dynamo paper) | 1,1 | 2.94 | 94.2% | 6 | 585 of 3055 |
| siblings, per-node vector clocks | 3,3 | 2.28 | 84.7% | 4 | 339 of 1302 |
| last-writer-wins | 1,1 | 1.00 | 0% | 1 | 4572 of 5426 |
| last-writer-wins | 3,3 | 0.99 | 0% | 1 | 903 of 1228 |

* Under heavy contention almost every read returns siblings, up to one per concurrent writer. Higher quorums return fewer, because reads see more of the history.
* **Why dotted version vectors.** The first version used plain per-node vector clocks, as in the Dynamo paper. When two clients write through the same coordinator from the same context, the coordinator increments its own entry twice, so the second write looks like it descends from the first and silently replaces it. That lost 19–28% of acknowledged items in this test. Keying the clock by client id instead also failed: a client whose read was stale would bump its counter past a write it had never seen. Dots fix this. A write supersedes another only if its context contains that write's dot. The coordinator also replicates its whole sibling set rather than only the new version (as Riak does), so a context assembled from one replica can't claim a write that replica lacks. `TestNoLostUpdatesUnderConcurrency` and `TestSameCoordinatorConcurrentWrites` guard this.
* LWW never shows conflicts because it throws them away: 84% of acknowledged items were lost at R=W=1.

## 5. Availability under a partition

Nodes are split {n1,n2} (A, minority) | {n3,n4,n5} (B, majority), with 6 clients per side and 64 keys. Clients on each side read, and half the time write back what they read. Availability is the fraction of attempted operations that succeeded.

| Quorum | R,W | A get / put | B get / put | stale reads A / B | acked writes missing after heal |
|---|---|---|---|---|---|
| strict | 1,1 | 89% / 89% | 100% / 100% | 97% / 86% | 0 |
| strict | 2,2 | 28% / 28% | 72% / 74% | **0% / 0%** | 0 |
| strict | 3,3 | 0% / 0% | 11% / 13% | 0% / 0% | 0 |
| strict | 1,3 | 88% / 0% | 100% / 13% | 0% / 0% | 0 |
| sloppy + hinted handoff | 1,1 | 100% / 100% | 100% / 100% | 97% / 96% | 0 |
| sloppy + hinted handoff | 2,2 | 100% / 100% | 99% / 100% | 92% / 87% | 0 |
| sloppy + hinted handoff | 3,3 | 0% / 0% | 100% / 100% | 0% | 0 |

* **Strict quorums trade availability for consistency, key by key.** A key stays readable or writable on a side only if that side holds R or W of its 3 replicas. With R=W=2, A can serve the 28% of keys with 2 replicas on its side and B the other 72%. No reads are stale, and both sides can never write the same key.
* **Sloppy quorums restore availability** wherever a side has at least max(R,W) live nodes (W=3 is still impossible on the 2-node side). The cost is that R+W>N no longer prevents stale reads: R=W=2 is 87–92% stale during the partition, because each side writes to different nodes.
* "Stale" here mostly means a client didn't see writes from the other side of the partition, which is unavoidable in any mode where both sides accept writes.
* **After reconnecting**, in every configuration, hinted handoff plus one anti-entropy round left all replicas of all 64 keys identical, and **no acknowledged write was missing**. Concurrent writes from the two sides survive as siblings (51–64 of the 64 keys), ready for the client's union merge.

## 6. Real processes over a real network

Each of the 5 nodes runs `dynode` in its own container (an image built `FROM scratch` holding two static Go binaries). Nodes talk gob over TCP with pooled connections and find out who is down with a heartbeat failure detector (probe every 100ms, suspect after 600ms) instead of the simulation's instant oracle. Load generators run in their own containers and record every operation; stale reads, availability and lost writes are computed afterwards from the merged histories.

### Quorum sweep (R1, no failures, fsync WAL)

| links | R,W | get p50 / p99 ms | put p50 / p99 ms | ops/s | stale reads |
|---|---|---|---|---|---|
| LAN | 1,1 | 0.92 / 13.6 | 2.58 / 17.9 | 3166 | 0.4% |
| LAN | 2,2 | 1.37 / 12.5 | 3.30 / 16.6 | 2796 | 0% |
| LAN | 3,3 | 1.81 / 8.2 | 3.99 / 12.9 | 2647 | 0% |
| cross-zone (emulated) | 1,1 | 0.80 / 12.7 | 3.11 / 20.0 | 3059 | 0.8% |
| cross-zone (emulated) | 2,2 | 3.60 / 7.2 | 4.51 / 8.7 | 1951 | 0% |
| cross-zone (emulated) | 3,3 | 4.72 / 9.4 | 5.63 / 10.5 | 1513 | 0% |

* As in the simulation, every R+W>3 setting had **zero** stale reads, and higher quorums cost latency and throughput (with cross-zone links, R=W=3 is half the throughput of R=W=1).
* **Stale reads at R=W=1 were far rarer on real processes (0.4–0.8%) than in the simulation (22%).** Two things differ. First, on a real bridge network a replica receives the write within a fraction of a millisecond, which is shorter than the time it takes the next read to arrive (each operation here takes 1–3ms end to end, including gob encoding, syscalls and fsync). Second, the workloads differ: the simulation had 16 readers polling keys that a dedicated writer had just updated, while here each client reads a random key, which is rarely one it just wrote. The simulation's number is a worst case for a hot key under slow replication; this one is closer to a typical LAN workload. Both agree on the rule that matters: R+W≤N allows stale reads, R+W>N does not.
* The "cross-zone" rows add an emulated delay inside the transport because the host has no `tc netem`. Real cross-zone links would also have more variance.

### Partition with real network rules (R2)

Every packet between {n1, n2, load generator A} and {n3, n4, n5, load generator B} is dropped by iptables on the Docker bridge. The nodes only learn about the partition through timeouts and missed heartbeats.

| quorum | R,W | A get / put available | B get / put available | stale A / B | acked writes lost after heal | replicas converged |
|---|---|---|---|---|---|---|
| strict | 1,1 | 89% / 90% | 100% / 100% | 99% / 86% | 0 of 5968 | yes |
| strict | 2,2 | 27% / 27% | 72% / 70% | **0% / 0%** | 0 of 932 | yes |
| strict | 3,3 | 0% / 0% | 11% / 10% | 0% | 0 of 42 | yes |
| sloppy | 1,1 | 100% / 99% | 100% / 100% | 97% / 97% | 0 of 4763 | yes |
| sloppy | 2,2 | 99% / 98% | 100% / 99% | 97% / 97% | 0 of 4014 | yes |

These match the simulation closely (strict R=W=2: 28% / 72% simulated, 27% / 72% real). Sloppy availability is just under 100% because, unlike the simulation's oracle, the real failure detector needs up to 600ms to notice the partition, and requests routed to the far side before then time out. After the rules were removed, hinted handoff and anti-entropy brought every replica of every key to the same state, and no acknowledged write was missing.

## 7. Crashes and durability

Every node now writes each change to a **write-ahead log** before acknowledging it. Records are CRC-checked so a half-written record at the end of the file is ignored on restart. There are three modes:

* **fsync**: the record is on disk before the ack. Concurrent writers share one fsync (group commit).
* **async**: records are buffered in the process and flushed every 10ms or every 1s. The ack does not wait.
* **none**: memory only.

### One node killed with `kill -9` (R3)

n3 is killed mid-run, restarted from its volume while still cut off by iptables (so its recovered state can be inspected before anything repairs it), then reconnected.

| WAL mode | W | writes n3 acknowledged before the kill | missing from n3 after restart | acked writes lost for good | put p50 ms |
|---|---|---|---|---|---|
| fsync | 1 | 246 | **0** | 0 of 4114 | 2.66 |
| fsync | 2 | 202 | **0** | 0 of 4086 | 3.66 |
| async 10ms | 1 | 385 | 1 (0.3%) | 0 of 6983 | 1.01 |
| async 1s | 1 | 460 | 426 (93%) | 0 of 7129 | 0.96 |
| none | 1 | 511 | 511 (100%) | 0 of 8045 | 0.81 |
| none | 2 | 434 | 434 (100%) | 0 of 7236 | 1.31 |

Only fsync kept everything on the crashed node. Yet **no acknowledged write was lost cluster-wide in any mode**: the other replicas still had the data, and anti-entropy and read repair refilled n3 after it rejoined. With one node down, replication is what provides durability. A restart (replay plus checkpoint) took about 0.6s.

### All five nodes killed at once (R3b)

| WAL mode | acknowledged writes | lost after restart |
|---|---|---|
| fsync | 1651 | **0** |
| async 10ms | 3069 | 2 (0.1%) |
| async 1s | 2321 | 491 (21%) |
| none | 2523 | **2523 (100%)** |

When every replica crashes together (a power cut, a bad deploy), replication can't help and only the log can. fsync lost nothing. Memory-only lost everything. Async lost roughly the last flush interval. SIGKILL ends the process but not the machine, so writes already handed to the OS survived. A real power cut would also lose the OS cache, which only fsync covers.

### What durability costs (R4, R=W=2, no failures)

| WAL mode | put p50 / p99 ms | ops/s |
|---|---|---|
| none | 1.10 / 8.5 | 5048 |
| async 10ms | 1.14 / 6.1 | 5727 |
| fsync | 3.09 / 14.0 | 3062 |

fsync makes writes about 3× slower at the median and costs about 40% of throughput on this host. Group commit keeps that cost from growing with the number of concurrent writers. Async at 10ms costs almost nothing and, in these tests, lost almost nothing.

## 8. Takeaways

1. R+W>N with strict quorums gave 0 stale reads in every run, at the cost of tail latency and of minority-side availability during partitions.
2. R=W=1 is the fastest and most available setting, but it allows stale reads. How many depends on the workload and on replication lag: 25% for a hot key in the simulation, 0.4–0.8% on real containers.
3. Sloppy quorums make the store always writable, but they also drop the R+W>N guarantee. Conflicts are then normal and the resolution rule does the work.
4. Version vectors only help if dominance is computed correctly. Plain per-node clocks silently lose concurrent writes, and dotted version vectors fixed that in these tests.
5. An explicit, data-preserving merge (union) kept every acknowledged write. LWW is simpler but lost acknowledged writes in every concurrent test.
6. The simulation's partition results held up on real processes with real firewall rules: the same availability split, zero stale reads for strict R+W>N, and full convergence with no lost writes after healing.
7. Replication protects against one node crashing, even with no log at all. The write-ahead log with fsync protects against everything crashing at once, at about 3× the write latency.

## Limitations

* The real-process runs share one 4-CPU host, so containers compete for CPU and the "network" is a local bridge. Cross-zone latency is emulated in the transport, not by the kernel.
* SIGKILL tests process crashes. Power loss (losing the OS page cache) was not tested; fsync is the only mode that would survive it.
* The simulation's failure detector is an oracle. The real one is heartbeat-based with a fixed 600ms suspicion timeout, not gossip or phi-accrual, and membership is static.
* The WAL is rewritten as a compact checkpoint only at startup, so it grows without bound while a node runs.
* Anti-entropy exchanges full state instead of Merkle trees, which is fine at this scale.
* Union as a merge can't express deletes. That needs tombstones or a CRDT such as an OR-set, as in Dynamo's shopping-cart caveat.
