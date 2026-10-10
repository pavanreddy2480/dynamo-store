# Dynamo-inspired store with configurable consistency: report

All numbers below come from `go run ./cmd/bench -duration 2s` ([results/benchmark.md](results/benchmark.md)) and `go run ./cmd/demo` ([results/demo.txt](results/demo.txt)). The simulation runs in one process, so latencies depend on Go's timers and the load on the machine. Treat them as relative, not absolute.

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
| 1,1 | no | **25.4%** | 0.69 / 5.99 | 0.56 / 5.52 |
| 1,2 | no | 0.4% | 1.35 / 4.69 | 4.21 / 9.66 |
| 2,1 | no | 0.9% | 3.53 / 8.99 | 0.61 / 4.68 |
| 1,3 | yes | 0% | 1.46 / 4.03 | 5.68 / 12.48 |
| 3,1 | yes | 0% | 5.36 / 12.80 | 0.62 / 4.60 |
| 2,2 | yes | 0% | 4.37 / 8.29 | 4.35 / 8.28 |
| 3,3 | yes | 0% | 5.71 / 12.52 | 5.72 / 12.69 |

* Every configuration with R+W>N had **zero** stale reads across about 50k reads. Read and write quorums must overlap, so every read reaches at least one replica with the latest acknowledged write.
* R+W≤N gives stale reads. R=W=1 is stale a quarter of the time: a read often lands on a replica that the write hasn't reached yet. Raising either quorum to 2 cuts this to under 1%, because only one replica can then be behind.
* Latency is the cost. Each step up in R or W adds roughly one replica round trip and, more importantly, ties the operation to the slower replicas. With one slow node (E1b in the results file), p99 at R=3 or W=3 rises to about 22ms, versus about 9ms at R=1 or W=1.
* Read repair barely changes these numbers. It fixes a replica only after a stale read has already happened, and with continuous writes the next write makes the replica stale again.

## 4. Concurrent updates: returned versions

Eight clients read-modify-write one key at the same time. Each one reads, merges any siblings by union, adds a unique item, and writes back.

| Policy / clocks | R,W | avg versions per read | reads with siblings | max siblings | acked items lost |
|---|---|---|---|---|---|
| siblings, dotted version vectors | 1,1 | 5.76 | 99.5% | 11 | **0** of 2693 |
| siblings, dotted version vectors | 2,2 | 4.84 | 97.2% | 8 | **0** of 1729 |
| siblings, dotted version vectors | 3,3 | 3.66 | 94.7% | 8 | **0** of 1267 |
| siblings, per-node vector clocks (Dynamo paper) | 1,1 | 2.97 | 93.4% | 7 | 653 of 3216 |
| siblings, per-node vector clocks | 3,3 | 2.23 | 81.1% | 4 | 328 of 1287 |
| last-writer-wins | 1,1 | 1.00 | 0% | 1 | 4621 of 5453 |
| last-writer-wins | 3,3 | 0.99 | 0% | 1 | 917 of 1236 |

* Under heavy contention almost every read returns siblings, up to one per concurrent writer. Higher quorums return fewer, because reads see more of the history.
* **Why dotted version vectors.** The first version used plain per-node vector clocks, as in the Dynamo paper. When two clients write through the same coordinator from the same context, the coordinator increments its own entry twice, so the second write looks like it descends from the first and silently replaces it. That lost 20–28% of acknowledged items in this test. Keying the clock by client id instead also failed: a client whose read was stale would bump its counter past a write it had never seen. Dots fix this. A write supersedes another only if its context contains that write's dot. The coordinator also replicates its whole sibling set rather than only the new version (as Riak does), so a context assembled from one replica can't claim a write that replica lacks. `TestNoLostUpdatesUnderConcurrency` and `TestSameCoordinatorConcurrentWrites` guard this.
* LWW never shows conflicts because it throws them away: 85% of acknowledged items were lost at R=W=1.

## 5. Availability under a partition

Nodes are split {n1,n2} (A, minority) | {n3,n4,n5} (B, majority), with 6 clients per side and 64 keys. Clients on each side read, and half the time write back what they read. Availability is the fraction of attempted operations that succeeded.

| Quorum | R,W | A get / put | B get / put | stale reads A / B | acked writes missing after heal |
|---|---|---|---|---|---|
| strict | 1,1 | 89% / 89% | 100% / 100% | 97% / 86% | 0 |
| strict | 2,2 | 28% / 28% | 72% / 74% | **0% / 0%** | 0 |
| strict | 3,3 | 0% / 0% | 11% / 13% | 0% / 0% | 0 |
| strict | 1,3 | 88% / 0% | 100% / 13% | 0% / 0% | 0 |
| sloppy + hinted handoff | 1,1 | 100% / 100% | 100% / 100% | 97% / 96% | 0 |
| sloppy + hinted handoff | 2,2 | 100% / 100% | 100% / 100% | 92% / 88% | 0 |
| sloppy + hinted handoff | 3,3 | 0% / 0% | 100% / 100% | 0% | 0 |

* **Strict quorums trade availability for consistency, key by key.** A key stays readable or writable on a side only if that side holds R or W of its 3 replicas. With R=W=2, A can serve the 28% of keys with 2 replicas on its side and B the other 72%. No reads are stale, and both sides can never write the same key.
* **Sloppy quorums restore availability** wherever a side has at least max(R,W) live nodes (W=3 is still impossible on the 2-node side). The cost is that R+W>N no longer prevents stale reads: R=W=2 is 88–92% stale during the partition, because each side writes to different nodes.
* "Stale" here mostly means a client didn't see writes from the other side of the partition, which is unavoidable in any mode where both sides accept writes.
* **After reconnecting**, in every configuration, hinted handoff plus one anti-entropy round left all replicas of all 64 keys identical, and **no acknowledged write was missing**. Concurrent writes from the two sides survive as siblings (51–64 of the 64 keys), ready for the client's union merge.

## 6. Takeaways

1. R+W>N with strict quorums gave 0 stale reads in every run, at the cost of tail latency and of minority-side availability during partitions.
2. R=W=1 is the fastest and most available setting, but 25% of reads were stale even with no failures.
3. Sloppy quorums make the store always writable, but they also drop the R+W>N guarantee. Conflicts are then normal and the resolution rule does the work.
4. Version vectors only help if dominance is computed correctly. Plain per-node clocks silently lose concurrent writes, and dotted version vectors fixed that in these tests.
5. An explicit, data-preserving merge (union) kept every acknowledged write. LWW is simpler but lost acknowledged writes in every concurrent test.

## Limitations

* This is a single-process simulation. "Latency" is simulated network delay plus Go scheduling. There is no disk and no node crash or restart; partitions are the only failure tested.
* The failure detector is an oracle: it sees partitions instantly and exactly. A real gossip detector would add false suspicions and delay.
* Anti-entropy exchanges full state instead of Merkle trees, which is fine at this scale.
* Union as a merge can't express deletes. That needs tombstones or a CRDT such as an OR-set, as in Dynamo's shopping-cart caveat.
