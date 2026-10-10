# ARCHITECTURE

This document describes how `dynamo-store` is built, why each design decision was made, and which test or experiment supports it. All numbers come from three commands:

* `go test -race ./...` (unit and scenario tests)
* `go run ./cmd/bench`, which writes [results/benchmark.md](results/benchmark.md) (experiments E1–E3)
* `go run ./cmd/archbench`, which writes [results/architecture.md](results/architecture.md) (experiments A1–A5, added to back the decisions below)

The system runs as a simulation inside one process, so absolute latencies depend on Go's timers and the machine. Compare the numbers to each other, not to a real deployment.

---

## Contents

1. [Goals](#1-goals)
2. [System overview](#2-system-overview)
3. [Components](#3-components)
4. [Data model: versions and causality](#4-data-model-versions-and-causality)
5. [Request flows](#5-request-flows)
6. [Consistency guarantees](#6-consistency-guarantees)
7. [Architecture decisions](#7-architecture-decisions)
8. [How the tests map to the design](#8-how-the-tests-map-to-the-design)
9. [Limitations and what production would change](#9-limitations-and-what-production-would-change)

---

## 1. Goals

The problem statement asks for:

| # | Requirement | Where it is met |
|---|---|---|
| G1 | Replicated KV store, 3 replicas per key | Consistent-hash ring with N=3 ([D3](#d3-consistent-hashing-with-md5-and-64-virtual-nodes), [D4](#d4-five-nodes-three-replicas-per-key)) |
| G2 | R and W exposed as client settings | Per-client, per-request quorums ([D6](#d6-r-and-w-travel-with-each-request)) |
| G3 | Track concurrent versions with vector clocks | Dotted version vectors ([D9](#d9-dotted-version-vectors-instead-of-plain-vector-clocks)) |
| G4 | Return conflicting versions for explicit reconciliation | Sibling sets plus a merged context returned by `Get` ([D11](#d11-siblings-with-explicit-client-reconciliation-lww-only-for-comparison)) |
| G5 | Explicit conflict-resolution policy | Client-side set union, with LWW as the comparison ([D11](#d11-siblings-with-explicit-client-reconciliation-lww-only-for-comparison)) |
| G6 | Partition, update both sides, reconnect, verify the rule | Partitionable network ([D2](#d2-an-in-memory-network-that-drops-messages)), repair ([D13](#d13-three-repair-mechanisms-hinted-handoff-read-repair-anti-entropy)), `RunPartitionScenario` |
| G7 | Compare latency, availability and returned versions; report stale reads per quorum | `cmd/bench` (E1–E3), with a stale-read oracle |

---

## 2. System overview

```
                         ┌──────────────────────────────────────────────┐
  Client (R, W, Sloppy)  │            Simulated network                 │
  ┌──────────────┐       │  per-message latency · partitions · slow    │
  │ Get / Put    │──────▶│  nodes · dropped messages → timeouts         │
  │ GetResolved  │       └──────┬──────────┬──────────┬────────┬───────┘
  └──────────────┘              │          │          │        │
         ▲                ┌─────▼───┐ ┌────▼────┐ ┌───▼────┐  ...  (n1..n5)
         │ siblings +     │ Node n1 │ │ Node n2 │ │ Node n3│
         │ context        │─────────│ │─────────│ │────────│
         │                │ data    │ │ data    │ │ data   │  key → []Version (siblings)
         │                │ hints   │ │ hints   │ │ hints  │  owner → key → []Version
         │                │ coord.  │ │ coord.  │ │ coord. │  any node can coordinate
         │                └─────────┘ └─────────┘ └────────┘
         │                     ▲  hinted handoff · read repair · anti-entropy
         │                     └──────────────── background ────────────────
         │
  Consistent-hash ring (MD5, 64 vnodes/node): key → walk clockwise →
  first 3 distinct nodes = preference list; nodes after that = sloppy fallbacks
```

Each node is both a **replica**, which stores sibling sets, and a **coordinator**, which runs quorum reads and writes for clients. A cluster has no master: every node runs the same code.

**Package layout**

| Package / file | Responsibility | Depends on |
|---|---|---|
| `vclock/` | Vector clocks: `Compare`, `Merge`, `Increment`, `Descends` | none |
| `store/version.go` | `Version`, `Dot`, dominance rules, sibling merge, LWW, clock modes | `vclock` |
| `store/network.go` | Message delivery, latency model, partitions, slow nodes, failure-detector view | none |
| `store/ring.go` | Consistent hashing, preference lists, fallback order | none |
| `store/node.go` | Replica storage, coordinator logic, hinted handoff, read repair, anti-entropy | all of the above |
| `store/cluster.go` | Cluster wiring, `Client` API, coordinator selection | node, ring, network |
| `store/scenario.go` | The partition, update, heal and verify scenario, and the `UnionResolver` rule | cluster |
| `cmd/demo` | Prints the partition scenario under 5 configurations | store |
| `cmd/bench` | E1–E3: quorum sweep, concurrent updates, partition availability | store |
| `cmd/archbench` | A1–A5: experiments behind the architecture decisions | store |

---

## 3. Components

### 3.1 Network (`store/network.go`)

* `Send(ctx, from, to, req)` starts a goroutine that sleeps for a sampled one-way delay, delivers the request to the handler, sleeps again, and delivers the reply. The caller `select`s on the reply or `ctx.Done()`.
* **Latency model.** Replica-to-replica hops take `0.4ms + Exp(mean 0.8ms)`, and 3% of messages get an extra +5ms (a heavy tail). Client-to-node hops take `0.1ms + Exp(0.1ms)` (the client sits in the same zone as the node). `SetSlow(id, d)` adds delay to every message to or from one node.
* **Partitions.** Every endpoint is in a group. A message whose sender and receiver are in different groups at delivery time is **dropped silently**, and so is a reply in the same situation.
* **Failure detector.** `Reachable(a, b)` gives each node an exact view of who is reachable, standing in for Dynamo's gossip-based detector.

### 3.2 Ring (`store/ring.go`)

* Each node places 64 tokens (`MD5("n3#17")`, ...) on a 64-bit ring.
* `Walk(key)` starts at `MD5(key)`, moves clockwise, and returns every physical node once. The first N=3 are the **preference list**; the rest are **fallbacks** in the order sloppy quorums use them.

### 3.3 Node (`store/node.go`)

State, protected by one mutex per node:

```go
data   map[key][]Version                  // siblings this node owns
hints  map[owner]map[key][]Version        // writes held for unreachable owners
issued map[key]uint64                     // last dot counter this node issued per key
```

Message handlers:

| Message | Sent by | Effect |
|---|---|---|
| `coordPutReq{Key, Value, Context, W, Sloppy}` | client | coordinate a write ([5.1](#51-write-put)) |
| `coordGetReq{Key, R, Sloppy}` | client | coordinate a read ([5.2](#52-read-get)) |
| `putReq{Key, V []Version, Hint}` | coordinator, read repair, handoff | merge versions into `data`, or into `hints[Hint]` |
| `getReq{Key}` | coordinator | return owned plus hinted versions |
| `syncReq{Items}` | anti-entropy | merge the peer's items and reply with ours for keys we share |

Background loop: every `HandoffInterval` (5ms), the node tries to deliver held hints to owners that have become reachable again.

### 3.4 Client (`store/cluster.go`)

```go
cl := cluster.NewClient("alice", R, W) // quorums are a client setting
cl.Sloppy = true                       // optional sloppy quorum
res, _ := cl.Get(k)                    // res.Versions = siblings, res.Context = merged clock
cl.Put(k, v, res.Context)              // supersedes everything in the context
cl.GetResolved(k, store.UnionResolver) // read, merge siblings, write back
```

The client picks a coordinator among the reachable preferred replicas ([D5](#d5-any-preferred-replica-coordinates-picked-at-random)). The client timeout is 3× the replica timeout, so a coordinator always has time to report a quorum failure explicitly.

---

## 4. Data model: versions and causality

```go
type Dot struct { Node string; Counter uint64 }   // names one write
type Version struct {
    Value  string
    Dot    Dot        // (coordinator, fresh counter)
    Ctx    vclock.VC  // what the writer had read: the Context from its Get
    TS     int64      // wall clock, used only by LWW
    Origin string     // LWW tie-breaker
}
Clock() = Ctx ∪ {Dot.Node: Dot.Counter}   // the plain vector clock of the version
```

**Rule for superseding a version** (dotted version vectors): version *a* makes *b* obsolete iff **b's dot is inside a's context**:

```
obsoletes(a, b) = a.Dot ≠ b.Dot  ∧  a.Ctx[b.Dot.Node] ≥ b.Dot.Counter
```

If neither version makes the other obsolete, they are **concurrent** and both are kept as siblings. A replica's sibling set never contains two versions where one supersedes the other.

**Context.** `Get` returns `Context = merge(Clock() of every sibling)`. A `Put` under that context supersedes exactly the siblings the client saw, and nothing it did not see.

Example from the partition demo:

```
{milk}            dot n2:1  ctx []                     ← initial write
{eggs,milk}       dot n2:2  ctx [n2:1]                 ← side A, during partition
{bread,milk}      dot n5:1  ctx [n2:1]                 ← side B, during partition
  → neither ctx contains the other's dot → 2 siblings returned after heal
{bread,eggs,milk} dot n2:3  ctx [n2:2 n5:1]            ← client's union, written back
  → its ctx contains both dots → both siblings dropped on every replica
```

---

## 5. Request flows

### 5.1 Write (`Put`)

```
Client          Coordinator (random reachable preferred replica)         Replicas
  │ coordPutReq(ctx, W) │                                                   │
  │────────────────────▶│ lock                                              │
  │                     │  counter = max(issued, ctx[self], stored) + 1      │
  │                     │  v = {dot:(self,counter), ctx}                     │
  │                     │  merge v into local siblings  (self = ack #1)      │
  │                     │  snapshot = local sibling set                      │
  │                     │ unlock                                             │
  │                     │── putReq(snapshot) ───────────────────────────────▶│ merge
  │                     │◀──────────────────────────────────────────── ack ──│
  │◀── ok after W acks ─│   (remaining replicas keep receiving in the background)
  │   or ErrUnavailable after the replica timeout
```

* Choosing the counter, applying the write locally, and taking the snapshot happen under one lock. Two concurrent writes on the same coordinator therefore get distinct, consecutive dots ([D10](#d10-replicate-the-coordinators-whole-sibling-set)).
* With `Sloppy`, any preferred replica the coordinator cannot reach is replaced by the next reachable fallback, and the write is tagged `Hint: <intended owner>`.

### 5.2 Read (`Get`)

```
Client ── coordGetReq(R) ──▶ Coordinator ── getReq ──▶ all N targets
                                  │◀── versions ── (first R replies)
Client ◀── merged siblings ───────│
                                  │  background: wait for the rest, then
                                  │  read repair: send each replica the versions it lacks
```

### 5.3 Partition and reconnect

```
 during partition              after heal
 ────────────────              ──────────────────────────────────────────────
 strict: ops fail if fewer     1. hinted handoff: fallbacks push hints to owners
         than R/W replicas     2. read repair: each read fixes the replicas it touched
         are on the client's   3. anti-entropy: each reachable pair exchanges state
         side                     for the keys they share
 sloppy: fallbacks accept      → every replica holds the same sibling set
         writes as hints       → client reconciles the siblings (union) and writes back
```

---

## 6. Consistency guarantees

| Setting | Guarantee | Evidence |
|---|---|---|
| Strict, R+W > N, no failures | A read sees every write acknowledged before it started | E1: 0 stale reads in about 50k reads over all six R+W>3 settings |
| Strict, R+W > N, partitioned | Same as above for operations that succeed; operations on the side with too few replicas fail | E3: R=W=2 had 0% stale reads, with 28% (minority) and 72% (majority) availability |
| R+W ≤ N | Eventual consistency only | E1: R=W=1 had 25.4% stale reads; R=1,W=2 had 0.4% |
| Sloppy quorum | Always writable if a side has ≥ W live nodes; R+W>N **no longer** guarantees fresh reads | E3: sloppy R=W=2 was 100% available but 88–92% stale |
| Any setting, sibling policy | No acknowledged write is lost; concurrent writes come back as siblings | E2: 0 lost items; E3: 0 acknowledged writes missing after heal; `TestNoLostUpdatesUnderConcurrency` |
| LWW policy | Converges to one value; acknowledged concurrent writes are discarded | E2: 85% of acknowledged items lost at R=W=1; demo case 5: skew makes the earlier write win |
| After heal + repair | All replicas converge | E3: `replicas converged = true` in all 10 configurations; A3 |

A failed write (`ErrUnavailable`) means "outcome unknown", not "rolled back". The write may have reached some replicas, and it will spread from there ([D12](#d12-a-failed-quorum-write-is-not-rolled-back)).

---

## 7. Architecture decisions

Each decision lists what was chosen, the alternatives considered, why the choice makes sense, and the evidence. Decisions marked **(new evidence)** had no test behind them until the A-series experiments were added for this document.

### D1. Go, as one process with simulated nodes

* **Decision.** Write in Go. Run all nodes as goroutines in one process with an in-memory network.
* **Alternatives.** Separate OS processes with real RPC (gRPC/HTTP); Python; Rust.
* **Why.** The assignment is about consistency behaviour, not deployment. A single process makes partitions exact, repeatable and instant to apply, and it lets the oracle see every acknowledged write, which is what makes stale-read counting possible. Goroutines plus `context` timeouts plus `select` map directly onto "send to N, wait for the first W". Python's GIL would serialise the concurrency the experiments depend on.
* **Evidence.** The full benchmark runs in about 2 minutes, and `go test -race` passes, so the concurrent code has no data races.

### D2. An in-memory network that drops messages

* **Decision.** Messages across a partition are dropped silently. Senders learn about failures only through timeouts. Coordinators can query an exact failure-detector view (`Reachable`).
* **Alternatives.** Return an immediate "unreachable" error; simulate a gossip failure detector.
* **Why.** Dropped messages are what really happens on a network, and they make failed operations cost a full timeout, which A5 measures. The exact failure detector keeps sloppy-quorum target selection simple and deterministic. Its cost (no false suspicions) is listed in [§9](#9-limitations-and-what-production-would-change).
* **Evidence.** E3: strict operations on the wrong side of a partition fail with latency equal to the timeout. A5: failed-operation p50 tracks the timeout (3.9ms at 2ms, 22.4ms at 20ms, 102.7ms at 100ms).

### D3. Consistent hashing with MD5 and 64 virtual nodes **(new evidence)**

* **Decision.** Hash keys and tokens with MD5 (as Dynamo does) and give each node 64 virtual nodes.
* **Alternatives.** FNV-1a (faster, non-cryptographic); fewer or more vnodes; modulo hashing.
* **Why.** Placement only works if keys spread evenly. The first implementation used FNV-1a with 16 vnodes and left one node with **no keys at all**. FNV-1a mixes short, similar strings such as `n3#17` and `n3#18` poorly, so tokens cluster. MD5 spreads them uniformly. More vnodes reduce variance further, with diminishing returns.
* **Evidence.** A1 places 100,000 keys on 5 nodes, where perfect balance is a 60% share per node:

  | hash | vnodes | min–max share | max/min |
  |---|---|---|---|
  | FNV-1a | 64 | 27.9% – 100% | 3.58 |
  | FNV-1a | 256 | 32.5% – 89.6% | 2.76 |
  | MD5 | 8 | 50.0% – 69.1% | 1.38 |
  | **MD5** | **64** | **56.7% – 66.3%** | **1.17** |
  | MD5 | 256 | 57.8% – 62.4% | 1.08 |

  64 vnodes gets within about 10% of perfect balance. 256 is slightly better but makes each `Walk` longer. Modulo hashing was rejected without measurement, because adding a node remaps almost every key.

### D4. Five nodes, three replicas per key

* **Decision.** 5 nodes, N=3.
* **Alternatives.** Exactly 3 nodes, so every node holds every key.
* **Why.** With 3 nodes and N=3 there are no fallback nodes, so sloppy quorums and hinted handoff would be impossible, and every key would have the same replica set. With 5 nodes, keys get different preference lists, a partition splits different keys differently, and fallbacks exist.
* **Evidence.** E3: availability during a partition differs key by key. At R=W=2 the 2-node side can serve 28% of keys and the 3-node side 72%, which matches how replicas fall across the split. Sloppy R=W=2 reaches 100% on both sides only because fallbacks exist.

### D5. Any preferred replica coordinates, picked at random **(new evidence)**

* **Decision.** The client sends each request to a random reachable node in the key's preference list, and that node coordinates.
* **Alternatives.** Always the first preferred replica (a "sticky" owner); a separate coordinator tier; a client library that talks to replicas directly.
* **Why.** Spreading coordination across the N replicas balances load and removes one hot node per key. It also avoids failover logic, since a dead first replica just isn't picked. The cost is real: with R=1, a read may go to a replica the latest write hasn't reached. A sticky coordinator hides that, because the coordinator applies every write locally first. Sticky coordination is still available as `Client.FirstCoordinator`.
* **Evidence.** A4 (R=W=1, 32 keys, one writer per key):

  | coordinator | stale reads | busiest node's share of coordination |
  |---|---|---|
  | always first preferred replica | 0.1% | 31% (n4) vs 12% (n2) |
  | **random preferred replica** | 25.7% | 23% (fairly even, 17–23%) |

  **Reading this result.** A sticky coordinator gives near read-your-writes behaviour at R=1, but load becomes 2.6× uneven and the freshness disappears as soon as that node fails. The design keeps random choice as the default because it doesn't make freshness depend on routing luck: users who need fresh reads set R+W>N (0% stale in E1), and users who want cheap sticky reads can set `FirstCoordinator`. **This is the trade-off with the biggest effect in the system, and the E1 stale-read numbers for R+W≤N should be read with it in mind.**

### D6. R and W travel with each request

* **Decision.** R and W are fields on the `Client` and are sent in every request. Nodes have no global quorum setting.
* **Alternatives.** Cluster-wide R/W, as in classic Dynamo configuration.
* **Why.** The problem statement asks for R and W as client settings. Per-request quorums also let different workloads share data with different trade-offs, for example a shopping cart at W=1 next to a reader at R=3.
* **Evidence.** E1 runs nine R/W pairs against identical clusters. `TestReadYourWriteWithStrictQuorum` (R=W=2) checks read-your-write over 50 iterations.

### D7. Return after W acks; replicate the rest asynchronously

* **Decision.** The coordinator sends to all N replicas but replies as soon as W have acknowledged. Late replicas still receive the write.
* **Alternatives.** Wait for all N, which is synchronous replication.
* **Why.** Waiting for all N ties every write to the slowest replica. Returning at W makes latency a tunable choice, and the background sends keep replicas close together even at low W.
* **Evidence.** E1: put p50 is 0.56ms at W=1, 4.3ms at W=2 and 5.7ms at W=3. With one slow replica (E1b), put p99 is 9.2ms at W=1 vs 21ms at W=3. Background replication is why R=1,W=2 is only 0.4% stale while R=W=1 is 25%.

### D8. Strict quorums by default, sloppy quorums as an option

* **Decision.** Strict quorums (preferred replicas only) by default. `Client.Sloppy` enables fallbacks and hinted handoff.
* **Alternatives.** Sloppy always, as Amazon's Dynamo does.
* **Why.** Strict quorums are the only mode where R+W>N actually means fresh reads, and freshness under quorum settings is what the assignment asks to measure. Sloppy mode is the availability end of the comparison.
* **Evidence.** E3 at R=W=2: strict quorums gave 0% stale reads but 28% and 72% availability on the two sides. Sloppy quorums gave 100% availability and 88–92% stale reads. Both modes lost **0** acknowledged writes after heal.

### D9. Dotted version vectors instead of plain vector clocks

* **Decision.** Each version carries a dot `(coordinator, counter)` plus the writer's context. Dominance is "the dot is in the other version's context".
* **Alternatives tried.**
  1. **Plain per-node vector clocks** (the Dynamo paper): the coordinator increments its own entry. Two concurrent writes through one coordinator from the same context get clocks `[n1:2]` and `[n1:3]`, so the second appears to descend from the first and **silently replaces it**.
  2. **Vector clocks keyed by client id**: a client whose read was stale bumps its counter past one of its own writes it has not seen, which again creates false dominance (3 items lost in one test run).
* **Why.** Dots separate *naming* a write from *what the writer had seen*. That gives an exact concurrency test while keeping one clock entry per server node, so clocks stay bounded by cluster size rather than by client count.
* **Evidence.** E2 / A2 (8 concurrent read-modify-write clients on one key):

  | clocks | R,W | acknowledged items | lost |
  |---|---|---|---|
  | **dotted version vector** | 1,1 | 8,627 | **0** |
  | **dotted version vector** | 2,2 | 5,369 | **0** |
  | per-node vector clock | 1,1 | 11,132 | 2,180 (19.6%) |
  | per-node vector clock | 2,2 | 5,463 | 1,366 (25.0%) |

  `TestSameCoordinatorConcurrentWrites` checks the exact failing case for both schemes. `TestNoLostUpdatesUnderConcurrency` checks the end-to-end property.

### D10. Replicate the coordinator's whole sibling set **(new evidence)**

* **Decision.** After applying a write locally, the coordinator sends its **entire** sibling set for the key to the other replicas, not only the new version (Riak does the same).
* **Alternative.** Send only the new version, which uses smaller messages.
* **Why.** A context built from one replica must not claim a write that replica lacks. If node n issues dots n:5 and then n:6, and replica r receives n:6 before n:5, a client reading r gets context `n:6`. That context "covers" n:5 although nobody saw it, so n:5's data is dropped. Sending the whole set means any replica holding n:6 also holds n:5 or something that supersedes it.
* **Evidence.** A2, summed over 3 runs of 2s each:

  | payload | R,W | acknowledged items | lost |
  |---|---|---|---|
  | **whole sibling set** | 1,1 / 2,2 | 8,627 / 5,369 | **0 / 0** |
  | new version only | 1,1 / 2,2 | 8,847 / 5,330 | 1 / 3 |

  The failure is rare (about 0.01–0.06%) because it needs messages to arrive out of order, but it is a correctness bug, not a performance cost. Messages grow with the number of siblings, which reconciliation keeps small.

### D11. Siblings with explicit client reconciliation; LWW only for comparison

* **Decision.** Replicas keep every concurrent version. The client merges them with a declared rule, **set union** (`UnionResolver`), and writes the result back under the merged context.
* **Alternatives.** Last-writer-wins on the server; server-side CRDT merge.
* **Why.** The assignment asks for conflicting versions to be returned for explicit reconciliation. Union is the shopping-cart rule from the Dynamo paper: it never loses an added item, and the outcome doesn't depend on clocks. LWW is kept to show what the simpler choice costs.
* **Evidence.**
  * Partition scenario (`TestPartitionScenarios`, `cmd/demo`): with siblings, both partition-side writes come back as 2 siblings, the union {bread, eggs, milk} is written, and all 3 replicas converge to it. With LWW, one acknowledged item is lost, and with +50ms clock skew the **earlier** write wins.
  * E2: siblings lost 0 acknowledged items. LWW lost 4,621 of 5,453 (85%) at R=W=1.
  * Cost: under contention, reads return about 4–6 siblings on average (E2), and the client must handle them.

### D12. A failed quorum write is not rolled back

* **Decision.** If fewer than W replicas acknowledge, the client gets `ErrUnavailable`, but replicas that did apply the write keep it.
* **Alternative.** Two-phase commit or a rollback, which would need coordination this AP design avoids.
* **Why.** Rolling back would need its own quorum and could fail too. Treating failure as "unknown outcome" matches Dynamo, and the union rule makes the resurfaced write harmless.
* **Evidence.** Demo case 2 (strict W=2): side A's put fails with 1/2 acks, but `{eggs,milk}` reappears as a sibling after heal and is kept by the union. The scenario logs this explicitly.

### D13. Three repair mechanisms: hinted handoff, read repair, anti-entropy **(new evidence)**

* **Decision.** All three run. Hinted handoff runs every 5ms. Read repair runs after every read. Anti-entropy is one full-state exchange per reachable replica pair, triggered after heal.
* **Alternatives.** Any one of them alone.
* **Why.** Each covers a different gap. Hinted handoff moves writes from **fallback nodes** back to their owners, which neither of the others can do, since fallbacks aren't in any key's preference list. Read repair fixes only keys that get read. Anti-entropy fixes everything between preferred replicas, but costs a full exchange.
* **Evidence.** A3: sloppy writes on both sides of a partition for 1s, then heal and apply only some mechanisms:

  | repair after heal | acked writes not on all 3 replicas | divergent keys / 64 |
  |---|---|---|
  | none | 93.7% | 57 |
  | hinted handoff only | 46.9% | 64 |
  | read repair only | 5.1% | 0 |
  | anti-entropy only | 5.2% | 0 |
  | **handoff + anti-entropy** | **0.0%** | **0** |

  The roughly 5% left over by read repair alone or anti-entropy alone is exactly the writes stuck on fallback nodes. Only hinted handoff delivers them. Read repair barely affects stale reads under continuous writes (E1: 25.4% with it vs 26.3% without), so its role is convergence, not freshness.

### D14. Replica timeout of 20ms **(new evidence)**

* **Decision.** The coordinator waits up to 20ms for replicas, and the client waits 60ms (3×).
* **Alternatives.** Shorter timeouts (fail fast) or longer ones.
* **Why.** The timeout must sit above the normal latency tail, or healthy operations fail spuriously. It should also be as short as possible, because every operation that really fails during a partition costs the full timeout.
* **Evidence.** A5 (strict R=W=2):

  | timeout | spurious put failures, healthy cluster | failed-op p50 during partition | put availability during partition |
  |---|---|---|---|
  | 2ms | 84.6% | 3.9ms | 10.0% |
  | 5ms | 14.0% | 6.8ms | 45.3% |
  | **20ms** | **0%** | 22.4ms | 57.2% |
  | 100ms | 0% | 102.7ms | 59.9% |

  20ms is the shortest tested timeout with no spurious failures, since it sits above the 5ms slow-path tail plus scheduling noise. Going to 100ms gains under 3 points of availability and makes each failure 5× slower.

### D15. Clients sit close to coordinators in the latency model

* **Decision.** A client hop costs about 0.2ms, while a replica-to-replica hop costs about 1.2ms on average with a 5ms tail.
* **Alternative.** One latency for every hop.
* **Why.** In real deployments clients talk to a nearby node, while replicas are spread across zones. With uniform latency, a write's ack and the next read raced the replication messages so closely that stale reads all but disappeared (under 1% at R=W=1 in an earlier run), which understates what R+W≤N costs in practice.
* **Evidence.** After this change, E1 shows the expected pattern: 25.4% stale reads at R=W=1, under 1% at R=1,W=2 and R=2,W=1, and 0% whenever R+W>N.

---

## 8. How the tests map to the design

| Test | What it proves | Decision |
|---|---|---|
| `vclock.TestCompare`, `TestIncrementFloor` | Clock ordering: Equal, Before, After, Concurrent | D9 |
| `TestMergeVersionsDropsDominated` | Sibling sets drop obsolete versions and ignore stale replays; LWW keeps one version | D9, D11 |
| `TestSameCoordinatorConcurrentWrites` | Dots keep same-coordinator concurrent writes apart; plain VCs don't | D9 |
| `TestReadYourWriteWithStrictQuorum` | R+W>N gives read-your-write | D6, D7 |
| `TestStrictQuorumUnavailableOnMinoritySide` | Strict W=2 fails with one reachable replica; sloppy W=2 succeeds through a fallback | D8 |
| `TestPartitionScenarios` (5 variants) | Partition, update both sides, heal: siblings returned, rule applied, replicas converge, acknowledged items kept (siblings) or lost (LWW) | D11, D12, D13 |
| `TestNoLostUpdatesUnderConcurrency` | 8 concurrent writers at R=W=1 lose 0 acknowledged items | D9, D10 |
| `cmd/bench` E1–E3 | Stale reads, latency and availability per quorum | D5–D8 |
| `cmd/archbench` A1–A5 | Ring balance, replication payload, repair, coordinator choice, timeout | D3, D5, D10, D13, D14 |

---

## 9. Limitations and what production would change

| Simplification here | Production approach |
|---|---|
| All nodes in one process; no disk | Separate processes with RPC and a durable storage engine (LSM or B-tree) with a write-ahead log |
| Exact, instant failure detector | Gossip or phi-accrual detection, which has false suspicions and detection delay |
| Anti-entropy exchanges full state and is triggered manually | Merkle trees per key range, running continuously in the background |
| Static membership (5 nodes) | Gossip-based membership with token rebalancing on join or leave |
| Union merge can't express deletes | Tombstones, or a CRDT such as an observed-remove set |
| No node crashes or restarts | A restarted node must persist `issued` counters so it never reuses a dot |
| Sibling sets can grow under sustained contention | Cap sibling count or apply server-side CRDT merges |
