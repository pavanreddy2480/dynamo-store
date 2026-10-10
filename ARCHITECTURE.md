# ARCHITECTURE

This document describes how `dynamo-store` is built, why each design decision was made, and which test or experiment supports it. All numbers come from four commands:

* `go test -race ./...` (unit and scenario tests)
* `go run ./cmd/bench`, which writes [results/benchmark.md](results/benchmark.md) (experiments E1–E3)
* `go run ./cmd/archbench`, which writes [results/architecture.md](results/architecture.md) (experiments A1–A5, added to back the decisions below)
* `go run ./cmd/realbench`, which writes [results/realnet.md](results/realnet.md) (experiments R1–R4 on real processes in Docker, with iptables partitions and `kill -9` crashes)

The same node code runs in two deployments ([D16](#d16-one-node-implementation-two-transports)):

* **Simulation**: all nodes in one process with a simulated network. Latencies are simulated, so compare the numbers to each other.
* **Real processes**: one `dynode` process per Docker container, TCP between them, a write-ahead log on each container's volume. These numbers come from a 4-CPU Linux host shared by all containers.

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
| G7 | Compare latency, availability and returned versions; report stale reads per quorum | `cmd/bench` (E1–E3), with a stale-read oracle; `cmd/realbench` R1–R2 from recorded histories |
| G8 | *(extension)* Run on a real network with real partitions | Nodes as processes over TCP in Docker, iptables partitions ([D16](#d16-one-node-implementation-two-transports)–[D18](#d18-heartbeat-failure-detector-with-a-600ms-suspicion-timeout), [D22](#d22-docker-from-scratch-and-iptables-on-the-bridge)) |
| G9 | *(extension)* Survive crashes and restarts | Write-ahead log per node ([D19](#d19-a-write-ahead-log-of-merges)–[D21](#d21-log-the-local-write-in-parallel-with-replication)) |

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

**Real deployment** (`cmd/realbench`):

```
 Docker bridge "dynstore" 172.28.0.0/24          host iptables: DOCKER-USER → DYNSTORE chain
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │  n1 .11   n2 .12   n3 .13   n4 .14   n5 .15      dynode processes, TCP :7000    │
 │   │vol     │vol     │vol     │vol     │vol       WAL on a volume per node       │
 │                                                                               │
 │  loadA .101   loadB .102   verifier .110         dyload: run / verify / dump    │
 └───────────────────────────────────────────────────────────────────────────────┘
   partition = DROP rules between the two sides' IPs     crash = docker kill -s KILL
   histories (JSON lines per operation) → analysed on the host
```

**Package layout**

| Package / file | Responsibility | Depends on |
|---|---|---|
| `vclock/` | Vector clocks: `Compare`, `Merge`, `Increment`, `Descends` | none |
| `store/version.go` | `Version`, `Dot`, dominance rules, sibling merge, LWW, clock modes | `vclock` |
| `store/network.go` | Message delivery, latency model, partitions, slow nodes, failure-detector view | none |
| `store/ring.go` | Consistent hashing, preference lists, fallback order | none |
| `store/node.go` | Replica storage, coordinator logic, hinted handoff, read repair, anti-entropy | all of the above |
| `store/cluster.go` | Cluster wiring, `Client` API, coordinator selection, simulated crash and restart | node, ring, network |
| `store/wal.go` | Write-ahead log: CRC-framed records, fsync with group commit, replay, checkpoint | version |
| `store/tcp.go` | TCP transport: gob framing, connection pool, heartbeat failure detector, admin calls | node messages |
| `store/scenario.go` | The partition, update, heal and verify scenario, and the `UnionResolver` rule | cluster |
| `cmd/demo` | Prints the partition scenario under 5 configurations | store |
| `cmd/bench` | E1–E3: quorum sweep, concurrent updates, partition availability | store |
| `cmd/archbench` | A1–A5: experiments behind the architecture decisions | store |
| `cmd/dynode` | One storage node as an OS process | store |
| `cmd/dyload` | Load generator and admin client for real clusters (`run`, `verify`, `dump`) | store |
| `cmd/realbench` | R1–R4: builds the image, runs containers, applies iptables partitions and crashes, analyses histories | docker, iptables |
| `deploy/Dockerfile` | `FROM scratch` image with the two static binaries | none |

---

## 3. Components

### 3.1 Network (`store/network.go`)

* `Send(ctx, from, to, req)` starts a goroutine that sleeps for a sampled one-way delay, delivers the request to the handler, sleeps again, and delivers the reply. The caller `select`s on the reply or `ctx.Done()`.
* **Latency model.** Replica-to-replica hops take `0.4ms + Exp(mean 0.8ms)`, and 3% of messages get an extra +5ms (a heavy tail). Client-to-node hops take `0.1ms + Exp(0.1ms)` (the client sits in the same zone as the node). `SetSlow(id, d)` adds delay to every message to or from one node.
* **Partitions.** Every endpoint is in a group. A message whose sender and receiver are in different groups at delivery time is **dropped silently**, and so is a reply in the same situation.
* **Failure detector.** `Reachable(a, b)` gives each node an exact view of who is reachable, standing in for Dynamo's gossip-based detector.
* **Crashes.** `SetDown(id)` drops everything to and from a node. `Cluster.Crash` also discards the node's memory and any WAL records it hadn't written yet, and `Cluster.Restart` boots it again from its log.

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

### 3.4 TCP transport (`store/tcp.go`)

* `Transport` is an interface with two methods, `Send` and `Reachable`. The simulated network and `TCPTransport` both implement it, and the node and client code never know which one they have.
* **Wire format.** Each request is an `envelope{From, Req}` encoded with `encoding/gob` on a persistent TCP connection. The reply is `{Resp, OK}`. Connections are pooled per peer (up to 32 idle). A connection that times out is closed rather than reused, since a late reply could still arrive on it.
* **Deadlines** come from the caller's `context`, so the replica timeout means the same thing as in the simulation.
* **Failure detector.** Every process probes every node every 100ms. A node is suspected after 600ms without a successful reply. Any successful request also counts as a heartbeat.
* **Admin calls.** `syncAllReq` makes a node run anti-entropy with every reachable peer now. `dumpReq` returns its full state and the number of WAL records it replayed at startup. The verifier uses both.
* **Emulated link delay.** `-link-delay` / `-link-jitter` add a sampled delay to node-to-node messages, because the test host has no `tc netem`.

### 3.5 Write-ahead log (`store/wal.go`)

* **Record** = one mutation: `data(key, versions)`, `hint(owner, key, versions)`, `hint delivered(owner, key, versions)`, or `issued(key, counter)`.
* **Frame** = `[length uint32][crc32 uint32][gob record]`. On replay, the first frame that is short or fails its CRC ends the log. Everything before it is kept, so a write torn by a crash costs only itself.
* **Modes.** `fsync`: the record is flushed and fsynced before the replica acks, and concurrent appends share one fsync (group commit: the first waiter syncs everything written so far, the rest wait on a condition variable). `async`: records go into an 8 MB in-process buffer that a background loop flushes every `-wal-flush`. `none`: no log.
* **Recovery.** On start the node replays the log into memory, then atomically rewrites it as a compact checkpoint (write a temp file, fsync, rename, fsync the directory).

### 3.6 Client (`store/cluster.go`)

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

On the coordinator, the local write is logged to the WAL in parallel with sending it to the other replicas. It counts as one of the W acks only once it is durable under the WAL mode ([D21](#d21-log-the-local-write-in-parallel-with-replication)). Replicas likewise ack a `putReq` only after logging it.


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
| R+W ≤ N | Eventual consistency only | E1: R=W=1 had 21.5% stale reads; R=1,W=2 had 0.4% |
| Sloppy quorum | Always writable if a side has ≥ W live nodes; R+W>N **no longer** guarantees fresh reads | E3: sloppy R=W=2 was over 99% available but 87–92% stale |
| Any setting, sibling policy | No acknowledged write is lost; concurrent writes come back as siblings | E2: 0 lost items; E3: 0 acknowledged writes missing after heal; `TestNoLostUpdatesUnderConcurrency` |
| LWW policy | Converges to one value; acknowledged concurrent writes are discarded | E2: 84% of acknowledged items lost at R=W=1; demo case 5: skew makes the earlier write win |
| After heal + repair | All replicas converge | E3: `replicas converged = true` in all 10 configurations; A3 |
| Real processes, real partition | Same guarantees as the simulation, with detection by heartbeat instead of an oracle | R1: 0% stale for every R+W>3 setting; R2: strict R=W=2 0% stale at 27% / 72% availability, 0 acknowledged writes lost, all replicas converged |
| One node crashes, WAL `fsync` | The node restarts with every write it acknowledged | R3: 0 local writes lost on the killed node; restart in about 0.6s |
| One node crashes, any WAL mode | No acknowledged write is lost cluster-wide as long as the other replicas survive (they hold it, and repair copies it back) | R3: 0 cluster-wide losses in every mode and at W=1 and W=2, including `none` |
| Whole cluster crashes | Only `fsync` guarantees acknowledged writes survive; `async` loses up to one flush interval | R3b: fsync 0 of 1651 lost; async 10ms 2 of 3069; async 1s 21%; none 100% |

A failed write (`ErrUnavailable`) means "outcome unknown", not "rolled back". The write may have reached some replicas, and it will spread from there ([D12](#d12-a-failed-quorum-write-is-not-rolled-back)).

---

## 7. Architecture decisions

Each decision lists what was chosen, the alternatives considered, why the choice makes sense, and the evidence. Decisions marked had no test behind them until the A-series experiments were added for this document.

### D1. Go, with a single-process simulation as the main test bed

* **Decision.** Write in Go. Do most experiments with all nodes as goroutines in one process on an in-memory network, and confirm the key results on real processes ([D16](#d16-one-node-implementation-two-transports)).
* **Alternatives.** Only real processes; Python; Rust.
* **Why.** The assignment is about consistency behaviour. A single process makes partitions exact, repeatable and instant to apply, and it lets an oracle see every acknowledged write, which makes stale-read counting easy. Goroutines plus `context` timeouts plus `select` map directly onto "send to N, wait for the first W". Python's GIL would serialise the concurrency the experiments depend on. Real processes then check that the simulation didn't flatter the design.
* **Evidence.** The full simulation benchmark runs in about 2 minutes and `go test -race` passes. The real-process partition results matched the simulation closely: strict R=W=2 gave 27% / 72% availability on the two sides, against 28% / 72% simulated (R2 vs E3).

### D2. An in-memory network that drops messages

* **Decision.** Messages across a partition are dropped silently. Senders learn about failures only through timeouts. Coordinators can query an exact failure-detector view (`Reachable`).
* **Alternatives.** Return an immediate "unreachable" error; simulate a gossip failure detector.
* **Why.** Dropped messages are what really happens on a network, and they make failed operations cost a full timeout, which A5 measures. The exact failure detector keeps sloppy-quorum target selection simple and deterministic. Its cost (no false suspicions) is listed in [§9](#9-limitations-and-what-production-would-change).
* **Evidence.** E3: strict operations on the wrong side of a partition fail with latency equal to the timeout. A5: failed-operation p50 tracks the timeout (3.9ms at 2ms, 22.4ms at 20ms, 102.7ms at 100ms).

### D3. Consistent hashing with MD5 and 64 virtual nodes

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

### D5. Any preferred replica coordinates, picked at random

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
* **Evidence.** E3 at R=W=2: strict quorums gave 0% stale reads but 28% and 72% availability on the two sides. Sloppy quorums gave over 99% availability and 87–92% stale reads. Both modes lost **0** acknowledged writes after heal.

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

### D10. Replicate the coordinator's whole sibling set

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
  * E2: siblings lost 0 acknowledged items. LWW lost 4,572 of 5,426 (84%) at R=W=1.
  * Cost: under contention, reads return about 4–6 siblings on average (E2), and the client must handle them.

### D12. A failed quorum write is not rolled back

* **Decision.** If fewer than W replicas acknowledge, the client gets `ErrUnavailable`, but replicas that did apply the write keep it.
* **Alternative.** Two-phase commit or a rollback, which would need coordination this AP design avoids.
* **Why.** Rolling back would need its own quorum and could fail too. Treating failure as "unknown outcome" matches Dynamo, and the union rule makes the resurfaced write harmless.
* **Evidence.** Demo case 2 (strict W=2): side A's put fails with 1/2 acks, but `{eggs,milk}` reappears as a sibling after heal and is kept by the union. The scenario logs this explicitly.

### D13. Three repair mechanisms: hinted handoff, read repair, anti-entropy

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

  The roughly 5% left over by read repair alone or anti-entropy alone is exactly the writes stuck on fallback nodes. Only hinted handoff delivers them. Read repair barely affects stale reads under continuous writes (E1: 21.5% with it vs 21.8% without), so its role is convergence, not freshness.

### D14. Replica timeout of 20ms

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
* **Evidence.** After this change, E1 shows the expected pattern: 21.5% stale reads at R=W=1, under 1% at R=1,W=2 and R=2,W=1, and 0% whenever R+W>N.

---

### D16. One node implementation, two transports

* **Decision.** Nodes and clients depend only on a two-method `Transport` interface (`Send`, `Reachable`). The simulated `Network` and the real `TCPTransport` both implement it.
* **Alternatives.** A separate server codebase for the real deployment; RPC stubs generated from a schema (gRPC).
* **Why.** Every consistency rule, quorum loop and repair path is then exercised by both the fast simulation and the real deployment, so a result from one says something about the other. gRPC would add code generation and a dependency for no gain at this size.
* **Evidence.** `TestTCPClusterRoundTrip` runs 5 nodes over real sockets in one test, and siblings, reconciliation and read-after-write behave as in the simulation. Real-process results R1–R2 reproduce the simulation's quorum rules: zero stale reads whenever R+W>N, and the same partition availability split.

### D17. gob over persistent, pooled TCP connections

* **Decision.** Requests are gob-encoded envelopes on long-lived TCP connections, pooled per peer, with `TCP_NODELAY`. A connection that hits a timeout is closed, not reused.
* **Alternatives.** A new connection per request; HTTP/JSON; gRPC.
* **Why.** A new connection per request would add a TCP handshake to every replica message. gob needs no schema files and encodes the existing Go structs directly. Closing a timed-out connection matters: a late reply could otherwise be read as the answer to the next request on that connection.
* **Evidence.** R1 on the Docker bridge: get p50 0.9ms at R=1 and 1.4ms at R=2, with about 2,800–3,400 operations per second from 8 clients. Without fsync the same setup reaches about 5,000 ops/s (R4), so the transport is not the bottleneck.

### D18. Heartbeat failure detector with a 600ms suspicion timeout

* **Decision.** Each process probes every node every 100ms and suspects a node after 600ms without a successful reply. Any successful request also counts as a heartbeat.
* **Alternatives.** Gossip with phi-accrual suspicion (Cassandra); no detector, so every request discovers failures by timing out.
* **Why.** Coordinators need a cheap "is it up?" answer to pick a coordinator and sloppy-quorum fallbacks. A fixed timeout is the simplest detector that works on a real network. 600ms is several probe intervals, so one lost probe doesn't cause a false suspicion.
* **Evidence.** R2: with sloppy quorums, availability during a real partition was 98–100%, against exactly 100% with the simulation's instant oracle. The gap is requests sent across the partition during the first ~600ms, before the detector noticed. Strict-quorum results were unaffected, since strict quorums don't use the detector to choose targets.

### D19. A write-ahead log of merges

* **Decision.** Every change to a node's state is logged as a record describing a *merge* (versions added to a key, a hint added or delivered, a dot counter issued). Records are CRC-framed. On start the log is replayed and then rewritten as a compact checkpoint.
* **Alternatives.** Periodic snapshots only; an embedded storage engine (BoltDB, Pebble).
* **Why.** Every change in this store is already a commutative, idempotent merge, so replaying records in any order rebuilds the same state, and replaying a record twice is harmless. CRC framing makes a torn final record detectable, so a crash mid-write costs only that record. The dot counters (`issued`) must be logged too: a node that forgot them after a restart could issue a dot it had already used, and a new write would then be mistaken for an old one ([D9](#d9-dotted-version-vectors-instead-of-plain-vector-clocks)). An embedded engine would hide exactly the durability behaviour this extension sets out to measure.
* **Evidence.** `TestWALRecoversStateAfterCrash` (fsync and async, state identical after replay), `TestWALIgnoresTornTail` (garbage after the last record is ignored and the node keeps accepting writes), `TestIssuedCountersSurviveRestart`. R3: with fsync, n3 lost **0** of the 246 and 202 writes it had acknowledged before `kill -9`.

### D20. fsync with group commit as the default WAL mode

* **Decision.** `dynode` defaults to `-wal fsync`: a replica acknowledges only after its record is fsynced. Concurrent appends share one fsync.
* **Alternatives.** Async flushing (Cassandra's default periodic commit log); memory only, relying on replication.
* **Why.** Replication alone covers one node crashing, but not every replica crashing at once (a power cut, a bad deploy, a crash-looping bug). Only fsync covers that. Group commit keeps the cost per write roughly flat as concurrency rises, because one fsync covers every record waiting at that moment.
* **Evidence.**
  * R3, one node killed: in **every** mode, including memory-only, no acknowledged write was lost cluster-wide, because the other replicas still had it. Locally, n3 lost 0% with fsync, 0.3% with async 10ms, 93% with async 1s and 100% with none.
  * R3b, all five killed at once: fsync lost **0** of 1,651 acknowledged writes, async 10ms lost 2 of 3,069, async 1s lost 21%, and memory-only lost **100%**.
  * R4, cost: put p50 is 3.09ms with fsync vs 1.10ms with none, and throughput drops from about 5,000 to 3,060 ops/s.

  Async at 10ms is a reasonable choice if correlated crashes are accepted as rare: almost free, and it lost almost nothing in these tests. It is a flag away (`-wal async`). Note that SIGKILL leaves the OS page cache intact; a real power cut would also lose what async had handed to the OS but not fsynced.

### D21. Log the local write in parallel with replication

* **Decision.** The coordinator logs its own copy of a write in a goroutine while it sends the write to the other replicas. The local copy counts as an ack only once it is durable.
* **Alternative.** Log locally first, then replicate.
* **Why.** The write's latency becomes max(local fsync, the replica round trips) instead of their sum. Durability is unchanged, because the local ack still waits for the log.
* **Evidence.** Not measured as a separate A/B experiment; this follows from the code path. For scale, R4 shows a W=2 put at 3.1ms p50 with fsync and 1.1ms without, so the log is the largest part of write latency and running it in sequence would add to it.

### D22. Docker FROM scratch, and iptables on the bridge

* **Decision.** Build static Go binaries and copy them into a `FROM scratch` image. Give each container a fixed IP on one user-defined bridge network. Create partitions with DROP rules in a `DYNSTORE` chain hooked into Docker's `DOCKER-USER` chain. Create crashes with `docker kill -s KILL` and restart with `docker start` on the same volume.
* **Alternatives.** `docker network disconnect`; `tc netem` inside containers; docker compose.
* **Why.** A scratch image needs no base-image download and starts instantly. Host-side iptables rules can cut any set of IPs off from any other, in both directions, while connections stay open. That is what a real partition looks like: packets vanish, and nothing reports an error. Disconnecting a container removes its interface, which is a different failure. Fixed IPs make the rules simple. Load generators run in their own containers, so their traffic crosses the same rules.
* **Evidence.** R2 ran on this setup. R3 used the same rules to keep a restarted node isolated while its recovered state was inspected. The host had no `tc`, so cross-zone latency is emulated in the transport ([3.4](#34-tcp-transport-storetcpgo)).
* **Limit.** It needs a Linux host with root. On Docker Desktop for macOS or Windows the iptables involved belong to the hidden Linux VM, so run `realbench` inside a Linux VM.

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
| `TestWALRecoversStateAfterCrash` | fsync and async logs rebuild a crashed node's exact state | D19 |
| `TestNoWALLosesLocalStateButReplicationRepairs` | A memory-only node restarts empty and anti-entropy restores it | D20 |
| `TestWALIgnoresTornTail` | A torn final record is ignored; the node recovers and keeps working | D19 |
| `TestIssuedCountersSurviveRestart` | Dot counters survive a restart, so dots are never reused | D19 |
| `TestTCPClusterRoundTrip` | 5 nodes over real sockets: writes, siblings, reconciliation | D16, D17 |
| `cmd/realbench` R1–R4 | Quorums, iptables partitions, kill -9 of one node and of all nodes, WAL cost, on real processes | D1, D16–D22 |

---

## 9. Limitations and what production would change

| Simplification here | Production approach |
|---|---|
| Real-process tests share one 4-CPU host; cross-zone latency is emulated | Separate machines across zones, with real network latency |
| The whole state lives in memory; the WAL only rebuilds it | A storage engine (LSM tree or B-tree) so data can exceed memory |
| The WAL is compacted only at startup | Periodic checkpoints and log truncation while running |
| Crashes tested with SIGKILL only (the OS page cache survives) | Power-loss tests, e.g. by cutting a VM's power |
| Fixed-timeout heartbeat detector (real) or an exact oracle (simulation) | Gossip with phi-accrual suspicion |
| Anti-entropy exchanges full state and is triggered manually | Merkle trees per key range, running continuously in the background |
| Static membership (5 nodes) | Gossip-based membership with token rebalancing on join or leave |
| Union merge can't express deletes | Tombstones, or a CRDT such as an observed-remove set |
| Sibling sets can grow under sustained contention | Cap sibling count or apply server-side CRDT merges |
