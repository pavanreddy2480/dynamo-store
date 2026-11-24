# Every design choice in dynamo-store, tested and explained from zero

This guide goes through every important choice in the project, one at a time, in an order that builds up. For each choice it gives a small everyday **story**, then the **real version** (what the code does, with file names), then **what we tested**, **what we measured**, and **what that means for the design**. It is the companion of [ARCHITECTURE.md](ARCHITECTURE.md) (explained from zero), [ARCHITECTURE_DECISIONS.md](../reference/ARCHITECTURE_DECISIONS.md) (decisions D1 to D15), [MESSAGES.md](MESSAGES.md) (every message), [REPORT.md](../reference/REPORT.md) (results). The raw tables are in [docs/experiments/ABLATIONS.md](../experiments/ABLATIONS.md) and [docs/reference/DECISIONS_AUDIT.md](../reference/DECISIONS_AUDIT.md), and every chart is an SVG in `results/charts/`.

It describes the branch `adaptive-defaults` (and `ablations`, which includes it). That is the design you chose: 256 fixed partitions, one Merkle tree per partition, eager trees, random coordinator, strict quorums by default.

> **How to read the numbers.** The whole system runs inside one Go program with a fake network (messages wait a random delay, and are dropped across a partition). So **compare numbers with each other, not with a real data centre.** Stale-read percentages in particular belong to our made-up delays; we always say which network and workload they came from.

> **Tip:** if a word is new, see the [glossary](#13-glossary) at the end.

---

## Contents

1. [What is an ablation, and how the experiments run](#1-what-is-an-ablation-and-how-the-experiments-run)
2. [The system in one minute](#2-the-system-in-one-minute)
3. [Part A: never lose a shopping-cart item](#3-part-a-never-lose-a-shopping-cart-item)
4. [Part B: how strict should reads and writes be?](#4-part-b-how-strict-should-reads-and-writes-be)
5. [Part C: where does each key live?](#5-part-c-where-does-each-key-live)
6. [Part D: fixing copies that drifted apart](#6-part-d-fixing-copies-that-drifted-apart)
7. [Merkle trees in detail, with the messages they send](#7-merkle-trees-in-detail-with-the-messages-they-send)
8. [Part E: when the network misbehaves](#8-part-e-when-the-network-misbehaves)
9. [A bug the experiments found](#9-a-bug-the-experiments-found)
10. [The final recipe and what changed from `main`](#10-the-final-recipe-and-what-changed-from-main)
11. [Honest limits](#11-honest-limits)
12. [Re-running everything](#12-re-running-everything)
13. [Glossary](#13-glossary)

---

## 1. What is an ablation, and how the experiments run

### The story

You are baking and not sure whether the pinch of salt matters. You bake two cakes that are identical except for the salt, and taste both. If you cannot tell the difference, you leave the salt out and save a step. If you can, you know exactly what the salt does. That is an **ablation**: change **one thing**, keep everything else the same, measure.

<div align="center">

```
 choice A  ---+
              +--> same workload, same machine, same seeds --> compare the numbers
 choice B  ---+
```

</div>

### The real version

We have three families of experiments, all runnable from the command line:

| Family | Command | Writes | What it covers |
|---|---|---|---|
| **E1 to E3** | `go run ./cmd/bench` | `results/benchmark.md` | the headline: R and W, concurrent writes, partitions |
| **A1 to A10** | `go run ./cmd/archbench` | `results/architecture.md` | one experiment per architecture decision (ring, clocks, repair, Merkle, timeout) |
| **X1 to X8** | `go run ./cmd/bench -ablate` | `results/ablations.md` | the remaining gaps: noise, network speed, workload, repair timing, failures |

Three habits keep the results honest:

* **Repeats.** Each ablation row runs 3 times with different latency seeds (`-reps`). Tables show `mean (min-max)`, so you can see how noisy a number is. X1 measured the noise itself: about **0.4 points** on stale reads, so differences larger than that are real.
* **Same setup, one change.** The plumbing in `cmd/bench/ablate.go` (`workload`, `run`, `measureAll`, `spread`) builds the same cluster and workload each time and varies one field of `store.Config`.
* **Charts.** `chart/chart.go` draws bar, error-bar and line SVGs; `cmd/plots` redraws the ones for the older experiments from the Markdown tables.

---

## 2. The system in one minute

### The story

You keep a shopping list. To not lose it, you keep **three copies** in notebooks at three friends' houses (out of five friends). To write, you phone friends and wait until **W** of them say "written". To read, you phone friends and wait for **R** answers, then take the newest.

<div align="center">

```
          +--------+   put "milk"    node A  [ list ]  ok
 client --|        |---------------> node B  [ list ]  ok     N = 3 copies
          | coord. |                 node C  [ list ]  (slow) W = 2: done after 2 acks
          +--------+ <-- get ------  R = 2: ask, wait for 2, take the newest
```

</div>

**The rule to remember:** if **R + W > N**, the group you read from and the group you wrote to must overlap, so a read always sees the latest write. If **R + W <= N**, they can miss each other and you can read an old value (a **stale read**).

### The real version

* 5 nodes, `N=3` (`store.DefaultConfig()` in `store/cluster.go`).
* The node that handles one request is the **coordinator**; it is picked at random among the key's 3 replicas.
* Every stored value is a **version** with a **dot** (a unique tag) and a **clock** (what that writer had seen). Concurrent versions are kept side by side as **siblings**.
* Copies can drift apart. Three repair tools put them back: **hinted handoff**, **read repair** and **anti-entropy**.

The rest of this guide asks, one at a time: was each of these the right choice?

---

## 3. Part A: never lose a shopping-cart item

### A1. Which kind of clock?

#### The story

You and your partner each add an item to the list at the same moment on separate phones. When the phones reconnect, the system must see "these two happened **at the same time**", not "one is older". A clock that gets this wrong throws one item away.

#### The real version

Each version carries a clock, a map from node name to a counter. The simple kind, a **per-node vector clock**, gives one number per node. If two writers go through the same coordinator, both writes get "the next number" and the clock cannot tell them apart. The fix, a **dotted version vector** (`store/version.go`), gives every write its own tag `(node, counter)` called a **dot**.

<div align="center">

```
plain clock:   two carts written at once through node A  -> "one is older"  -> one cart DELETED
dotted clock:  each write gets its own dot (A,7) and (A,8) -> "both are new" -> both KEPT (siblings)
```

</div>

#### What we tested (A2, E2)

Eight clients each repeatedly read the one shared cart, add a unique item, and write it back, for 2 seconds. Afterwards we count **acknowledged items that are missing** from the final merged cart.

| clocks | R,W | acknowledged items | lost |
|---|---|---|---|
| dotted version vector | 1,1 | 2,457 | **0** |
| dotted version vector | 2,2 | 1,855 | **0** |
| plain per-node clock | 1,1 | 3,278 | **665 (20%)** |
| plain per-node clock | 2,2 | 1,806 | **490 (27%)** |

#### What it means

Plain clocks lose a fifth to a quarter of acknowledged writes. Dotted version vectors lose none. They stay.

### A2. Send the whole pile of siblings, or only the new write?

#### The story

When a friend updates the list, do they tell the others only the new line, or read out the whole page including the lines they had heard earlier?

#### The real version

When a coordinator replicates, it can send the **new version only**, or its **whole sibling set** for that key. The code sends the whole set (`coordPut` in `store/coordinator.go`).

#### What we tested (A2)

Same read-modify-write test, summed over 3 runs:

| payload | R,W | acked items | lost |
|---|---|---|---|
| whole sibling set | 1,1 | 8,626 | 0 |
| whole sibling set | 2,2 | 5,703 | 0 |
| new version only | 1,1 | 8,802 | 2 |
| new version only | 2,2 | 5,717 | 3 |

#### What it means

The loss is tiny (2 to 3 items in thousands) but it is not zero, and the whole-set rule is also provably correct: a replica that missed an earlier write receives it inside the set. We keep it.

### A3. When two versions conflict: keep both, or let the last writer win?

#### The story

You wrote "eggs" and your partner wrote "bread" at the same time. A **last-writer-wins** rule keeps whichever was written a moment later and silently drops the other. A **sibling** rule keeps both and asks you to combine them.

#### The real version

`store/merge.go` implements both. `KeepSiblings` returns every concurrent version to the client, which merges them (for a cart, a set union). `LastWriterWins` keeps the latest timestamp. The resolver code is in `store/client.go` (`GetResolved`).

#### What we tested (E2)

The same 8-writer test with each policy (dotted clocks, R=W=1):

| policy | acked items | lost |
|---|---|---|
| siblings + client union | 2,457 | **0** |
| last-writer-wins | 5,196 | **4,397 (85%)** |

#### What it means

Last-writer-wins loses most concurrent writes. We keep siblings as the default, and last-writer-wins only to show the difference. (Union suits a cart; other data needs another merge rule. That is the client's choice, not the store's.)

---

## 4. Part B: how strict should reads and writes be?

### B1. The R and W dial

#### The story

Before you trust an answer you can phone one friend (fast, maybe out of date) or two (slower, surer) or all three (slowest, surest).

#### The real version

`R` and `W` are per client, and travel with each request (`store/client.go`). The coordinator asks all the replicas but answers as soon as R (or W) have replied.

#### What we tested (E1)

32 keys, one writer per key, 16 readers, 2 seconds, default network:

| R,W | R+W>N | get p50 / p99 ms | put p50 / p99 ms | stale reads |
|---|---|---|---|---|
| 1,1 | no | 0.66 / 5.96 | 0.55 / 5.29 | **25.5%** |
| 1,2 | no | 1.39 / 3.89 | 4.12 / 8.28 | 0.3% |
| 2,1 | no | 3.60 / 8.35 | 0.62 / 3.81 | 1.0% |
| 2,2 | yes | 4.40 / 8.60 | 4.41 / 8.40 | **0.0%** |
| 3,3 | yes | 5.85 / 12.59 | 5.82 / 12.78 | **0.0%** |

#### What it means

Every setting with R + W > N had **zero** stale reads, as the theory says. R=W=1 is fastest but stale about a quarter of the time. Dial it to the freshness you need.

### B2. Do these numbers depend on how fast the network is?

#### The story

If your friends live next door, a message is quick and writes spread to all three notebooks almost at once, so stale reads are rare. If they live in another country, there is a long gap before the other notebooks catch up.

#### The real version

`LatencyModel` in `network/network.go` draws each message delay as base + random jitter, plus a rare slow path. We ran E1 under three models (X2): fast LAN (0.1 ms hop), our default (0.4 ms hop), cross-region (5 ms hop).

#### What we measured

| network | R=W=1 stale reads |
|---|---|
| LAN | **8.3%** |
| default | **26.2%** |
| cross-region | **40.7%** |

R + W > N stayed at 0% stale in all three.

#### What it means

The stale-read percentage is **not a constant**; it is a property of the network. In the report we quote it as a **range, with the network named**, and lean on the part that does not change: R + W > N gives zero.

### B3. Does the workload matter? Does adding nodes?

#### The story

If everyone edits the same few lists, a read very often lands right after a write to that same list, so it sees something old. If everyone edits different lists, it rarely does.

#### What we tested (X5, X4)

* **Workload (X5):** R=W=1 stale reads are **9.8%** with 256 keys and **26.5%** with 32 keys (12.2% with slow writers). R=1,W=2 stays between 0.1% and 1.2%.
* **Node count (X4):** 5 nodes and 7 nodes (N=3 both) give the same 26.4%. More machines do not change freshness.
* **Number of copies N (X4):** R=W=1 stale reads are 15.2% at N=2, 26.4% at N=3, 23.4% at N=5. A majority quorum is 0% stale at every N.

#### What it means

Freshness depends on the **workload and N**, not on the number of machines. We keep 5 nodes, N=3 (the Dynamo paper's default), and always state the workload next to any stale-read number.

### B4. Ask all the replicas, or only R of them?

#### The story

You need 2 answers. You can phone all 3 friends and use the first 2 to answer, or phone only 2 and wait for both.

<div align="center">

```
ask all 3, use the first 2:      ask only 2:
  A ---- fast  <- used             A ---- fast  <- used
  B ---- fast  <- used             B ---- SLOW  <- must wait for it
  C ---- slow  (ignored)           C            (never asked)
```

</div>

#### The real version

`coordGet` normally sends the request to every replica. The option `ReadFromROnly` (`store/node.go`) shuffles the replicas and asks only R.

#### What we tested (X7)

| | messages per operation | read p99 | read p99 with one slow replica |
|---|---|---|---|
| ask all N | 3.3 | 5.1 ms | 9.8 ms |
| ask only R | 2.8 (15% fewer) | 10.7 ms | 20.0 ms |

#### What it means

Asking only R saves 15% of messages but **doubles the tail latency**, because one slow replica in the group stalls the read. We keep asking all N.

### B5. Which node coordinates a request?

#### The story

Do you always phone the same friend first (they get to know your recent edits, but they get all the work), or whoever picks up (even workload, but they may not know your latest edit)?

#### What we tested (A4)

R=W=1, same workload as E1:

| coordinator | stale reads | load share n1..n5 | busiest node |
|---|---|---|---|
| always the first replica | 0.1% | 16 / 16 / 22 / 25 / 22 % | 25% |
| random replica (**default**) | 26.5% | 21 / 18 / 18 / 21 / 23 % | 23% |

#### What it means

The first replica is fresher because every write to a key passes through the same node, so that node sees its own recent writes. But it only helps at R=W=1, it makes that node the busiest, and it breaks when that node is down. Raising R and W fixes freshness for every setup, so we keep **random** (this one is a judgement call you approved).

### B6. Strict or sloppy quorums?

#### The story

A friend is unreachable. A **strict** rule says "wait or fail: only the proper three friends count". A **sloppy** rule says "ask someone else to hold it for now and pass it on later" (**hinted handoff**).

<div align="center">

```
strict:  owner dead -> write refused         always consistent, less available
sloppy:  owner dead -> stand-in keeps it     always available, may be stale until handed over
```

</div>

#### What we tested (E3, X8)

A partition `{n1,n2} | {n3,n4,n5}` with clients on both sides:

* Stale reads at R=W=2: **0%** strict, **87% to 92%** sloppy (stand-ins are not in the read group).
* How much stays writable under different splits (X8, strict vs sloppy at R=W=2):

| shape | strict | sloppy |
|---|---|---|
| 1 node vs 4 | 72.8% | 82.0% |
| 2 vs 3 | 48.5% | 100% |
| 2 vs 2 vs 1 | 26.0% | 88.9% |
| flapping link | 85.7% | 99.1% |

#### What it means

Sloppy is far more available but gives up the R+W>N guarantee during the split. We make **strict the default** (correctness first) and let a client opt in to sloppy.

---

## 5. Part C: where does each key live?

### C1. The ring

#### The story

Picture a clock face. You hash a key to a spot on the clock and give it to the next three friends clockwise.

<div align="center">

```
             12
        n1   .   n2
     n5      .       n3        key hashes to a spot between n2 and n3
        n4   .   n5            -> stored on the next three: n3, n4, n5
```

</div>

#### The real version

`ring/ring.go`. A key's `PreferenceList(key, N)` is the next N distinct nodes clockwise. A **sloppy** write may spill past these onto further nodes.

### C2. Which hash function?

#### What we tested (A1)

100,000 keys, 5 nodes. Perfect balance means every node holds 60% of the copies. The coefficient of variation (CV) measures unevenness; lower is better:

| hash | 64 vnodes per node: min share / max share | CV |
|---|---|---|
| FNV-1a | 27.9% / 100.0% | 0.401 |
| MD5 | 56.7% / 66.3% | **0.056** |

#### What it means

MD5 spreads keys far more evenly (FNV mixes short, similar strings poorly). We use MD5.

### C3. Random slices (virtual nodes) or equal fixed pieces?

#### The story

You can give each friend many small **random** slices of the clock (some will get unlucky), or cut the clock into **equal pieces** and deal them out like cards.

<div align="center">

```
random vnodes:   | a |bb| c |aaa| b |cc| a |    unequal luck
fixed pieces:    | a | b | c | a | b | c | a |    equal and predictable
```

</div>

#### The real version

`NewRing` (random vnodes) and `NewFixedRing` (equal pieces, in `ring/ring.go`). With fixed pieces the whole clock is cut into `Q` equal **partitions**; partition `i` starts as node `i mod 5`'s. Each partition has a precomputed list of its owner nodes. `Config.Partitions` selects it (default 256).

#### What we tested (A1b, A8)

*How many random vnodes?* (A1b: 20 random placements per count, spread = most loaded node minus least loaded node, in points, ideal 0)

| vnodes per node | mean spread | worst spread | moved on join |
|---|---|---|---|
| 8 | 31.3 | 52.3 | 20.3% |
| 16 | 18.1 | 36.2 | 20.0% |
| **64** (old default) | **10.6** | 22.0 | 19.9% |
| 256 | 5.9 | 11.5 | 20.0% |
| 1024 | 2.6 | 4.5 | 20.2% |

Imbalance falls only like the square root of the vnode count: 64 was **acceptable, not good**. It had come from a coarse sweep of four values.

*Random vs fixed* (A8; "moved on join" = copies of keys a joining 5th node must receive; the ideal is 20%):

| scheme | min / max share | moved on join |
|---|---|---|
| random vnodes, 64 per node | 56.7% / 66.3% | 22.1% |
| **fixed partitions, Q=256** | **59.8% / 60.8%** | **19.9%** |

#### What it means

Fixed pieces are balanced within about **1 point** and move almost exactly the ideal amount when a node joins. They also remove the "how many vnodes" question and (see Part D) give Merkle trees a natural unit. We switched.

### C4. How many fixed pieces?

#### What we tested (A9)

| partitions Q | spread after a join (points) |
|---|---|
| 16 / 32 / 64 | about 18.9 |
| 128 | 2.5 |
| **256** | **1.0** |
| 512 | 1.4 |
| 4096 | 0.5 |

#### What it means

With too few pieces, dealing them out unevenly leaves a node short. 256 is the **smallest Q that balances to about 1 point**; bigger buys little and means more, smaller Merkle trees. We use **Q=256**. (Balance after a join depends on how the new node's pieces interleave with the old ones, so a smarter join could remove this sensitivity; not built.)

### C5. Why not simply `hash(key) mod number_of_nodes`?

#### The story

Deal the lists out by "list number mod 5". Perfectly even. But now a sixth friend joins and the rule becomes "mod 6": almost every list changes owner.

#### What we measured (A9)

| scheme | balance (spread) | copies moved when a node joins |
|---|---|---|
| modulo | 0.2 points | **40%** |
| fixed partitions Q=256 | 1.0 points | **19.9%** (ideal 20%) |

#### What it means

Modulo is even but moves **twice** the ideal amount of data on every membership change. Rejected, now with a measurement.

---

## 6. Part D: fixing copies that drifted apart

### D1. Do we need all three repair tools?

#### The story

While a friend was away, the other two notebooks got new entries. When the friend returns there are three ways it can be put right:

<div align="center">

```
hinted handoff : whoever held the entry as a stand-in hands it to the right friend
read repair    : someone reads, notices one notebook is behind, and fixes it right then
anti-entropy   : two friends compare notebooks and fix every difference
```

</div>

#### The real version

`store/repair.go` (hinted handoff, a loop every 5 ms), `coordGet` in `store/coordinator.go` (read repair) and `store/antientropy.go` (anti-entropy).

#### What we tested (A3)

Sloppy R=1, W=2 writes on both sides of a partition for 1 second, then heal and apply one repair. We count acknowledged writes that are **not yet on all 3 replicas**:

| repair after heal | still missing |
|---|---|
| none | 90.2% |
| hinted handoff only | 47.6% |
| read repair only (one read per key) | 6.4% |
| anti-entropy only (one round) | 6.4% |
| **handoff + anti-entropy** | **0.0%** |

#### What it means

Only the pair reaches zero. Handoff catches writes parked on stand-ins (which anti-entropy between owners does not see); anti-entropy catches everything else. Read repair only fixes keys that happen to be read (see D9).

### D2. How should two nodes compare their notebooks cheaply?

#### The story

Two friends each have 20,000 lines. To find the lines that differ, they could read every line to each other (long) or compare **fingerprints**: first a fingerprint of the whole notebook, then of halves, then of quarters, narrowing down to just the pages that differ. That is a **Merkle tree**. Section 7 explains it step by step with a worked example and the messages involved; here is the measured result.

#### What we tested (A6)

20,000 keys on all replicas, then a fraction of keys gets a newer version on one replica only. One round over all 10 node pairs. "Keys sent" counts keys put on the wire, both directions:

| strategy | difference | keys sent | KB sent | round trips |
|---|---|---|---|---|
| full state | 0% | 120,000 | 6,966 | 10 |
| full state | 1% | 120,000 | 6,976 | 10 |
| full state | 10% | 120,000 | 7,062 | 10 |
| **Merkle** | 0% | **0** | **9** | 10 |
| **Merkle** | 1% | **832** | **151** | 70 |
| **Merkle** | 10% | **8,240** | **1,145** | 70 |

A randomized test (`TestMerkleSyncMatchesFullStateSync`, 25 seeds) also proves that after one round Merkle leaves every replica with exactly the same data as full-state sync.

#### What it means

When copies already agree Merkle sends almost nothing (9 KB of root fingerprints instead of 7 MB); at 1% difference it sends **97% fewer bytes**. The honest cost: it needs more round trips (70 vs 10), so in our simulator, where each round trip costs time, it was not faster on the clock at 1% to 10% difference (285 ms vs 285 ms and 337 ms vs 283 ms). On a real network, bytes are usually the scarcer resource.

### D3. One tree per node pair, or one per partition?

#### The story

If every pair of friends builds its own private fingerprint tree, each new write must update many trees. If instead every **partition** (every ring piece) has one tree, and all three friends holding that piece keep an identical one, each write updates exactly one tree, and any two friends sharing the piece can compare directly.

#### The real version

`merkle/merkle.go` holds the tree and `Node.trees` maps `partition -> tree`. When a key is written, `Node.put` (`store/antientropy.go`) updates the tree of the key's partition. Fixed partitions are what made this natural: the ring has exactly `Q` pieces and each key belongs to exactly one.

A test (`checkTreesMatchData`) rebuilds every tree from scratch from the stored data and checks that the incrementally updated trees never drift from it.

#### What it means

One tree per partition: smaller trees, one update per write, and no rebuilding when a node pair changes. This is the design you chose.

### D4. How deep should each tree be?

#### The story

A tree of depth 0 is one fingerprint per partition: if it differs, you must send everything in the partition. A deeper tree narrows the difference down more precisely but needs more back-and-forth.

#### What we tested (A6 depth sweep, 1% difference)

| depth (leaves = 2^depth) | hashes sent | KB sent | round trips |
|---|---|---|---|
| 0 | 25,772 | 496 | 30 |
| 1 | 15,568 | 321 | 40 |
| 2 | 9,885 | 222 | 50 |
| 3 | 7,049 | 171 | 60 |
| **4** | **6,042** | **151** | **70** |

#### What it means

Each extra level cuts bytes but adds a round trip, with diminishing returns. **Depth 4** (16 leaves per partition) sent 151 KB, a third of depth 0, at 7 round trips per node pair. We use 4 (`Config.Node.MerkleDepth`).

### D5. Walk the tree one level at a time, or fetch it whole?

#### The story

You can ask "do the tops match?" then "do the left halves match?" and so on (many short phone calls), or say "send me the whole tree now" (one call, but a lot to read out).

<div align="center">

```
level by level:  ask, answer, ask, answer, ...       more calls, fewer bytes
whole tree:      ask once, get everything            fewer calls, more bytes
```

</div>

#### The real version

`merkleSync` in `store/antientropy.go` walks level by level. The option `MerkleOneShot` makes it send all the hashes of a differing partition at once.

#### What we tested (A10)

At depth 4:

| walk | difference | round trips | KB sent | time |
|---|---|---|---|---|
| level by level | 1% | 70 | **151** | 281 ms |
| one shot | 1% | 40 | 229 | 171 ms |
| level by level | 10% | 70 | **1,145** | 333 ms |
| one shot | 10% | 40 | 1,242 | 220 ms |

#### What it means

One shot is 52% heavier at 1% difference (229 vs 151 KB) and is faster only because the simulator charges time per round trip. We keep **level by level**: anti-entropy is background traffic and bytes matter most. (If round trips cost far more than bandwidth, one shot would win; the option is there.)

### D6. Update trees on every write (eager), or build them when asked (lazy)?

#### The story

Do you keep your fingerprints up to date as you write each line (a little work every time), or put the notebook down and work out the fingerprints only when a friend calls?

#### What we tested (A7, on branch `merkle-lazy`)

A CPU microbenchmark of the write path: lazy makes writes about **3x cheaper** when the same keys are written repeatedly (most updates are never hashed), and ties when each key is written once.

#### What it means

Lazy trees save CPU on write-heavy workloads but make the first anti-entropy slower and the code longer. You chose **eager**: the tree is always ready and the code is simpler. The lazy variant stays on its own branch.

### D7. How often should anti-entropy run?

#### The story

On `main`, nothing ever started anti-entropy: it only ran when a test or benchmark called it by hand. So after a partition the store did not heal by itself unless hints were waiting. It needs a clock: "every so often, pick a friend and compare notebooks".

#### The real version

`NodeConfig.AntiEntropyInterval` (`store/node.go`). If positive, each node runs a loop that syncs with one random reachable peer every interval.

#### What we tested (X3)

How long until all replicas agree after a heal, and what idle clusters cost:

| interval | time to heal | idle cost |
|---|---|---|
| 20 ms | 126 ms | 192 messages/s |
| **100 ms** | **404 ms** | **37 messages/s** |
| 500 ms | 2.3 s | 9 messages/s |
| 2 s | 6 s | 3 messages/s |

#### What it means

Heal time is roughly 4 times the interval, and idle traffic falls as the interval grows. **100 ms** is a good point in the simulator and is now the default. (A real cluster would use seconds to minutes. The idle cost is dominated by sending one root fingerprint per shared partition each round, about 77 per node pair; sending a single combined fingerprint first would cut that a lot. Not built.)

### D8. How often should hinted handoff run?

#### What we tested (X6)

Time to hand over every stored hint after a heal (after the bug fix in section 9):

| handoff interval | time to deliver all hints |
|---|---|
| 1, 5 or 20 ms | 115 ms |
| 100 ms | 151 ms |
| 500 ms | 563 ms |

#### What it means

Anything up to 20 ms is the same. We keep 5 ms.

### D9. Is read repair worth it?

#### The story

When you read from several friends and notice one of them is behind, you can fix their notebook on the spot, for free, since you already have the new line.

#### What we tested (X1, X7, A3)

* It adds 0.3 messages per operation (3.0 to 3.3, **+10%**).
* It does **not** reduce stale reads: 26.2% with, 26.1% without (the ranges overlap).
* It does help keys that actually get read (A3: one read per key reduces missing copies from 90% to 6%).

#### What it means

We keep it because it is cheap and helps read keys, but we **do not claim it makes reads fresher**. (Earlier we thought a 21.5% vs 21.8% difference was evidence; X1 showed that single runs are only precise to about 0.4 points, so that was noise.)

---

## 7. Merkle trees in detail, with the messages they send

This section is the "how" behind D2 to D5. Everything here is in `merkle/merkle.go` and `store/antientropy.go`. The numbers in the worked example are small and made up so it fits on a page; the measured numbers are in D2.

### 7.1 One tree, built from the keys

#### The story

A tree of fingerprints. At the bottom, each **leaf** is the fingerprint of a bundle of keys. Each box above it is the fingerprint of the two boxes below it. The top box is a fingerprint of everything. If two friends hold exactly the same notebook, their top boxes are equal.

<div align="center">

```
                         [ top ]
                        /       \
                  [ L ]           [ R ]
                  /   \           /   \
               [a]     [b]     [c]     [d]        <- leaves (depth 2 here)
               k1,k7   k3      k2,k9   k5          <- keys that hash to each leaf
```

</div>

#### The real version

* `newMerkle(depth)` allocates `2^(depth+1)` hashes, stored as a heap: node 1 is the top, node `i` has children `2i` and `2i+1`, and leaves are `2^depth .. 2^(depth+1)-1`. Each tree node is named `nodeRef{P, I}` (partition, index).
* A key's leaf is chosen by `leafOf`: a well-mixed version of the key's ring hash, so keys spread evenly over the leaves.
* A key's hash is `hashVersions(key, versions)`, a hash of the key and its whole sibling set. The leaf hash is the **XOR** of its keys' hashes. XOR makes an update cheap and order-free: to change one key, XOR out the old hash and XOR in the new one.
* After a leaf changes, `rehash` walks up to the top recomputing each parent with `mix(left, right)`. That is `depth` small steps per write.

### 7.2 The new messages (`store/messages.go`, `store/antientropy.go`)

Merkle anti-entropy adds six message types. A "round" between node `a` and node `b` goes:

<div align="center">

```
 a                                         b
 |-- merkleReq {refs, a's hashes} -------->|   "here are my fingerprints for these tree nodes"
 |<------------ merkleResp {diff} ---------|   "these ones are different"
 |-- merkleReq {children of diff} -------->|   one level per round trip ...
 |<------------ merkleResp {diff} ---------|   ... until only leaves are left
 |-- leafReq {leaves, a's key hashes} ---->|   "here are the key hashes under those leaves"
 |<---- leafResp {b's items, want} --------|   b sends keys whose hash differs and asks for keys it lacks
 |-- pushReq {a's items for want} -------->|   a sends what b asked for
```

</div>

| Message | Sent by | Carries | Receiver's job |
|---|---|---|---|
| `merkleReq{Refs, Hashes}` | the starter | tree nodes and the sender's hash for each | compare with its own hash for each ref |
| `merkleResp{Diff}` | the other node | the refs that differ | (the starter descends into these) |
| `leafReq{Leaves, Keys}` | the starter | differing leaves and every key hash under them | find keys whose hashes differ, find keys it lacks |
| `leafResp{Items, Want}` | the other node | its versions for keys that differ; keys it wants | the starter merges the items |
| `pushReq{Items}` | the starter | versions for the keys `Want` named | merge them (answered by the usual `putResp`) |

The old full-state `syncReq` / `syncResp` pair still exists (`FullStateSync`) so the two can be compared.

### 7.3 A worked example

Two nodes `a` and `b` share partition 7. They agree on everything except key `k2`, which `a` has a newer version of.

<div align="center">

```
round trip 1   a -> b   merkleReq{ (7, top) , hash 9f3a }
               b -> a   merkleResp{ Diff: [(7, top)] }          tops differ, so keep looking
round trip 2   a -> b   merkleReq{ (7, L) 41c2 ,  (7, R) d07e }
               b -> a   merkleResp{ Diff: [(7, R)] }            only the right half differs
round trip 3   a -> b   merkleReq{ (7, c) 5b11 ,  (7, d) 88aa }
               b -> a   merkleResp{ Diff: [(7, c)] }            only leaf c differs
round trip 4   a -> b   leafReq{ Leaves: [(7, c)], Keys: {k2: 77e1, k9: 02bd} }
               b -> a   leafResp{ Items: {k2: <b's old version>}, Want: [] }
               a merges b's old k2 into its own (nothing changes: a's is newer)
round trip 5   a -> b   pushReq{ Items: {k2: <a's newer version>} }
               b merges it. Both nodes now agree on k2.
```

</div>

Only the one key `k2` ever crossed the wire; every other key in the partition was settled by comparing fingerprints. If the tops had matched in round trip 1, the exchange would have ended there. That is what the **9 KB, 0 keys** row in D2 shows: at 0% difference the whole 10-pair round is only the root fingerprints.

### 7.4 Why the result is the same as sending everything

Merging two sibling sets is **idempotent and commutative** (`rules.merge`): applying a version twice or in a different order gives the same answer. So it does not matter that Merkle sends fewer keys; any key it skips was already identical. `TestMerkleSyncMatchesFullStateSync` checks this on 25 random divergent states, `TestMerkleRepairsPartition` checks a real partition with concurrent clients, and `TestMerkleOneShotAndPeriodic` checks the one-shot walk and the periodic loop.

---

## 8. Part E: when the network misbehaves

### E1. How long to wait for a replica before giving up?

#### The story

You phone a friend and nobody answers. Hang up too quickly and you give up on friends who were only a bit slow (a healthy request fails). Wait too long and when a friend is truly gone, everyone stands around waiting.

<div align="center">

```
timeout too short :  healthy but slow replies are treated as failures
timeout too long  :  a real failure holds the request for the whole timeout
```

</div>

#### The real version

`NodeConfig.ReplicaTimeout` (how long the coordinator waits), and the client waits three times that. It used to be a fixed 20 ms. It is now **computed from the network delays** (`AutoTimeout` in `store/cluster.go`): two hops of `base + 4 x mean jitter + slow-path extra`, never below 1 ms. If `ReplicaTimeout` is 0, `NewCluster` computes it.

#### What we tested (A5, X2)

*A5, default network, fixed timeouts:*

| timeout | healthy put failures | a failed request takes |
|---|---|---|
| 2 ms | 84% | 3.6 ms |
| 5 ms | 7.6% | 7.0 ms |
| **20 ms** | **0%** | 22 ms |
| 100 ms | 0% | 103 ms |

*X2, healthy put failures when the network speed changes:*

| timeout | LAN | default | cross-region (5 ms hops) |
|---|---|---|---|
| 5 ms | 0.0% | 2.0% | **100%** |
| 20 ms | 0.0% | 0.0% | **22%** |
| 100 ms | 0.0% | 0.0% | 0.0% |
| **computed** | 0.1% | 0.0% | **0.0%** |

#### What it means

A fixed value that is right on one network is wrong on another: 20 ms was perfect on the default network and failed one put in five at cross-region speeds. The computed timeout stays near zero failures on all three, and on the default network comes to about 17 ms.

### E2. What if the failure detector is wrong?

#### The story

You wrongly decide a healthy friend is unreachable. You write to a stand-in instead, and later pass it on.

#### The real version

`Network.Reachable` normally tells the truth. `Network.SetFalseSuspicion(p)` makes it wrongly say "down" with probability `p`, while real delivery still follows the true state.

#### What we tested (X8)

20% false suspicion, sloppy quorums:

* stale reads: 0.2%
* messages: +6%
* about 150 extra stored hints
* failed puts: **0**

#### What it means

A wrong failure detector costs a little extra traffic and some hints, **not correctness**. (Before this test the exact detector flattered every availability number.)

### E3. Other ways to split the network

#### What we tested (X8)

Splits of 1 vs 4, 2 vs 3, 2 vs 2 vs 1, and a flapping link (heal and break repeatedly). Availability numbers are in B6.

#### What it means

In **every** shape, **no acknowledged write was lost** and every replica agreed after the heal. Strict quorums are very unavailable in a three-way split (26%), which is the price of their correctness.

---

## 9. A bug the experiments found

#### The story

A friend holds a stack of notes for a neighbour who was away. When the neighbour returns, the friend starts handing over notes but gives themselves **one stopwatch of 20 seconds for the whole stack**. With a big stack, the stopwatch runs out before the last notes are delivered, so the friend assumes the whole delivery failed, keeps the entire stack, and starts again next round with the same big stack. They never finish.

#### The real version

`handoffHints` in `store/repair.go` used **one** 20 ms deadline for all of an owner's keys, and kept every hint unless **every** send succeeded. With about 15 or more hinted keys the later sends always timed out, so nothing was ever removed. X6 first showed it as "hints not drained after 10 seconds".

#### The fix

Each send gets its own timeout; keys that were delivered are removed even if a later one fails; the round stops at the first failure. The regression test `TestHandoffDeliversManyHints` (150 hints) failed before the fix and passes after.

#### What it means

This was in `main` too, so the fix also lives on its own small branch, `fix-handoff-timeout`, and is now merged into `main`. Earlier results that depended on hints draining (A3's handoff row, E3 after heal) were regenerated; the conclusions held because anti-entropy repaired what handoff missed. That is exactly the kind of bug that benchmarks of an unrelated question surface.

---

## 10. The final recipe and what changed from `main`

| Choice | Value | Evidence |
|---|---|---|
| clocks | dotted version vectors | A2, E2 |
| conflicts | keep siblings, client merges | E2 |
| replication payload | whole sibling set | A2 |
| copies per key, nodes | N=3, 5 nodes | X4 |
| quorums | R and W per client, strict by default | E1, E3, X8 |
| coordinator | random replica | A4 |
| asking replicas | all N, use the first R or W | X7 |
| hash | MD5 | A1 |
| placement | 256 fixed equal partitions | A1b, A8, A9 |
| repair | hinted handoff + read repair + Merkle anti-entropy | A3, X7 |
| Merkle trees | one per partition, eager, depth 4, level by level | A6, A7, A10 |
| anti-entropy schedule | background loop, 100 ms | X3 |
| replica timeout | computed from the network delays | A5, X2 |

What differs from `main` (non-comment lines of `store/`: 1,151 on `main`, about 1,616 on this branch):

1. Merkle anti-entropy replaces sending everything (largest addition: `merkle/merkle.go`, `store/antientropy.go`).
2. 256 fixed partitions replace 64 random vnodes, with a tree per partition.
3. Anti-entropy runs in the background every 100 ms; on `main` nothing started it.
4. The replica timeout is derived from the network model instead of fixed at 20 ms.
5. The hinted-handoff fix (now also on `main`).
6. Optional switches, all off by default: `ReadFromROnly`, `MerkleOneShot`, false suspicion, the message counter.
7. Tests, benchmarks (A1b, A6 to A10, X1 to X8), the chart package, and docs.

---

## 11. Honest limits

* One in-process simulation with a made-up network. A real network is on the separate `real-network-and-wal` branch.
* Stale-read percentages belong to our delays and workload; quote them as ranges.
* **Not yet tested:** sibling and clock growth when many writers work through a long partition (there is no cap on siblings), mixed R and W per client in one run, a node joining a running cluster (the ring was tested on its own, not live), and hash-collision risk of the 64-bit XOR leaf hashes (about 2^-64; argued, not measured).

---

## 12. Re-running everything

```bash
go test -race ./...           # correctness tests
go run ./cmd/bench            # E1-E3  -> results/benchmark.md
go run ./cmd/bench -ablate    # X1-X8  -> results/ablations.md (use -only X3 for one)
go run ./cmd/archbench        # A1-A10 -> results/architecture.md (use -only A6 for one)
go run ./cmd/plots            # redraw the charts for E1-E3 and A1-A5
```

---

## 13. Glossary

| Word | Plain meaning |
|---|---|
| replica | one of the copies of a key (N of them) |
| N, R, W | copies per key; how many to ask for a read; how many must confirm a write |
| quorum | the group of R or W nodes you wait for |
| stale read | a read that returns an older value than the latest completed write |
| coordinator | the node that runs one read or write for a client |
| version, dot, clock | one stored value; its unique tag `(node, counter)`; the record of which writes it had seen |
| sibling | one of several concurrent versions of a key kept side by side |
| ring, partition | the clock-face of keys; one equal piece of it (256 of them) |
| vnode | a small random slice of the ring owned by a node (the old way) |
| strict / sloppy quorum | only the proper replicas count / a stand-in may take a write while an owner is unreachable |
| hinted handoff | a stand-in holds a write and later hands it to the real owner |
| read repair | fixing an out-of-date replica when a read notices it |
| anti-entropy | two nodes compare and fix every difference between their copies |
| Merkle tree | a tree of fingerprints that finds the differing keys without sending everything |
| leaf, depth | the bottom of a tree, holding a bundle of keys; how many levels the tree has |
| round trip | one request and its answer |
| false suspicion | wrongly believing a healthy node is down |
| ablation | the same test with one choice changed, to measure that choice alone |
| spread | most-loaded node's share minus least-loaded node's share, in percentage points |
