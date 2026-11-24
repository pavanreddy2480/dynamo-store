# ABLATION: why every choice in this store was made, in plain words

This is the "explain it like I'm 5" companion to [ARCHITECTURE.md](ARCHITECTURE.md) (how it is built) and [REPORT.md](REPORT.md) (results). The detailed tables live in [docs/ABLATIONS.md](docs/ABLATIONS.md) and [docs/DECISIONS_AUDIT.md](docs/DECISIONS_AUDIT.md); every chart is in `results/`.

## Contents

0. [What is an ablation?](#0-what-is-an-ablation)
1. [The one picture to remember](#1-the-one-picture-to-remember)
2. [Part A: never lose a shopping-cart item](#2-part-a-never-lose-a-shopping-cart-item)
3. [Part B: how strict should reads and writes be?](#3-part-b-how-strict-should-reads-and-writes-be)
4. [Part C: where does each key live?](#4-part-c-where-does-each-key-live)
5. [Part D: fixing copies that drifted apart](#5-part-d-fixing-copies-that-drifted-apart)
6. [Part E: when the network misbehaves](#6-part-e-when-the-network-misbehaves)
7. [The final recipe](#7-the-final-recipe)
8. [Honest limits](#8-honest-limits)
9. [Re-run everything](#9-re-run-everything)
10. [Glossary](#10-glossary)

---

## 0. What is an ablation?

An ablation is a fair fight between two choices.

<div align="center">

```
   choice A  ----+                                                   
                 +--> same test, same machine --> compare the numbers
   choice B  ----+                                                   
```

</div>

We change **one thing at a time**, keep everything else fixed, and measure. If the number moves, the choice matters. If it does not move, the choice does not matter and we pick the simpler one. Each run is repeated 3 times; the numbers in brackets are the lowest and highest run, so you can see how noisy it is (very little: about 0.4 points).

The system is a toy cluster inside one program: 5 nodes, every key copied to 3 of them (N=3). Messages between nodes get a made-up delay, so **compare numbers to each other, not to a real data centre.**

## 1. The one picture to remember

A client stores a shopping cart. The cart is copied to 3 nodes. To write, we wait for **W** nodes to say "got it". To read, we ask and wait for **R** nodes to answer.

<div align="center">

```
 client --put--> [ node A ] [ node B ] [ node C ]     N = 3 copies                    
                    ok         ok        (slow)       W = 2 -> done after 2 acks      
 client <-get--  [ node A ] [ node B ] [ node C ]                                     
                    ok         ok                     R = 2 -> read 2, take the newest
```

</div>

If **R + W > N** the read group and the write group must overlap, so a read always sees the latest write. If R + W <= N they can miss each other and you may read an old value ("stale").

Everything below is about the questions that come next: where do keys live, what if nodes disagree, what if the network breaks, how do copies get fixed.

---

## 2. Part A: Never lose a shopping-cart item

**The story.** You and your partner both add things to the same shopping list at the same moment, on two different phones that cannot talk. When the phones reconnect, nobody wants an item to vanish.

**The real version.** The choices below decide whether the store keeps or drops writes that happen at the same time.

### A1. Which kind of clock? (dotted version vectors vs plain vector clocks)

A clock says "this write happened after that one" or "these two happened at the same time". Plain per-node clocks sometimes cannot tell two writes that went through the same node apart, so one gets thrown away.

<div align="center">

```
 plain clock:   two carts written at once on node A  -->  "one is older"  -->  one cart DELETED     
 dotted clock:  each write gets its own tag (A,7)    -->  "both are new"   -->  both KEPT (siblings)
```

</div>

Test: 8 clients add items to one cart at the same time (experiment A2).
* dotted version vector: **0%** of acknowledged items lost
* plain per-node clock: **20%** lost at R=W=1, **26%** at R=W=2

Effect: dotted clocks are the only safe choice here.

### A2. Copy the whole pile of siblings, or only the new write?

When a node gets a write it can send just the new version, or all the versions it holds for that key.
* whole sibling set: 0 items lost
* new version only: 2 to 3 items lost over thousands of writes

Tiny numbers, but the whole-set rule is also provably correct, so we use it.

### A3. When two versions conflict: keep both (siblings) or last-writer-wins?

Two people edit the cart at the same time. Last-writer-wins keeps the one with the later timestamp and silently drops the other. Siblings keep both and let the client merge them (for a cart, merge = union).
* siblings + client union: **0%** lost
* last-writer-wins: **84%** of concurrent items lost (experiment E2)

Effect: LWW is only kept as a comparison.

---

## 3. Part B: How strict should reads and writes be?

**The story.** You keep 3 notebook copies. Before you trust an answer, how many friends must you ask? Ask one and it is quick but maybe old. Ask two and it is surer but slower.

**The real version.** R and W are that number of friends for reads and writes. The tests below show what each setting costs and buys.

### B1. The R and W dial (experiment E1)

32 keys, one writer per key, 16 readers, 2 seconds.

| R, W | stale reads | what it feels like |
|---|---|---|
| 1, 1 | about 26% | fastest, often old data |
| 1, 2 or 2, 1 | 0.2% to 1% | nearly fresh |
| 2, 2 and above (R+W > N) | **0%** | always fresh, slower (p99 about 8 to 12 ms) |

### B2. Do these numbers depend on how fast the network is? (X2)

Yes, a lot. The "26%" is not a law of nature, it is a property of our made-up delays.

| network | R=W=1 stale reads |
|---|---|
| fast LAN (0.1 ms hop) | 8.3% |
| default (0.4 ms hop) | 26.2% |
| cross-region (5 ms hop) | 40.7% |

What does **not** change: R+W>N is 0% stale in all three. So in the report, quote stale reads as a **range**, and state the network.

### B3. Does workload shape matter? (X5) And node count, and N? (X4)

* Workload: R=W=1 stale reads go from 9.8% (256 keys, so few collisions) to 26.5% (32 keys, so reads often hit just-written keys).
* Node count: 5 nodes vs 7 nodes, same answer (26.4%). Adding machines does not change freshness.
* N (copies per key) does: R=W=1 stale is 15.2% at N=2, 26.4% at N=3, 23.4% at N=5. A majority quorum is 0% stale at every N.

Decision: N=3, 5 nodes, as in the Dynamo paper.

### B4. Ask all N replicas and take the first R? Or ask only R? (X7)

<div align="center">

```
 ask all 3, use the first R answers        ask only R                                 
   fast: a slow node is simply ignored       slow node in the group -> you wait for it
```

</div>
* Ask only R: 15% fewer messages, but read p99 doubles (5.1 to 10.7 ms), and with a slow replica 9.8 to 20.0 ms.

Decision: keep asking all N. Speed beats a few saved messages.

### B5. Which coordinator takes the request? (A4)

A coordinator is the node that runs one read or write.
* always the first replica: 0.1% stale reads, but the first node is busiest (25% of the load)
* random replica: 26.5% stale at R=W=1, load more even (18% to 23%)

Why the first is fresher: all writes to a key go through the same node, so it sees its own recent writes. Why random was chosen: even load, no hot spot, nothing breaks if that node is down. If you need freshness, raise R and W instead, which fixes staleness for every setup.

### B6. Strict quorums or sloppy quorums? (E3, X8)

When the right replicas are unreachable, a **sloppy** quorum lets a stand-in node accept the write and hand it over later ("hinted handoff"). A **strict** quorum just says no.

<div align="center">

```
 strict:  node dead -> write refused         (always consistent, less available)           
 sloppy:  node dead -> stand-in takes it     (always available, may be stale until handoff)
```

</div>
* strict R=W=2 stale reads: **0%**; sloppy R=W=2: **87% to 92%** stale during a partition
* availability when the cluster is split in two (X8, strict vs sloppy): 1 vs 4 nodes 72.8% vs 82.0%; 2 vs 3 nodes 48.5% vs 100%; three-way split 26.0% vs 88.9%

Decision: strict by default (correct first), sloppy opt-in per client.

---

## 4. Part C: Where does each key live?

**The story.** You have 5 friends and many shopping lists. You need a rule that says which 3 friends hold which list, that spreads lists evenly, and that does not reshuffle everything when a new friend joins.

**The real version.** This is the consistent-hash ring and its partitions.

### C1. The ring

Imagine a clock face. Each key is hashed to a spot on the clock and stored on the next 3 nodes clockwise.

<div align="center">

```
            0                                                 
        n1  .  n2                                             
      n5   KEY>>>   n3      key lands here, goes to n3, n4, n5
        n4  .  n5                                             
```

</div>

### C2. Which hash function? (A1)

Keys should spread evenly. We measured how uneven (smaller is better): MD5 gave 0.056, FNV-1a gave 0.401. MD5 spreads far better. Decision: MD5.

### C3. Random slices (virtual nodes) or equal fixed slices? (A1b, A8)

Each node can own many small random slices of the clock (virtual nodes), or the clock can be cut into **equal pieces** (partitions) dealt out to the nodes.

<div align="center">

```
 random vnodes:   |a|bb|c|aaa|b|cc|a|      unequal luck, some nodes get more
 fixed pieces:    |a|b|c|a|b|c|a|b|c|      equal and predictable            
```

</div>
* 64 random vnodes per node: max-minus-min share is about **10 points** (one node 10 points more loaded than another), and a joining node moves 22.1% of copies.
* 256 fixed partitions: spread about **1 point**, and a joining node moves 19.9% of copies (the ideal is 20%).
* The old choice of 64 vnodes came from a coarse test. The finer sweep (A1b, 11 counts) shows 64 is acceptable but not best: imbalance falls with the square root of the vnode count.

Decision: fixed partitions. This also removes the "how many vnodes" question.

### C4. How many fixed pieces? (A9)

| partitions Q | spread (points) |
|---|---|
| 16, 32, 64 | about 18.9 |
| 128 | 2.5 |
| **256** | **1.0** |
| 512 | 1.4 |
| 4096 | 0.5 |

Decision: Q=256, the smallest size that is balanced to about 1 point. Bigger buys little and makes more small trees (see part D).

### C5. Why not `hash(key) % number_of_nodes`? (A9)

It is perfectly even, but when a node joins almost everything changes owner.
* modulo hashing: **40%** of copies move on a join
* fixed partitions: **19.9%** (ideal 20%)

Decision: modulo is rejected, now with a measurement.

---

## 5. Part D: Fixing copies that drifted apart

**The story.** While a friend was away, the other two notebooks got new items. When the friend returns, someone must bring their notebook up to date, without re-reading every page of every notebook.

**The real version.** These are the three repair tools and the Merkle trees that make comparing cheap.

Copies drift apart when a node is down or cut off. Three repair tools, each with a different job:

<div align="center">

```
 hinted handoff : a stand-in kept the write, now hands it to the right node
 read repair    : a read notices an old copy and fixes it on the spot      
 anti-entropy   : two nodes compare everything and fix every difference    
```

</div>

### D1. Do we need all three? (A3)

After a partition heals, how many acknowledged writes are still missing from some copy:

| repair | still missing |
|---|---|
| none | 90.2% |
| hinted handoff only | 47.6% |
| read repair only | 6.4% |
| anti-entropy only | 6.4% |
| **handoff + anti-entropy** | **0%** |

Only the pair reaches zero, so the default has both.

### D2. How does anti-entropy compare two nodes cheaply? Merkle trees (A6)

Naive way: send everything and compare. Merkle way: each node keeps a **tree of fingerprints**. Compare the top fingerprint; if it matches, stop; if not, go one level down only where fingerprints differ.

<div align="center">

```
        [top]              same?  -> done, nothing sent          
       /     \                                                   
    [L]       [R]          differ -> look inside only that side  
    / \       / \                                                
  [a] [b]   [c] [d]        differ -> send just the keys under [c]
```

</div>
20,000 keys, one round over 10 node pairs:

| how many keys differ | full-state sends | Merkle sends |
|---|---|---|
| 0% | 120,000 keys (7 MB) | **0 keys (9 KB)** |
| 1% | 120,000 keys | **832 keys** (151 KB) |
| 10% | 120,000 keys | **8,240 keys** (1.1 MB) |

Merkle sends 97% fewer bytes at 1% difference. Honest cost: it needs more round trips (70 vs 10), so in our simulator it was not faster on the clock at 1% and 10%. A randomized test also proves Merkle ends with exactly the same data as full-state sync.

### D3. One tree per piece, how deep, eager or lazy

* **One tree per partition.** The 256 pieces each get their own small tree; two nodes sharing a piece hold identical trees. Fits the fixed-partition choice.
* **Depth 4** (16 leaves per tree): from a depth sweep, deeper trees send fewer hashes and fewer bytes but need more round trips; depth 4 sent 151 KB at 1% difference with 7 round trips per pair, while depth 0 sent 496 KB.
* **Eager** (update the tree on every write) vs **lazy** (rebuild when asked): lazy makes writes about 3x cheaper, but ties when each key is written once. We took eager for simplicity: the tree is always ready and the code is shorter.

### D4. Walk the tree one level at a time, or fetch it all at once? (A10)

<div align="center">

```
 level by level: ask, answer, ask, answer ...     (more trips, fewer bytes)
 whole tree:     ask once, get everything         (fewer trips, more bytes)
```

</div>
At depth 4: 70 round trips vs 40, but the whole-tree version sends **52% more bytes** at 1% difference (229 vs 151 KB). Decision: level by level; bytes are the scarce resource.

### D5. How often should anti-entropy run on its own? (X3)

Before, nothing started it, so after a partition the store did not heal by itself unless hints existed. Now it runs every T milliseconds against a random reachable peer.

| every | time to heal | idle cost |
|---|---|---|
| 20 ms | 126 ms | 192 messages/s |
| **100 ms** | **404 ms** | **37 messages/s** |
| 500 ms | 2.3 s | 9 messages/s |
| 2 s | 6 s | 3 messages/s |

Decision: **100 ms** is the default (a real cluster would use seconds or minutes).

### D6. How often should hint hand-over run? (X6)

Time to hand over all stored hints after a heal: 115 ms at 1, 5 and 20 ms intervals; 151 ms at 100 ms; 563 ms at 500 ms. Decision: keep 5 ms; anything up to 20 ms is the same.

### D7. Is read repair worth it? (X1, X7)

It adds 0.3 messages per operation (+10%) and does not change how often stale reads happen (26.2% vs 26.1%). It helps keys that actually get read. Decision: keep it, but do not claim it makes reads fresher.

### D8. The bug these tests found

While testing D6 the hand-over **never finished** once a node held about 15 or more hints. It used one shared 20 ms deadline for all keys and kept every hint unless every send succeeded, so later sends always timed out. Fix: each send gets its own timeout and only delivered hints are removed. Test `TestHandoffDeliversManyHints` (150 hints) failed before and passes now. The fix is on branch `fix-handoff-timeout`.

---

## 6. Part E: When the network misbehaves

**The story.** You phone a friend and nobody answers. How long do you wait before assuming the line is cut? And what if you wrongly think a friend is gone?

**The real version.** These are the timeout, the failure detector, and different ways the network can split.

### E1. How long to wait for a replica before giving up? (A5, X2)

Too short and healthy requests fail because of a slow message. Too long and a real failure makes everyone wait.

| timeout (default network) | healthy put failures | failed request takes |
|---|---|---|
| 2 ms | 84% | 3.6 ms |
| 5 ms | 7.6% | 7.0 ms |
| 20 ms | 0% | 22 ms |
| 100 ms | 0% | 103 ms |

The right value depends on the network: at 5 ms hops a fixed 20 ms timeout failed 23% of puts, while a LAN with a fixed 100 ms timeout would wait far too long. Decision: the timeout is now **computed from the network delays** (about two hops of a slow message), so it adapts. On the default network it comes to about 17 ms. Measured: the computed timeout gave 0.0% put failures on the cross-region network (a fixed 20 ms gave 22%, 5 ms gave 100%), 0.0% on the default network and 0.1% on the LAN.

### E2. What if the failure detector is wrong? (X8)

We made nodes wrongly believe a healthy node was down 20% of the time. Result: 0.2% stale reads, 6% more messages, about 150 extra stored hints, and **0 failed puts**. A bad detector costs a little traffic, not correctness.

### E3. Other ways to split the network (X8)

Splits of 1 vs 4, 2 vs 3, 2 vs 2 vs 1, and a flapping link. In every shape no acknowledged write was lost and all replicas agreed after healing.

---

## 7. The final recipe

| choice | value | proven by |
|---|---|---|
| clocks | dotted version vectors, keep siblings, client merges | A2, E2 |
| replication | whole sibling set | A2 |
| copies per key, nodes | N=3, 5 nodes | X4 |
| quorums | per client, strict by default | E1, E3, X8 |
| coordinator | random replica | A4 |
| asking replicas | all N, use the first R or W | X7 |
| key placement | 256 equal partitions, MD5 | A1, A8, A9 |
| repair | hinted handoff + read repair + Merkle anti-entropy | A3, X7 |
| Merkle trees | one per partition, eager, depth 4, level by level | A6, A7, A10 |
| anti-entropy interval | 100 ms (simulation) | X3 |
| replica timeout | computed from the network delays | A5, X2 |

## 8. Honest limits

* One in-process simulation, not a real network (the `real-network-and-wal` branch has that).
* Not yet tested: many writers causing sibling pile-ups during a long partition, different R,W per client in one run, nodes joining a live cluster, and hash-collision risk (argued, not measured).
* Stale-read percentages belong to our made-up delays and workload. Quote them as ranges, as in B2 and B3.

## 9. Re-run everything

```
go test -race ./...           # correctness
go run ./cmd/bench            # E1-E3  -> results/benchmark.md
go run ./cmd/bench -ablate    # X1-X8  -> results/ablations.md
go run ./cmd/archbench        # A1-A10 -> results/architecture.md
go run ./cmd/plots            # redraw charts
```

## 10. Glossary

| word | plain meaning |
|---|---|
| replica | one of the 3 copies of a key |
| N, R, W | copies per key, how many to ask when reading, how many must confirm a write |
| stale read | a read that returns an older value than the latest write |
| quorum | the group of R or W nodes you wait for |
| sibling | one of several conflicting versions kept side by side |
| vector clock / dot | a tag that says which writes a version has seen |
| coordinator | the node that runs one read or write |
| hinted handoff | a stand-in holds a write for a down node and hands it over later |
| read repair | fixing an old copy when a read notices it |
| anti-entropy | two nodes compare and fix every difference |
| Merkle tree | a tree of fingerprints that finds differences without sending everything |
| partition (ring) | one of the equal pieces of the key space; also, in networks, a split cluster |
| false suspicion | wrongly believing a healthy node is down |
