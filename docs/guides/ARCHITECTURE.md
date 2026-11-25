# ARCHITECTURE: dynamo-store explained from zero

This guide walks through the whole project in the order you need to understand it: first the problem, then the idea for solving it, then how the pieces talk to each other, and finally every piece on its own, starting with the smallest and building up. Every section uses a small everyday story first and then shows what the Go code actually does, with the file name so you can open it and follow along.

It describes the current design (branches `adaptive-defaults` and `ablations`), which runs the whole system as a **simulation inside one Go program**. The "network" is fake (messages are passed in memory with a random delay), which lets us create network failures on purpose and measure what happens.

> **What is different from the first version of this guide.** The ring is now **256 equal fixed partitions** instead of 64 random flags per node. Anti-entropy compares **Merkle trees** instead of sending everything, and a **background loop** starts it every 100 ms. The replica timeout is **computed from the network speed** instead of fixed at 20 ms. Hinted handoff had a bug (one shared stopwatch) that is fixed. Why each change was made, with measurements, is in [ABLATION.md](ABLATION.md); the reference version with decision numbers (D1 to D15) is [ARCHITECTURE_DECISIONS.md](../reference/ARCHITECTURE_DECISIONS.md); every message is in [MESSAGES.md](MESSAGES.md).

> **Tip:** If a word is new, check the [glossary](#16-glossary) at the end.

---

## Contents

1. [The problem we are solving](#1-the-problem-we-are-solving)
2. [How we approach it: the big ideas](#2-how-we-approach-it-the-big-ideas)
3. [The whole picture in one drawing](#3-the-whole-picture-in-one-drawing)
4. [How the components talk: a write and a read](#4-how-the-components-talk-a-write-and-a-read)
5. [Component 1: vector clocks (`vclock/`)](#5-component-1-vector-clocks-vclock)
6. [Component 2: versions and dots (`store/version.go`)](#6-component-2-versions-and-dots-storeversiongo)
7. [Component 3: merge rules (`store/merge.go`)](#7-component-3-merge-rules-storemergego)
8. [Component 4: the ring (`ring/ring.go`)](#8-component-4-the-ring-ringringgo)
9. [Component 5: the network (`network/network.go`, `store/messages.go`)](#9-component-5-the-network-networknetworkgo-storemessagesgo)
10. [Component 6: the node and the coordinator (`store/node.go`, `store/coordinator.go`)](#10-component-6-the-node-and-the-coordinator)
11. [Component 7: the three repair helpers (`store/repair.go`)](#11-component-7-the-three-repair-helpers)
12. [Component 8: the client, the resolver and the cluster](#12-component-8-the-client-the-resolver-and-the-cluster)
13. [Putting it all together: the partition story, step by step](#13-putting-it-all-together-the-partition-story)
14. [Under the hood: how each component is implemented](#14-under-the-hood-how-each-component-is-implemented)
15. [The programs in `cmd/` and what they prove](#15-the-programs-in-cmd-and-what-they-prove)
16. [Glossary](#16-glossary)

---

## 1. The problem we are solving

### The story

Imagine you keep your shopping list in **one notebook**. Easy. You read it, you add "milk", done.

But one day the notebook falls in a puddle and the list is gone. So you get smart: you keep **three copies** of the notebook, at three friends' houses. Now if one notebook is ruined, two others still have the list.

That fixes one problem and creates three new ones:

1. **Copies can disagree.** You told Friend A to add "eggs", but Friend B hasn't heard yet. If you ask Friend B, you get an old list. That is a **stale read**.
2. **Friends can be unreachable.** The phone line to Friend A is cut. Do you refuse to add anything until the line is fixed (safe, but annoying), or do you write it down somewhere and fix it later (always works, but copies drift apart)?
3. **Two people can change the list at the same time.** You add "eggs" through Friend A while your partner adds "bread" through Friend B, and the two friends can't talk to each other. When the phone line comes back, which list is right? Both! Neither of you knew about the other. Throwing one away would lose something somebody was promised was saved.

### The real version

That is exactly the problem a **replicated key-value store** has. A key-value store is a giant dictionary: you `Put(key, value)` and later `Get(key)`. "Replicated" means each key is stored on several computers (here, **3 copies** spread over **5 computers**) so a broken computer loses nothing.

The course problem asks us to build one in the style of **Amazon's Dynamo** and to:

| Requirement | Plain words |
|---|---|
| Store every key on 3 replicas | Three notebooks per list |
| Let the client choose **R** and **W** | Let the caller decide how many friends must answer before a read or a write counts |
| Track concurrent versions with **vector clocks** | Know when two changes happened "at the same time" without knowing about each other |
| Return conflicting versions to the client | Don't secretly pick a winner; show both and let the caller decide |
| Survive **network partitions** and reconnect | Keep working when phone lines are cut, and tidy up afterwards |
| Report how often reads are stale | Measure problem 1 for each choice of R and W |

---

## 2. How we approach it: the big ideas

Each problem above gets one idea. Don't worry about the details yet; every idea has its own section later.

| Problem | Idea | One-line explanation | Where in the code |
|---|---|---|---|
| Where does each key live? | **Consistent-hash ring with fixed partitions** | Cut a circle into 256 equal pieces dealt out to the 5 computers; a key lives on the first 3 computers clockwise from its piece | `ring/ring.go` |
| Who handles a request? | **Coordinator** | The client picks one of the key's 3 computers; that one talks to the other two on the client's behalf | `store/coordinator.go` |
| How many answers are enough? | **Quorums R and W** | A write waits for W "saved!" replies, a read waits for R answers. If R + W > 3, reads can't be stale | `store/coordinator.go` |
| What if a replica is unreachable? | **Strict vs sloppy quorum + hinted handoff** | Strict: refuse. Sloppy: give the write to a stand-in computer with a sticky note "this belongs to n4", and the stand-in delivers it later | `store/coordinator.go`, `store/repair.go` |
| Did two changes happen at the same time? | **Dotted version vectors** (a sharper kind of vector clock) | Each write gets a unique name and remembers which writes its author had seen | `vclock/`, `store/version.go` |
| What do we do with two "at the same time" changes? | **Siblings + client merge** | Keep both, return both, the client merges them (here: union of the shopping items) and writes the result back | `store/merge.go`, `store/client.go`, `store/resolver.go` |
| How do copies catch up after a failure? | **Read repair, hinted handoff, Merkle anti-entropy** | Three cleaners that copy missing versions to replicas that lack them; the third compares fingerprint trees so it only sends what differs, and runs by itself every 100 ms | `store/coordinator.go`, `store/repair.go`, `store/antientropy.go`, `merkle/merkle.go` |
| How do we test failures safely? | **Simulated network** | An in-memory network with random (but repeatable) delays, which we can cut on purpose | `network/network.go` |

For comparison, the code also has a simpler, worse policy, **last-writer-wins (LWW)**: keep only the version with the latest timestamp. It is there to show, with numbers, what goes wrong when you secretly pick a winner.

---

## 3. The whole picture in one drawing

<div align="center">

```
                      +----------------------------------------------------------+
                      |                     Cluster  (cluster.go)                |
                      |                                                          |
+----------+  Get/Put |   +------------------- Network (network.go) -----------+ |
| Client A |----------+-->|  in-memory "post office": every message waits a    | |
| R=2, W=2 |<---------+---|  random delay, and is DROPPED if sender and        | |
+----------+          |   |  receiver are on different sides of a partition    | |
                      |   +---^-------^--------^--------^--------^-------------+ |
+----------+          |       |       |        |        |        |               |
| Client B |----------+--> +--+-+  +--+-+   +--+-+   +--+-+   +--+-+             |
| R=1, W=1 |<---------+--- | n1 |  | n2 |   | n3 |   | n4 |   | n5 |  Nodes      |
+----------+          |    +----+  +----+   +----+   +----+   +----+  (node.go)  |
                      |       every node: stores data, stores hints,             |
                      |       can act as coordinator, runs repair                |
                      |                                                          |
                      |   Ring (ring.go): tells everyone which 3 nodes own a key |
                      +----------------------------------------------------------+
```

</div>

* **Clients** are the callers. Each client has its own R and W.
* **Nodes** `n1`..`n5` are the storage computers. They are all equal: any of them can store data and any of them can be the coordinator for a request.
* The **Network** carries every message between clients and nodes and between nodes.
* The **Ring** is shared knowledge: given a key, it answers "which 3 nodes keep it?".
* The **Cluster** builds all of this and offers a few whole-system buttons (run anti-entropy, add clock skew, wait for hints).

### How the code depends on itself

Read the components in this order; each one only uses the ones above it:

<div align="center">

```
vclock/vclock.go            counting "what have I seen?"                      
      |                                                                       
      v                                                                       
store/version.go            one stored value + its history (Dot, Version)     
      |                                                                       
      v                                                                       
store/merge.go              rules: which version replaces which               
      |                                                                       
      v                                                                       
ring/ring.go               which nodes own which key                         
network/network.go            fake network with delays and partitions           
store/messages.go           the envelopes sent over the network               
      |                                                                       
      v                                                                       
store/node.go               one storage computer                              
store/coordinator.go        a node handling a client's request                
store/repair.go             hinted handoff                                    
merkle/merkle.go             fingerprint tree of one partition's keys          
store/antientropy.go        compare two nodes' trees, exchange the differences
      |                                                                       
      v                                                                       
store/client.go             what the application calls: Get / Put             
store/resolver.go           how the application merges siblings               
store/cluster.go            builds and owns everything                        
      |                                                                       
      v                                                                       
scenario/scenario.go, cmd/*    experiments that use all of the above             
```

</div>

---

## 4. How the components talk: a write and a read

Before looking at each piece, here is the journey of one write and one read, so you know where each piece fits. The key is `cart:alice` and its 3 replicas are `n2`, `n5`, `n4`.

### A write (`Put`) with W = 2

<div align="center">

```
Client                 n2 (coordinator)               n5                 n4
  |                          |                         |                  |
  | 1. Put("cart:alice",     |                         |                  |
  |    "eggs,milk", context) |                         |                  |
  |------------------------->|                         |                  |
  |                          | 2. make a new version,  |                  |
  |                          |    give it a unique     |                  |
  |                          |    name (dot n2:1),     |                  |
  |                          |    save it locally      |                  |
  |                          |    -- that's ack #1 --  |                  |
  |                          |                         |                  |
  |                          | 3. putReq ------------->|                  |
  |                          | 3. putReq -------------------------------->|
  |                          |                         | saves, replies   |
  |                          |<------------ putResp ---|                  |
  |                          |  ack #2 -> W=2 reached  |                  |
  |<-------- 4. "ok" --------|                         |                  |
  |                          |                         |     (slow...)    |
  |                          |<------------------------------ putResp ----|
  |                          |  5. arrives later; the client already left |
```

</div>

The important part is step 5: **n4 gets the write a bit later**. If someone reads from n4 in that gap with R = 1, they see the old list. That is exactly where stale reads come from.

### A read (`Get`) with R = 2

<div align="center">

```
Client                 n5 (coordinator)               n2                 n4           
  | 1. Get("cart:alice")     |                         |                  |           
  |------------------------->| 2. ask everyone (itself |                  |           
  |                          |    included) -- getReq >|                  |           
  |                          | ---------------- getReq ------------------>|           
  |                          | own copy  = reply #1    |                  |           
  |                          |<---- {eggs,milk} -------|  reply #2        |           
  |                          | 3. merge the 2 replies  |                  |           
  |<-- 4. versions+context --|                         |                  |           
  |                          |<------------------------------ {milk} -----| late reply
  |                          | 5. READ REPAIR: n4 is behind,              |           
  |                          |    send it {eggs,milk} -- putReq --------->|           
```

</div>

The client gets back **every version** that the R replies contained, plus a **context** (a summary of their history). The next `Put` must carry that context so the store knows what the writer had seen.

### Why R + W > N means "no stale reads"

<div align="center">

```
3 replicas:          [ n2 ]  [ n5 ]  [ n4 ]                             
                                                                        
write reached W=2:   [ ok ]  [ ok ]  [    ]                             
read asked R=2:              [ ?? ]  [ ?? ]                             
                                                                        
2 + 2 = 4 > 3, so the two groups MUST share at least one node (n5 here).
That shared node has the new value, so the read sees it.                
                                                                        
With R=1, W=1 (1 + 1 = 2, not > 3) the read can land only on n4,        
which hasn't heard yet  ->  stale read.                                 
```

</div>

In our measurements (no failures), R = W = 1 gave about **26% stale reads** (it depends on the network speed and workload, see [ABLATION.md](ABLATION.md)), and every setting with R + W > 3 gave **0%** (see `REPORT.md`).

---

## 5. Component 1: vector clocks (`vclock/`)

### The story

Each friend keeps a tally on the notebook: "n2 has made 1 change, n5 has made 1 change". Compare two tallies:

* If one tally is **at least as big in every column**, it has seen everything the other saw, so it is **newer**.
* If each tally is bigger in **some** column, each saw something the other didn't. They happened **at the same time** (concurrent). Neither may replace the other.

<div align="center">

```
A = [n2:1  n5:1]          B = [n4:1  n5:1]                            
                                                                      
       n2   n4   n5                                                   
  A:    1    0    1                                                   
  B:    0    1    1                                                   
        ^    ^                                                        
        A>B  B>A    -> each wins somewhere -> CONCURRENT              
                                                                      
C = [n2:1 n4:2 n5:1]                                                  
  C vs A:  1>=1, 2>=0, 1>=1   -> C is AFTER A (C saw everything A saw)
```

</div>

### The code

`vclock.VC` is just `map[string]uint64` (node id → counter). The package has five small, pure functions; none of them changes its inputs:

| Function | What it does |
|---|---|
| `New()` | An empty clock (a key never written) |
| `Copy()` | A separate copy, so changing one doesn't change the other |
| `Increment(node, floor)` | Copy and set `node`'s counter to `max(current, floor) + 1`. The `floor` makes sure a node never reuses a number (see section 10) |
| `Merge(clocks...)` | Column-by-column maximum: "a clock that has seen everything any of them saw". Used to build the read context |
| `Compare(a, b)` | Returns `Equal`, `Before`, `After` or `Concurrent` using the rule in the story |
| `Descends(a, b)` | True if `a` is `Equal` or `After` `b` |

`String()` prints clocks sorted by node id, like `[n2:1 n5:1]`, so logs always look the same.

---

## 6. Component 2: versions and dots (`store/version.go`)

### Why plain vector clocks are not enough

The Dynamo paper's recipe is: "the coordinator bumps its own column". Picture two clients who both read `[n1:1]` and both write **through the same coordinator** n1:

<div align="center">

```
client X writes "eggs"  via n1  -> clock [n1:2]                     
client Y writes "bread" via n1  -> clock [n1:3]                     
                                                                    
Compare: [n1:3] is AFTER [n1:2]  -> "bread" silently replaces "eggs"
```

</div>

But X and Y never saw each other's write! They were concurrent, and "eggs" was lost. In our benchmark this plain scheme lost **585 of 3055** acknowledged items. That is why the code uses a sharper idea.

### Dotted version vectors

Each version stores two things separately:

* **Dot** = the write's own unique name: `(node, counter)`, like `n1:3`. "The 3rd write that n1 coordinated for this key."
* **Ctx** (context) = the vector clock of what the writer had **read** before writing.

The rule becomes: **version A replaces version B only if B's dot is inside A's context**, meaning A's author actually saw B.

<div align="center">

```
X: dot n1:2, ctx [n1:1]       Y: dot n1:3, ctx [n1:1]           
                                                                
Does Y's ctx contain X's dot n1:2?   ctx[n1]=1, and 1 < 2  -> NO
Does X's ctx contain Y's dot n1:3?   1 < 3                 -> NO
-> concurrent -> keep BOTH as siblings -> nothing lost          
```

</div>

With this, the same benchmark lost **0** items.

### The code

```go
type Dot struct { Node string; Counter uint64 }

type Version struct {
    Value  string    // the stored value, e.g. "eggs,milk"
    Dot    Dot       // unique name of this write
    Ctx    vclock.VC // what the writer had seen
    TS     int64     // wall-clock time; only used by last-writer-wins
    Origin string    // coordinating node; LWW tie-breaker
}
```

* `v.Clock()` = `Ctx` with the dot added in. This is the "plain" clock of the version, used to build contexts and to sort.
* `v.Covers(w)` = "v is w, or v's writer had seen w": `v.Ctx[w.Dot.Node] >= w.Dot.Counter`.

Two settings live here too:

* `Policy`: `KeepSiblings` (default, Dynamo style) or `LastWriterWins` (for comparison).
* `ClockMode`: `DottedVV` (default) or `PerNodeVC` (the paper's plain scheme, kept only to measure the difference above).

---

## 7. Component 3: merge rules (`store/merge.go`)

### The story

A friend's notebook page holds a small pile of versions for a key (usually one, sometimes a few "siblings"). When a new version arrives, the friend follows one rule:

* If something in the pile already **covers** the newcomer, throw the newcomer away (it's old news).
* If the newcomer covers something in the pile, throw **that** away (it's been replaced).
* Otherwise, the newcomer and the pile entry are **siblings**; keep both.

### The code

Everything goes through one small struct, `rules{policy, clocks}`, so every node, coordinator and repair path applies the **same** ordering:

| Method | Meaning |
|---|---|
| `obsoletes(a, b)` | `a` replaces `b` (uses `Covers` under DottedVV, plain `Compare == After` under PerNodeVC) |
| `same(a, b)` | Is this the same write? (same dot) |
| `merge(set, incoming...)` | Fold new versions into the pile, using the three bullet rules above. Under LWW, keep only the one winner |
| `lwwBeats(a, b)` | LWW winner: causal order first, then higher timestamp, then higher node id on a tie |
| `covers(set, v)` | Does the pile already have `v` or something newer? Used by repair to send only what is missing |

<div align="center">

```
pile:      [ {eggs,milk} dot n2:1 ]                          
incoming:    {bread,milk} dot n4:1, ctx [n5:1]               
                                                             
n2:1 covered by incoming?  ctx[n2]=0 < 1  -> no              
n4:1 covered by pile?      pile ctx[n4]=0 -> no              
-> pile becomes [ {eggs,milk} , {bread,milk} ]   two siblings
```

</div>

Two properties make the whole system converge:

* **Order doesn't matter** (commutative): merging A then B gives the same pile as B then A.
* **Repeats don't matter** (idempotent): merging A twice is the same as once.

So it is always safe to resend data, which the repair helpers do freely. After merging, the pile is sorted so every replica holding the same versions lists them in the same order.

---

## 8. Component 4: the ring (`ring/ring.go`)

### The story

Draw a big clock face and cut it into **256 equal pieces**, like a pizza. Number the pieces 0 to 255 and deal them out to the 5 friends like cards: piece 0 to n1, piece 1 to n2, ... piece 5 to n1 again. To find where a key lives, you turn the key's name into a spot on the clock, see which piece it falls in, then **walk clockwise from that piece** and write down the first **3 different** names you meet. Those 3 friends keep that key.

<div align="center">

```
                    piece 0 (n1)                                         
              p255 (n1)   |   p1 (n2)                                    
          p254 (n5)       |       p2 (n3)                                
        ..                |                ..        256 equal pieces,   
       p4 (n5)  ---------- . ---------  p3 (n4)      dealt n1 n2 n3 n4 n5
        ..                                ..          n1 n2 n3 ...       
                                                                         
 key "cart:alice" hashes into piece 2 (owner n3)                         
 walk clockwise from piece 2: n3 (keep), n4 (keep), n5 (keep)            
                                                                         
 preference list (first 3 distinct nodes):  [n3 n4 n5]                   
 fallbacks (the rest, in walk order):       [n1 n2]                      
```

</div>

Why equal pieces instead of the earlier random flags? With random flags (64 per friend), some friends get more by luck: the most-loaded friend held about 10 points more of the data than the least-loaded one. Equal pieces dealt in turn give every friend almost exactly the same amount (within about 1 point), and when a sixth friend joins they take pieces from everybody, so only about 20% of the data moves. A piece is also the natural unit for the fingerprint trees in section 11. ([ABLATION.md](ABLATION.md) parts C3 to C5 show the measurements, including why 256 pieces and why not `hash mod n`.)

### The code

* `NewFixedRing(nodes, q)` (the default, `Config.Partitions = 256`): creates `q` tokens evenly spaced over the 64-bit hash space; token `i` belongs to `nodes[i % 5]`. The older `NewRing(nodes, vnodes)` with random MD5 flags is kept to compare against.
* `Partition(key)`: hashes the key with MD5 (first 8 bytes), finds the first token at or after that spot (wrapping), and returns that partition number and the hash. A key's partition **never changes**, even when nodes join.
* `Walk(key)` / `PartitionList(p, n)`: the order of distinct nodes walking clockwise from partition `p`. These walks are **precomputed once per partition** (`plists`) when the ring is built, so a lookup is one array read.
* `PreferenceList(key, n)`: the first `n` (= 3) entries of the key's walk. These are the **home** replicas. The rest of the walk are **fallbacks**.
* `WithNode` / `WithoutNode`: build the ring after a node joins or leaves (used by tests and by experiment A8), choosing evenly spaced pieces for the newcomer.

The ring never changes after it is built, so everyone can read it at the same time safely.

---

## 9. Component 5: the network (`network/network.go`, `store/messages.go`)

### The story

All messages go through a pretend **post office**. Each letter waits a random amount of time before delivery. We can also build a **wall** (a partition): letters between the two sides are silently thrown away. Nobody is told "your letter was lost"; the sender just waits, and gives up when its **timeout** runs out, exactly like a real network.

<div align="center">

```
       side A                 #               side B                            
+------+  +------+  +-------+ # +------+ +------+ +------+ +-------+            
|  n2  |  |  n1  |  |clientA| # |  n5  | |  n4  | |  n3  | |clientB|            
+------+  +------+  +-------+ # +------+ +------+ +------+ +-------+            
                              #                                                 
  n2 -- putReq ------------#--> X   dropped. n2 waits one timeout, then gives up
```

</div>

### The code

* **Latency model.** Each one-way hop takes `Base + random jitter + (sometimes) a slow-path delay`:
  * Node to node (`DefaultLatency`): 400 µs + jitter averaging 800 µs, and 3% of messages get an extra 5 ms (the "heavy tail" real networks have).
  * Hops touching a client (`DefaultClientLatency`): 100 µs + jitter averaging 100 µs (the client sits close to its coordinator).
  * The random numbers come from a **seeded** generator, so a run can be repeated.
* `Register(id, handler)`: adds an endpoint. Nodes register a handler function; clients register `nil` (they only send).
* `Send(ctx, from, to, req)`: delivers the request after a delay, calls the receiver's handler, waits another delay for the reply. The partition is checked **at delivery time** for each leg, so:
  * a request can be lost (nothing happened), or
  * the request can arrive and be processed, and only the **reply** is lost (it happened, but the sender thinks it failed).
  The sender can't tell these apart, just like in real life. If `from == to`, it is a direct local call with no delay.
* `Partition(groups...)`, `Heal()`: build and remove walls.
* `SetSlow(id, d)`: make one node slow.
* `Reachable(from, to)`: "can these two talk right now?" This stands in for a **failure detector** (Dynamo uses gossip for this). Coordinators use it to decide who looks alive.

`messages.go` lists the envelopes. Two levels:

| Message | Sent by → to | Meaning |
|---|---|---|
| `coordPutReq` / `coordPutResp` | client → coordinator | "Please write this value with this context and W" / "here's the new version and how many acks" |
| `coordGetReq` / `coordGetResp` | client → coordinator | "Please read with R" / "here are the merged versions" |
| `putReq` / `putResp` | node → node | "Merge these versions into your pile" (optional `Hint`: "you're holding this for someone else") / "done" |
| `getReq` / `getResp` | node → node | "What do you have for this key?" / "this pile" |
| `merkleReq` / `merkleResp` | node → node | Anti-entropy: "here are my fingerprints for these tree nodes" / "these ones differ" |
| `leafReq` / `leafResp` | node → node | Anti-entropy: "here are my key hashes under these leaves" / "here are my versions of the keys that differ, and the keys I want" |
| `pushReq` | node → node | Anti-entropy: "here are the versions you asked for" (answered by `putResp`) |
| `syncReq` / `syncResp` | node → node | The old full-state anti-entropy (`FullStateSync`), kept so Merkle can be compared against it |

`ErrUnavailable` ("quorum not reached") is the error when fewer than R or W replicas answered.

---

## 10. Component 6: the node and the coordinator

### 10.1 The node (`store/node.go`)

### The story

Each friend has a notebook with **two sections**:

* **My lists** (`data`): keys this friend is a home replica for.
* **Sticky notes for others** (`hints`): writes the friend is holding on behalf of a friend who was unreachable.

The friend also keeps a little private counter per key (`issued`) so they never give two different writes the same name.

<div align="center">

```
+------------------------- Node n1 --------------------------+
| data   : "cart:bob"   -> [ {apples}  dot n1:4 ]            |
|          "cart:carl"  -> [ {tea} , {coffee} ]  siblings    |
| hints  : for n4 -> "cart:alice" -> [ {eggs,milk} ]         |
| issued : "cart:bob" -> 4, "cart:alice" -> 1                |
|                                                            |
| handle(msg):  putReq / getReq     -> touch my notebook     |
|               coordPutReq/GetReq  -> act as coordinator    |
|               merkleReq etc.      -> anti-entropy          |
| background:   every 5 ms deliver sticky notes;             |
|               every 100 ms sync with a random peer         |
+------------------------------------------------------------+
```

</div>

### The code

* `NodeConfig`: N (= 3), `Policy`, `ReplicaTimeout` (how long a coordinator waits; 0 means "compute it from the network delays", about 17 ms on the default network), `AntiEntropy` (`MerkleSync` by default), `MerkleDepth` (4), `AntiEntropyInterval` (100 ms), `ReadRepair` (on), `HandoffInterval` (5 ms), `ClockSkew` (for LWW experiments), `Clocks`, and `ReplicateNewOnly` (an experiment switch, see below).
* `handle(from, req)` is the node's front door. The network calls it once per incoming letter, possibly many at the same time, so the notebook is protected by a lock (`mu`). **No node ever holds its lock while sending a message**, so a busy node still answers others.
  * `putReq` with a `Hint` for another node → merge into `hints[owner][key]`; otherwise merge into `data[key]`.
  * `getReq` → `localRead`: my data **plus** any sticky notes for that key (so a stand-in can answer a sloppy read).
  * `coordPutReq` / `coordGetReq` → become the coordinator (next section).
  * `merkleReq`, `leafReq`, `pushReq` (and the old `syncReq`) → anti-entropy (section 11).
* `start()` launches two background loops: `handoffHints()` every `HandoffInterval` (5 ms), and, if `AntiEntropyInterval` is positive (100 ms by default), a loop that picks a random reachable peer and runs `syncWith(peer)`.
* `Snapshot`, `HintCount`, `Coordinated` are read-only peeks used by tests and benchmarks.

### 10.2 The coordinator (`store/coordinator.go`)

Any node becomes "the coordinator" for a request just by receiving it from a client. It does three jobs.

#### Job 1: decide who to contact (`targets`)

<div align="center">

```
Walk("cart:alice") = [ n2  n5  n4 | n1  n3 ]                                 
                       home (N=3)   fallbacks                                
                                                                             
STRICT quorum:  always [n2, n5, n4]. If n4 is cut off, that reply just fails.
                                                                             
SLOPPY quorum, n4 unreachable:                                               
                [n2, n5, n1 (hint: "for n4")]                                
                 n1 stands in for n4 and holds the write as a sticky note    
```

</div>

Each fallback is used at most once, in ring order. If there's no live fallback left, the home node stays in the list and its reply simply fails. The list always has exactly N entries.

#### Job 2: coordinate a write (`coordPut`)

1. **Pick a fresh, never-used counter** for the new dot. It must be higher than anything this node has issued for the key, anything in the client's context, and anything already stored. Reusing a number would make two different writes look identical, and one would vanish.
2. **Build the version**: value, dot, context from the client, timestamp (wall clock + skew).
3. **Save it locally** (in `data`, or as a hint if this node isn't a home replica). This counts as the first ack.
4. **Send the whole local pile** (not just the new version) to the other targets. Why the whole pile? It guarantees that any replica holding write `n2:5` also holds `n2:4` or something newer. Without it, a replica could hand out a context that claims a write it doesn't actually have. The `ReplicateNewOnly` switch turns this off so the benchmark can show the damage.
5. **Count acks** until W succeed or everyone has answered or timed out. Answer the client.
6. Replicas that haven't answered yet **keep receiving the write in the background** until the timeout. That lag is the stale-read window.

A failed write (fewer than W acks) is **not undone**. The coordinator already saved it, so it can show up later as a sibling. The demo shows this happening.

#### Job 3: coordinate a read (`coordGet`)

1. Send `getReq` to **all N targets** at once.
2. As soon as **R successful replies** are in, merge them into one pile and answer the client. Don't wait for slow replicas.
3. In the background, wait for the remaining replies, then do **read repair** (section 11).

---

## 11. Component 7: the three repair helpers

Copies drift apart because of slow replicas, partitions and sloppy writes. Three helpers bring them back together. Because `merge` doesn't care about order or repeats, all three can send data without fear of breaking anything.

<div align="center">

```
+----------------+----------------------------+-----------------------------+
| Helper         | When it runs               | What it fixes               |
+----------------+----------------------------+-----------------------------+
| Read repair    | after every read           | replicas that answered the  |
|                | (coordinator.go)           | read with old data          |
+----------------+----------------------------+-----------------------------+
| Hinted handoff | every 5 ms on each node    | writes a stand-in is        |
|                | (repair.go)                | holding for a missing owner |
+----------------+----------------------------+-----------------------------+
| Anti-entropy   | every 100 ms on each node, | anything else: keys nobody  |
| (Merkle trees) | or Cluster.AntiEntropy()   | has read recently           |
|                | (antientropy.go)           |                             |
+----------------+----------------------------+-----------------------------+
```

</div>

### Read repair

After a read, the coordinator has every reply. For each replica, it computes what that replica is **missing** (versions in the merged pile that the replica's answer doesn't cover) and sends only those. A replica that's already up to date gets nothing.

### Hinted handoff

<div align="center">

```
during the partition                    after healing                                  
+----+  sticky note:                    +----+  putReq {eggs,milk}   +----+            
| n1 |  "cart:alice for n4"   ------>   | n1 | --------------------> | n4 |            
+----+                                  +----+ <---- ack ----------- +----+            
                                        n1 removes the note (only the versions it sent)
```

</div>

Every 5 ms, each node looks at its sticky notes, picks the owners that are reachable again, sends them their writes, and removes each note **as soon as that owner acknowledged it**. If a newer note for the same key arrived meanwhile, that one stays for the next round. Each send has its own timeout; the first version of this code shared one stopwatch across all of an owner's notes, so a big stack never finished (see section 9 of [ABLATION.md](ABLATION.md)).

### Anti-entropy: comparing fingerprints instead of everything

**The story.** Two friends each have 20,000 lines in their notebooks. They could read every line to each other, but almost every line is the same. Instead each keeps a **tree of fingerprints** per piece of the clock: the bottom of the tree holds a fingerprint of a small bundle of keys, each box above holds a fingerprint of the two boxes below, and the top box is a fingerprint of the whole piece. They compare tops; if they match, the piece is identical and they stop. If not, they compare the two halves, then the quarters, narrowing down to the few bundles that differ, and only then exchange those keys.

<div align="center">

```
          [ top ]                 tops equal?   -> identical, nothing to send
         /       \                                                           
     [ L ]       [ R ]            L equal, R differs -> look only inside R   
     /   \       /   \                                                       
   [a]   [b]   [c]   [d]          only c differs -> send just the keys in c  
```

</div>

Because **two replicas of a piece keep identical trees** (every write updates the one tree of the key's piece), any two nodes that share a piece can compare directly. When two nodes agree on everything, the whole exchange is just the top fingerprints. [MESSAGES.md](MESSAGES.md) shows every message of an exchange with a worked example.

**The numbers** (20,000 keys, one round over all node pairs): with 1% of keys differing, Merkle sends 832 keys (151 KB) where the old way sends 120,000 keys (7 MB). The price is more round trips (70 vs 10 per round).

**When it runs.** Every 100 ms each node picks a random reachable peer and runs one exchange. On the first version of this project nothing started anti-entropy by itself; it only ran when a test called `Cluster.AntiEntropy()`.

---

## 12. Component 8: the client, the resolver and the cluster

### 12.1 The client (`store/client.go`)

This is what an application uses. A `Client` has `R`, `W`, `Sloppy` and `Timeout`, so two clients can use different settings on the same data.

* **Choosing a coordinator**: pick a **random** home replica the client can reach. Random spreads load, but it also means a read may land on a replica the last write hasn't reached yet. (`FirstCoordinator` always picks the first one instead; the architecture benchmark compares the two.) If no home replica is reachable and `Sloppy` is on, use the first reachable node further round the ring.
* `Get(key)` → `GetResult{Versions, Context}`. `Context` is the `Merge` of every sibling's clock: "everything you just saw".
* `Put(key, value, ctx)`: pass the `Context` from your last `Get` (or `nil` for a first, blind write). The new version replaces exactly the versions that context covers.
* `GetResolved(key, resolve)`: read; if there are siblings, merge their values with `resolve` and write the result back with the merged context. That single write replaces all siblings at once, so they collapse into one.

<div align="center">

```
Get  --> siblings: {eggs,milk} [n2:1 n5:1]   {bread,milk} [n4:1 n5:1]
         context:  [n2:1 n4:1 n5:1]                                  
         |                                                           
resolve  v  UnionResolver -> "bread,eggs,milk"                       
         |                                                           
Put("bread,eggs,milk", context [n2:1 n4:1 n5:1])                     
         |   new version's context contains both dots n2:1 and n4:1  
         v                                                           
every replica: [ {bread,eggs,milk} ]   one version again             
```

</div>

### 12.2 The resolver (`store/resolver.go`)

`UnionResolver` treats a value as a comma-separated set, like a shopping cart, and returns the sorted union of all siblings. Sorting means the same siblings always give the same answer. Its promise: **no item that any client was told "saved" is ever lost**. (It can't delete an item that one side removed; that's the known trade-off of a union cart, the same as in the Dynamo paper.)

### 12.3 The cluster (`store/cluster.go`)

`DefaultConfig()` describes the standard setup:

| Setting | Value |
|---|---|
| Nodes | 5 (`n1`..`n5`), 256 fixed partitions |
| Replicas per key (N) | 3 |
| Policy | keep siblings |
| Replica timeout | computed from the network delays (about 17 ms by default) |
| Read repair | on |
| Hinted handoff | every 5 ms |
| Anti-entropy | Merkle trees (depth 4), every 100 ms |
| Network | default latencies, seed 1 |

`NewCluster(cfg)` builds the network, the ring and the nodes, and starts each node's background loop. Then:

* `NewClient(id, R, W)`: a new client (R and W must be between 1 and N). Its timeout is 3 × the replica timeout.
* `AntiEntropy()`: one sync round for every reachable pair (the background loop does this by itself; this button forces a round).
* `SyncStats()`: traffic counters for anti-entropy (round trips, hashes, keys, bytes).
* `SetSkew(id, d)`: shift one node's clock (to show LWW going wrong).
* `WaitHintsDrained(d)`: wait until every sticky note has been delivered.
* `Close()`: stop the background loops.

---

## 13. Putting it all together: the partition story

`scenario/scenario.go` (`RunPartitionScenario`) plays out the shopping-list story from section 1 for real. This is the actual output of `go run ./cmd/demo`, case 1 (siblings, R = 1, W = 1, strict quorum), with each step explained.

**Step 1. Everyone agrees.** A first write `{milk}` reaches all three replicas. Its dot is `n5:1`.

<div align="center">

```
n2: {milk}      n5: {milk}      n4: {milk}          all hold dot n5:1
```

</div>

Both clients read it, so both have context `[n5:1]`.

**Step 2. The wall goes up.**

<div align="center">

```
side A: n2, n1, clientA     #     side B: n5, n4, n3, clientB
```

</div>

**Step 3. Both sides write.**

* Client A adds eggs through n2. New version: value `{eggs,milk}`, dot `n2:1`, context `[n5:1]`. With W = 1, n2's own save is enough: **ok**.
* Client B adds bread through n4. New version: value `{bread,milk}`, dot `n4:1`, context `[n5:1]`. **ok**.

<div align="center">

```
n2: {eggs,milk}             #     n5: {bread,milk}   n4: {bread,milk}
```

</div>

**Step 4. The wall comes down; repair runs.** Hinted handoff drains, then one anti-entropy round. Now each replica receives the other side's version and merges it. Neither version's context contains the other's dot, so they're **siblings**:

<div align="center">

```
every replica: [ {eggs,milk} clock [n2:1 n5:1] ,  {bread,milk} clock [n4:1 n5:1] ]
```

</div>

**Step 5. Read with R = 3 and reconcile.** The client gets 2 siblings, merges them with `UnionResolver` into `{bread,eggs,milk}`, and writes that back with the merged context `[n2:1 n4:1 n5:1]`. The coordinator was n4, so the new dot is `n4:2`.

**Step 6. Check.**

<div align="center">

```
replica n2 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]                      
replica n5 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]                      
replica n4 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]                      
converged, and every acknowledged item (milk, eggs, bread) is still there
```

</div>

### The same story with other settings

| Setting | What happens | Why |
|---|---|---|
| Siblings, R = W = 2, strict | Side A's write **fails** (only 1 of 2 acks: n2 can't reach n5 or n4) but n2 already saved it, so after healing it still shows up as a sibling. Final value is still `{bread,eggs,milk}` | A failed quorum is not a rollback |
| Siblings, R = W = 2, sloppy | Side A's write **succeeds** using n1 as a stand-in with a sticky note; hinted handoff delivers it after healing | Sloppy quorum trades consistency for availability |
| Last-writer-wins | Only one version survives: `{bread,milk}`. **Eggs is lost** even though client A was told "ok" | LWW silently throws one away |
| LWW, side A's clock +50 ms | Now `{eggs,milk}` wins and **bread is lost**, even though bread was written later | Clocks on different machines disagree, so "latest timestamp" isn't really "latest" |

---

## 14. Under the hood: how each component is implemented

Sections 5 to 12 explained **what** each component does. This section shows **how** it is built in Go: the data it keeps, the steps of its main function, how it stays safe when many goroutines use it at once, and the real code for the important parts. The order is the same as before, so each part only uses parts already covered. For the exact contents of every message, see [MESSAGES.md](MESSAGES.md).

### 14.1 Vector clocks (`vclock/vclock.go`, 195 lines)

**Data.** A clock is a Go map. A missing entry means zero.

```go
type VC map[string]uint64          // node id -> counter
```

**Safety.** No function changes its input. `Increment` and `Merge` build a new map, so a clock stored inside a version can be shared freely as long as nobody writes to it.

**Compare in one pass.** It walks both maps once and remembers two flags: "a is behind somewhere" and "b is behind somewhere". The four combinations of the two flags are the four answers:

```go
aLess, bLess := false, false
for k, an := range a {
    bn := b[k]
    if an < bn { aLess = true } else if an > bn { bLess = true }
}
for k, bn := range b {             // entries only b has are zero in a
    if _, ok := a[k]; !ok && bn > 0 { aLess = true }
}
switch {
case !aLess && !bLess: return Equal
case aLess && !bLess:  return Before
case !aLess && bLess:  return After
default:               return Concurrent
}
```

<div align="center">

```
                  bLess = false        bLess = true   
               +------------------+------------------+
aLess = false  |      Equal       |  After (a newer) |
               +------------------+------------------+
aLess = true   | Before (b newer) |    Concurrent    |
               +------------------+------------------+
```

</div>

**Stable printing.** `String()` sorts the node ids first, because Go visits map entries in random order and logs would otherwise change from run to run.

### 14.2 Versions and dots (`store/version.go`, 106 lines)

**Data.** Two small structs (shown in section 6) and two enums, `Policy` and `ClockMode`, each a plain `int` with named constants.

**The two key methods are one line each:**

```go
// Clock: the context with this write's own dot added.
func (v Version) Clock() vclock.VC {
    c := v.Ctx.Copy()
    if c[v.Dot.Node] < v.Dot.Counter { c[v.Dot.Node] = v.Dot.Counter }
    return c
}

// Covers: v is w, or v's writer had seen w's dot.
func (v Version) Covers(w Version) bool {
    return v.Dot == w.Dot || v.Ctx[w.Dot.Node] >= w.Dot.Counter
}
```

`Covers` is the whole idea of dotted version vectors: to decide if A replaces B, look up **one** number in A's context and compare it with B's counter.

### 14.3 Merge rules (`store/merge.go`, 182 lines)

**Data.** A tiny struct that bundles the two settings, so every caller applies the same rules:

```go
type rules struct {
    policy Policy
    clocks ClockMode
}
```

**The merge loop.** For each incoming version, compare it with every version already in the pile. Build the result in a fresh slice so the loop never overwrites what it is still reading.

```go
for _, v := range incoming {
    if r.policy == LastWriterWins {                // keep one winner only
        if len(set) == 0 || r.lwwBeats(v, set[0]) { set = []Version{v} }
        continue
    }
    dominated := false
    kept := set[:0:0]                              // empty slice, new backing array
    for _, e := range set {
        switch {
        case r.same(e, v) || r.obsoletes(e, v):   // v is old news
            dominated = true
            kept = append(kept, e)
        case r.obsoletes(v, e):                    // e is replaced: drop it
        default:                                   // concurrent: keep both
            kept = append(kept, e)
        }
    }
    if !dominated { kept = append(kept, v) }
    set = kept
}
sortVersions(set)                                  // same order on every replica
```

<div align="center">

```
pile before        incoming           decision                 pile after
-----------        --------           --------                 ----------
[A]                A again            same            -> drop  [A]       
[A]                B (covers A)       B obsoletes A   -> swap  [B]       
[B]                A (B covers A)     B obsoletes A   -> drop  [B]       
[A]                C (concurrent)     neither covers  -> keep  [A, C]    
```

</div>

**Cost.** Each incoming version is compared with each pile entry, so merging is `pile size x incoming`. Piles are small (one version normally, a handful of siblings at worst), so this is fast.

**LWW tie-break.** `lwwBeats` checks, in order: same write (no), causally older (no), causally newer (yes), higher timestamp, and finally higher node id. The last step only has to be the same on every replica, so they all pick the same winner.

### 14.4 The ring (`ring/ring.go`, 258 lines)

**Data.**

```go
type Ring struct {
    tokens []uint64            // all tokens, sorted
    owner  map[uint64]string   // token -> physical node
    nodes  []string            // physical nodes
    hash   func(string) uint64 // MD5, first 8 bytes
    plists [][]string          // plists[i]: Walk order for keys in partition i
}
```

**Building it.** `NewFixedRing(nodes, q)` puts token `i` at `i * (2^64 / q)` and gives it to `nodes[i % len(nodes)]`, then `index()` sorts the tokens and precomputes `plists`: for each partition, the distinct nodes in walk order.

**Finding a key's partition.** Binary-search for the first token at or after the key's hash (wrapping with `%`):

```go
h := hash64(key)
p := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= h }) % len(r.tokens)
return p, h          // Partition(key)
```

<div align="center">

```
tokens (partition starts):  [p0] [p1] [p2] [p3] [p4] [p5] ...      
                                  ^                                
                  key hash lands here, so key is in partition p2   
                  PartitionList(p2, 3) = plists[2][:3] = [n3 n4 n5]
```

</div>

**Cost.** Finding the partition is a binary search, about 8 steps for 256 tokens; the walk itself is a precomputed array read. **Joins.** `WithNode(id)` hands the newcomer evenly spaced partitions taken from the existing owners, so about 20% of copies move. **Safety.** The ring is never changed after it is built (a join builds a new one), so it needs no lock.

### 14.5 The network (`network/network.go`, 287 lines)

**Data.**

```go
type Network struct {
    mu       sync.RWMutex
    handlers map[string]Handler       // endpoint -> its handle function (nil for clients)
    group    map[string]int           // endpoint -> which side of the wall
    latency  LatencyModel             // node-to-node delays
    clientLt LatencyModel             // delays for hops touching a client
    isClient map[string]bool
    extra    map[string]time.Duration // per-node slowdown
    rngMu    sync.Mutex               // the random generator has its own lock
    rng      *rand.Rand               // seeded, so runs repeat
}
```

**Partitions are just numbers.** Every endpoint has a group number. Two endpoints can talk if their numbers are equal. `Partition(groups...)` resets everyone to group 0 and then gives the listed groups numbers 1, 2 and so on.

**Sampling a delay.**

```go
d := l.Base + time.Duration(n.rng.ExpFloat64()*float64(l.JitterMean))
if l.SlowProb > 0 && n.rng.Float64() < l.SlowProb { d += l.SlowExtra }
```

An exponential jitter gives many quick messages and a few slow ones, like a real network.

**Sending.** Each `Send` starts one goroutine that sleeps, checks the wall, calls the receiver's handler, sleeps again, checks the wall again, and puts the reply in a channel with room for one value. The caller waits on that channel **or** on its deadline, whichever comes first:

```go
out := make(chan any, 1)          // room for one reply, so the goroutine never blocks
go func() {
    time.Sleep(n.delay(lat) + slow)
    if !sameGroup || h == nil { return }      // request dropped
    resp := h(from, req)
    time.Sleep(n.delay(lat) + slow)
    if !n.Reachable(to, from) { return }      // reply dropped
    out <- resp
}()
select {
case r := <-out:   return r, nil
case <-ctx.Done(): return nil, ErrTimeout
}
```

**Safety.** A read-write lock guards the maps (many readers, few writers), and the random generator has its own lock because Go's `rand.Rand` is not safe to share.

### 14.6 The node (`store/node.go`, 283 lines)

**Data.**

```go
type Node struct {
    ID   string
    cfg  NodeConfig
    net  *Network
    ring *Ring

    mu     sync.Mutex
    data   map[string][]Version            // key -> sibling pile (keys I own)
    hints  map[string]map[string][]Version // owner -> key -> pile (sticky notes)
    issued map[string]uint64               // key -> highest counter I've issued
    trees  map[int]*merkle                 // partition -> fingerprint tree of its keys

    stop chan struct{}  // closed to stop the background loop
    wg   sync.WaitGroup // lets Close wait for the loop to finish

    coordinated atomic.Int64 // how many client requests I coordinated
}
```

**The background loops.** `start()` launches one goroutine per loop, each with a ticker, until `stop` is closed. Every 5 ms one calls `handoffHints()`; every 100 ms the other picks a random reachable peer and calls `syncWith(peer)`:

```go
t := time.NewTicker(nd.cfg.HandoffInterval)
for {
    select {
    case <-nd.stop: return
    case <-t.C:     f()   // handoffHints, or a sync with a random peer
    }
}
```

**The locking rule.** One mutex guards `data`, `hints`, `issued` and the Merkle `trees`. It is only held for quick in-memory work and **never** while sending a message. If a node held its lock while waiting for another node, and that node was waiting for it, both would freeze forever. Not holding it while sending rules that out.

**Copies at the border.** Versions coming in from a message are cloned (`cloneVersions`) before being stored, and versions going out are cloned before the lock is released. That way no two goroutines ever share the same map.

### 14.7 The coordinator (`store/coordinator.go`, 283 lines)

**Choosing targets (`targets`).** Take the ring walk. The first N are home replicas. For a strict quorum, use them as they are. For a sloppy quorum, replace each unreachable home replica with the next reachable stand-in, each stand-in used once:

```go
fallback := walk[n:]
fi := 0
for _, p := range pref {
    if nd.net.Reachable(nd.ID, p) { out = append(out, target{node: p}); continue }
    for fi < len(fallback) && !nd.net.Reachable(nd.ID, fallback[fi]) { fi++ }
    if fi < len(fallback) {
        out = append(out, target{node: fallback[fi], hint: p}); fi++
    } else {
        out = append(out, target{node: p})   // no stand-in left: will just fail
    }
}
```

**Writing (`coordPut`), step by step:**

<div align="center">

```
+-------------------- hold nd.mu ----------------------+              
| 1. floor = max(issued[key], ctx[me], every stored    |              
|             version's clock[me])                     |              
| 2. dot   = (me, floor+1);  issued[key] = floor+1     |              
| 3. build Version{value, dot, ctx, now+skew, me}      |              
| 4. merge into data[key]  (or hints, if not a home)   |              
| 5. snapshot = clone of my whole pile for key         |              
+------------------- release nd.mu ---------------------+             
6. one goroutine per other target:  Send(putReq{snapshot})            
7. read the acks channel until W ok, or all answered                  
8. reply coordPutResp; stragglers keep going until the replica timeout
```

</div>

Steps 1 and 2 are why a counter is never reused. Step 5 is the "send the whole pile" decision from section 10.

**Reading (`coordGet`).** Same fan-out shape, with a channel of replies instead of acks. After R good replies it answers. The **same** channel is then drained by a background goroutine, which finishes the merge and sends read repair:

```go
go func() {
    for ; done < len(ts); done++ {           // collect the late replies
        rp := <-replies
        if rp.err == nil { got = append(got, rp); merged = nd.rules().merge(merged, rp.vs...) }
    }
    if !nd.cfg.ReadRepair { return }
    for _, rp := range got {                 // repair each replica with only what it lacks
        var missing []Version
        for _, v := range merged {
            if !nd.rules().covers(rp.vs, v) { missing = append(missing, v) }
        }
        if len(missing) > 0 {
            go nd.net.Send(rctx, nd.ID, rp.t.node, putReq{Key: r.Key, V: missing, Hint: rp.t.hint})
        }
    }
    <-rctx.Done()
}()
```

The answer to the client is cloned before this goroutine starts, because the goroutine keeps changing `merged`.

### 14.8 Repair (`store/repair.go` 74 lines, `store/antientropy.go` 419 lines, `merkle/merkle.go` 157 lines)

**Hinted handoff (`handoffHints`).**

<div align="center">

```
1. lock:   copy the sticky notes for every owner that looks reachable  
2. unlock                                                              
3. for each owner, for each key: send one putReq (no hint) with ITS OWN
   ReplicaTimeout; stop this owner's round at the first failure        
4. lock: remove only the versions that were delivered                  
         (a newer note that arrived meanwhile stays)                   
   unlock                                                              
```

</div>

Resending a note that was already delivered is harmless, because merging the same version twice changes nothing.

**The tree (`merkle/merkle.go`).**

```go
type Tree struct {           // package merkle
    depth int                 // levels below the root; leaves = 1<<depth
    h     []uint64            // node hashes in one flat array, length 2*leaves
    keys  []map[string]uint64 // per leaf: key -> key hash
}
```

Node 1 is the top; node `i` has children `2i` and `2i+1`; leaves are `leaves .. 2*leaves-1`. `Set(key, ringHash, valueHash)` picks the leaf with `leafOf` (a mixed version of the ring hash), XORs the old key hash out of the leaf hash and the new one in, then `rehash` walks up recomputing each parent as `mix(left, right)`. `Node.put` is the only way data is written, and it calls `Set` on the tree of the key's partition, so the trees always match the data (a test rebuilds trees from scratch and compares).

**Three hashes, three jobs.** The code uses a different hash where a different job needs it:

| Job | Where | Hash | Why |
|---|---|---|---|
| Where does a key live on the ring? | `hash64` in `ring/ring.go` | MD5 (first 8 bytes) | The paper uses MD5, and it spreads keys evenly (A1: CV 0.056 vs 0.401 for FNV-1a). Computed once per operation |
| What is the fingerprint of one key's data? | `hashVersions` in `store/antientropy.go` | FNV-1a (`hash/fnv`) over the key and every version's value, dot, clock and timestamp | Runs on every write; needs to be cheap and deterministic, not attack-proof |
| How do child fingerprints combine into a parent? | `mix` / `splitmix` in `merkle/merkle.go` | A few shifts and multiplies on 64-bit numbers (the finalizer of the SplitMix64 random generator) | Works on numbers, not bytes, so it needs no allocation. Mixing before combining makes `mix(a,b)` differ from `mix(b,a)` and keeps `mix(x,x)` from collapsing to 0 |

None of these needs to resist an attacker: the worst a chance collision (about 1 in 2^64) can do is hide a difference until the next write to that leaf. Go's standard library has no Murmur3. `hash/maphash` is fast but its seed is random per process, which would break comparison between real machines.

**Anti-entropy (`merkleSync` / `handleMerkle` / `handleLeaf`).** The starter sends its fingerprints for the root of every partition it shares with the peer; the peer answers with the ones that differ; the starter then sends the children of those, level by level, until only leaves remain; it sends the key hashes under the differing leaves; the peer replies with its versions of the keys whose hashes differ and the keys it wants; finally the starter pushes what the peer asked for:

```go
// receiver: which of these tree nodes differ from mine?
for i, ref := range r.Refs {
    if nd.trees[ref.P].Hash(ref.I) != r.Hashes[i] { diff = append(diff, ref) }
}
```

Equal trees end the exchange after the first round trip. `FullStateSync` (`fullSync`, `handleSync`) still exists: send every shared key both ways. `SyncStats` counts round trips, hashes, keys and bytes of the exchanges a node started, which is how the benchmarks measure Merkle against full state.

`Cluster.AntiEntropy` runs the pairs one after another (not in parallel), which keeps the result easy to reason about in tests. The background loop does the same thing one random peer at a time.

### 14.9 The client and the resolver (`store/client.go` 207 lines, `store/resolver.go` 62 lines)

**Data.** The client holds only its settings plus pointers to the shared network and ring. It keeps nothing between requests, so one client can be used from many goroutines.

**Picking a coordinator.**

<div align="center">

```
live = home replicas I can reach                                                      
if FirstCoordinator and live is not empty -> live[0]                                  
if live is not empty                      -> random pick from live                    
if Sloppy                                 -> first reachable node on the ring walk    
otherwise                                 -> ErrUnavailable "no reachable coordinator"
```

</div>

**Building the context.** After a `Get`, merge the clock of every returned version:

```go
res := GetResult{Versions: r.Versions, Context: vclock.New()}
for _, v := range r.Versions {
    res.Context = vclock.Merge(res.Context, v.Clock())
}
```

**`UnionResolver`.** Split every sibling on commas, put the items in a set, sort, and join. Sorting makes the answer the same no matter what order the siblings came in.

### 14.10 The cluster (`store/cluster.go`, 275 lines)

**Building.** `NewCluster` names the nodes `n1`..`n5`, builds one ring, one network (seeded) and one node per name, registers each node's `handle` on the network, and starts each node's background loop.

**Waiting for hints.** `WaitHintsDrained` checks every millisecond whether the total `HintCount()` across nodes is zero, up to a deadline.

**Joining.** `AddNode` adds a node to a running cluster: it builds a new ring with `Ring.WithNode` (the newcomer takes about 20% of the copies, evenly spaced), creates and registers the node, then swaps the new ring into every node and client through an atomic pointer. Reads and writes keep going on whichever ring they already loaded. The newcomer starts empty and anti-entropy fills it (about two anti-entropy intervals; experiment X11). **Timeout.** `AutoTimeout` computes `ReplicaTimeout()` from the network delays.

**Shutting down.** `Close` closes every node's `stop` channel and waits on its `WaitGroup`, so no background loop is left running.

### 14.11 How it is tested

| Test file | What it checks |
|---|---|
| `vclock/vclock_test.go` | `Compare` gives the right order, and `Increment` respects its floor |
| `store/store_test.go` | Merge drops replaced versions; two concurrent writes through one coordinator stay siblings; a strict quorum reads its own write; a strict quorum refuses on the minority side |
| `store/concurrency_test.go` | Many clients writing the same key at once lose no acknowledged items |
| `store/antientropy_test.go` | From random divergent states, one Merkle round leaves every replica identical to what full-state sync produces (25 seeds); a second round sends no keys; trees equal trees rebuilt from the data; a real partition with concurrent clients heals with no acknowledged write lost; the one-shot walk and the periodic loop converge |
| `ring/ring_test.go`, `store/ring_test.go` | Fixed rings stay balanced; a key's partition does not change when a node joins; join and leave move about 20% of copies; Merkle also works on random vnodes |
| `store/membership_test.go` | A node can join a live cluster without losing or hiding an acknowledged write |
| `scenario/scenario_test.go` | Every partition scenario keeps its declared rule |
| `store/store_test.go` | (also) hinted handoff delivers 150 hints (`TestHandoffDeliversManyHints`) |

Run them with `go test -race ./...`. The `-race` flag makes Go watch for two goroutines touching the same memory without a lock, which is how the locking rules above are checked.

---

## 15. The programs in `cmd/` and what they prove

| Command | What it runs | Output file |
|---|---|---|
| `go run ./cmd/demo` | The partition story above, for 5 settings. Exits with an error if a declared rule is broken | `results/demo.txt` |
| `go run ./cmd/bench` | **E1** stale reads per (R, W); **E2** many clients writing the same keys concurrently (siblings, lost items, clock modes); **E3** availability and staleness during a partition, strict vs sloppy | `results/benchmark.md` |
| `go run ./cmd/archbench` | **A1**–**A10**: evidence for design choices (ring balance and vnodes, sending the whole pile, repair helpers, random vs first coordinator, timeout length, Merkle vs full state, fixed partitions, how many partitions, tree walks) | `results/architecture.md` |
| `go run ./cmd/bench -ablate` | **X1**–**X11**: noise, network speed, anti-entropy interval, node count, workload, handoff, read path, failures, sibling growth (X9), mixed R and W (X10), a node joining a live cluster (X11) | `results/ablations.md` |
| `go run ./cmd/plots` | Redraws the charts (SVG) for E1–E3 and A1–A5 | `results/*.svg` |
| `go test -race ./...` | Unit and scenario tests, with Go's race detector | |

How does the benchmark know a read was stale? `cmd/bench/oracle.go` is a **referee**. Every client tells the referee about each write that was acknowledged. Before a read starts, the client asks the referee "which writes were already confirmed?", and after the read it checks that the store returned all of them (or something newer). Missing one means **stale**.

Headline numbers (from `docs/reference/REPORT.md`):

* **Stale reads, no failures:** R = W = 1 → about 26% (8% on a fast LAN, 41% cross-region; see ABLATION.md); R = 1, W = 2 → 0.4%; any R + W > 3 → 0%.
* **Concurrent writers:** dotted version vectors lost 0 acknowledged items; plain per-node vector clocks lost 665 of 3278; LWW lost 4397 of 5196.
* **Partition:** strict R = W = 2 had 0% stale reads but the minority side could barely write; sloppy quorums at R = W ≤ 2 stayed about 100% available, but around 90% of reads were stale during the partition. No acknowledged write went missing after healing in any setting.

---

## 16. Glossary

| Word | Meaning |
|---|---|
| **Key-value store** | A big dictionary: `Put(key, value)`, `Get(key)` |
| **Replica** | One of the copies of a key (here 3 per key) |
| **Node** | One storage computer (`n1`..`n5`) |
| **Preference list** | The N nodes that are a key's home, found by walking the ring |
| **Consistent hashing / ring** | Placing nodes and keys on a circle so each key has a clear owner |
| **Partition** | One of the 256 equal pieces of the ring; each key belongs to exactly one |
| **Virtual node (vnode)** | One of a node's many random positions on the old ring (64 each); replaced by fixed partitions |
| **Merkle tree** | A tree of fingerprints (hashes) over a partition's keys; two replicas compare trees to find the keys that differ |
| **Leaf / depth** | The bottom of a Merkle tree, holding a bundle of keys; how many levels it has (4) |
| **Coordinator** | The node a client sends a request to; it talks to the other replicas |
| **Quorum, R, W** | How many replicas must answer a read (R) or confirm a write (W) |
| **N** | Copies per key (3) |
| **Stale read** | A read that misses a write that was already confirmed |
| **Partition** | A network split; messages between the sides are lost |
| **Strict quorum** | Only the home replicas count toward R or W |
| **Sloppy quorum** | Stand-in nodes may count when home replicas are unreachable |
| **Hint / hinted handoff** | A write held by a stand-in for its real owner, delivered later |
| **Read repair** | After a read, sending newer versions to replicas that answered with old ones |
| **Anti-entropy** | Background syncing between pairs of replicas; now by comparing Merkle trees every 100 ms |
| **Vector clock** | A per-node counter map that tells "before", "after" or "concurrent" |
| **Dot** | A write's unique name: `(node, counter)` |
| **Context** | The vector clock of what a client had read before writing |
| **Dotted version vector** | Dot + context; the scheme this store uses to tell concurrent writes apart |
| **Siblings** | Concurrent versions of one key, kept side by side |
| **Resolver / reconcile** | The client's rule for merging siblings (here: set union) |
| **Last-writer-wins (LWW)** | Keep only the version with the highest timestamp; loses concurrent writes |
| **Clock skew** | Different machines' clocks disagreeing, which breaks LWW |
| **Seed** | The starting number for the random generator, so runs can be repeated |
| **Idempotent / commutative** | Doing it twice = once / order doesn't matter. Why repair is safe |

---

For every message the components exchange, with full real examples, see [MESSAGES.md](MESSAGES.md). For the detailed design decisions and every experiment's evidence, see [ARCHITECTURE_DECISIONS.md](../reference/ARCHITECTURE_DECISIONS.md), [ABLATION.md](ABLATION.md) and [REPORT.md](../reference/REPORT.md).
