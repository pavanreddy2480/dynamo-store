# dynamo-store, explained from zero

This guide walks through the whole project in the order you need to understand it: first the problem, then the idea for solving it, then how the pieces talk to each other, and finally every piece on its own, starting with the smallest and building up. Every section uses a small everyday story first and then shows what the Go code actually does, with the file name so you can open it and follow along.

It describes the `main` branch, which runs the whole system as a **simulation inside one Go program**. The "network" is fake (messages are passed in memory with a random delay), which lets us create network failures on purpose and measure what happens.

> **Tip:** If a word is new, check the [glossary](#15-glossary) at the end.

---

## Contents

1. [The problem we are solving](#1-the-problem-we-are-solving)
2. [How we approach it: the big ideas](#2-how-we-approach-it-the-big-ideas)
3. [The whole picture in one drawing](#3-the-whole-picture-in-one-drawing)
4. [How the components talk: a write and a read](#4-how-the-components-talk-a-write-and-a-read)
5. [Component 1: vector clocks (`vclock/`)](#5-component-1-vector-clocks-vclock)
6. [Component 2: versions and dots (`store/version.go`)](#6-component-2-versions-and-dots-storeversiongo)
7. [Component 3: merge rules (`store/merge.go`)](#7-component-3-merge-rules-storemergego)
8. [Component 4: the ring (`store/ring.go`)](#8-component-4-the-ring-storeringgo)
9. [Component 5: the network (`store/network.go`, `store/messages.go`)](#9-component-5-the-network-storenetworkgo-storemessagesgo)
10. [Component 6: the node and the coordinator (`store/node.go`, `store/coordinator.go`)](#10-component-6-the-node-and-the-coordinator)
11. [Component 7: the three repair helpers (`store/repair.go`)](#11-component-7-the-three-repair-helpers)
12. [Component 8: the client, the resolver and the cluster](#12-component-8-the-client-the-resolver-and-the-cluster)
13. [Putting it all together: the partition story, step by step](#13-putting-it-all-together-the-partition-story)
14. [The programs in `cmd/` and what they prove](#14-the-programs-in-cmd-and-what-they-prove)
15. [Glossary](#15-glossary)

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
| Where does each key live? | **Consistent-hash ring** | Put the 5 computers on a circle; a key lives on the first 3 computers clockwise from its spot | `store/ring.go` |
| Who handles a request? | **Coordinator** | The client picks one of the key's 3 computers; that one talks to the other two on the client's behalf | `store/coordinator.go` |
| How many answers are enough? | **Quorums R and W** | A write waits for W "saved!" replies, a read waits for R answers. If R + W > 3, reads can't be stale | `store/coordinator.go` |
| What if a replica is unreachable? | **Strict vs sloppy quorum + hinted handoff** | Strict: refuse. Sloppy: give the write to a stand-in computer with a sticky note "this belongs to n4", and the stand-in delivers it later | `store/coordinator.go`, `store/repair.go` |
| Did two changes happen at the same time? | **Dotted version vectors** (a sharper kind of vector clock) | Each write gets a unique name and remembers which writes its author had seen | `vclock/`, `store/version.go` |
| What do we do with two "at the same time" changes? | **Siblings + client merge** | Keep both, return both, the client merges them (here: union of the shopping items) and writes the result back | `store/merge.go`, `store/client.go`, `store/resolver.go` |
| How do copies catch up after a failure? | **Read repair, hinted handoff, anti-entropy** | Three cleaners that copy missing versions to replicas that lack them | `store/coordinator.go`, `store/repair.go` |
| How do we test failures safely? | **Simulated network** | An in-memory network with random (but repeatable) delays, which we can cut on purpose | `store/network.go` |

For comparison, the code also has a simpler, worse policy, **last-writer-wins (LWW)**: keep only the version with the latest timestamp. It is there to show, with numbers, what goes wrong when you secretly pick a winner.

---

## 3. The whole picture in one drawing

```
                         ┌──────────────────────────────────────────────────────────┐
                         │                     Cluster  (cluster.go)                │
                         │                                                          │
   ┌──────────┐  Get/Put │   ┌─────────────────── Network (network.go) ───────────┐ │
   │ Client A │──────────┼──▶│  in-memory "post office": every message waits a    │ │
   │ R=2, W=2 │◀─────────┼───│  random delay, and is DROPPED if sender and        │ │
   └──────────┘          │   │  receiver are on different sides of a partition    │ │
                         │   └───▲───────▲────────▲────────▲────────▲─────────────┘ │
   ┌──────────┐          │       │       │        │        │        │               │
   │ Client B │──────────┼──▶ ┌──┴─┐  ┌──┴─┐   ┌──┴─┐   ┌──┴─┐   ┌──┴─┐             │
   │ R=1, W=1 │◀─────────┼─── │ n1 │  │ n2 │   │ n3 │   │ n4 │   │ n5 │  Nodes      │
   └──────────┘          │    └────┘  └────┘   └────┘   └────┘   └────┘  (node.go)  │
                         │       every node: stores data, stores hints,             │
                         │       can act as coordinator, runs repair                │
                         │                                                          │
                         │   Ring (ring.go): tells everyone which 3 nodes own a key │
                         └──────────────────────────────────────────────────────────┘
```

* **Clients** are the callers. Each client has its own R and W.
* **Nodes** `n1`..`n5` are the storage computers. They are all equal: any of them can store data and any of them can be the coordinator for a request.
* The **Network** carries every message between clients and nodes and between nodes.
* The **Ring** is shared knowledge: given a key, it answers "which 3 nodes keep it?".
* The **Cluster** builds all of this and offers a few whole-system buttons (run anti-entropy, add clock skew, wait for hints).

### How the code depends on itself

Read the components in this order; each one only uses the ones above it:

```
  vclock/vclock.go            counting "what have I seen?"
        │
        ▼
  store/version.go            one stored value + its history (Dot, Version)
        │
        ▼
  store/merge.go              rules: which version replaces which
        │
        ▼
  store/ring.go               which nodes own which key
  store/network.go            fake network with delays and partitions
  store/messages.go           the envelopes sent over the network
        │
        ▼
  store/node.go               one storage computer
  store/coordinator.go        a node handling a client's request
  store/repair.go             hinted handoff + anti-entropy
        │
        ▼
  store/client.go             what the application calls: Get / Put
  store/resolver.go           how the application merges siblings
  store/cluster.go            builds and owns everything
        │
        ▼
  store/scenario.go, cmd/*    experiments that use all of the above
```

---

## 4. How the components talk: a write and a read

Before looking at each piece, here is the journey of one write and one read, so you know where each piece fits. The key is `cart:alice` and its 3 replicas are `n2`, `n5`, `n4`.

### A write (`Put`) with W = 2

```
 Client                 n2 (coordinator)               n5                 n4
   │                          │                         │                  │
   │ 1. Put("cart:alice",     │                         │                  │
   │    "eggs,milk", context) │                         │                  │
   │─────────────────────────▶│                         │                  │
   │                          │ 2. make a new version,  │                  │
   │                          │    give it a unique     │                  │
   │                          │    name (dot n2:1),     │                  │
   │                          │    save it locally      │                  │
   │                          │    ── that's ack #1 ──  │                  │
   │                          │                         │                  │
   │                          │ 3. putReq ─────────────▶│                  │
   │                          │ 3. putReq ─────────────────────────────── ▶│
   │                          │                         │ saves, replies   │
   │                          │◀──────────── putResp ───│                  │
   │                          │  ack #2  →  W=2 reached │                  │
   │◀──────── 4. "ok" ────────│                         │                  │
   │                          │                         │     (slow...)    │
   │                          │◀────────────────────────────── putResp ────│
   │                          │  5. arrives later; the client already left │
```

The important part is step 5: **n4 gets the write a bit later**. If someone reads from n4 in that gap with R = 1, they see the old list. That is exactly where stale reads come from.

### A read (`Get`) with R = 2

```
 Client                 n5 (coordinator)               n2                 n4
   │ 1. Get("cart:alice")     │                         │                  │
   │─────────────────────────▶│ 2. ask everyone (itself │                  │
   │                          │    included) ── getReq ▶│                  │
   │                          │ ──────────────── getReq ──────────────────▶│
   │                          │ own copy  = reply #1    │                  │
   │                          │◀──── {eggs,milk} ───────│  reply #2        │
   │                          │ 3. merge the 2 replies  │                  │
   │◀── 4. versions+context ──│                         │                  │
   │                          │◀────────────────────────────── {milk} ─────│ late reply
   │                          │ 5. READ REPAIR: n4 is behind,              │
   │                          │    send it {eggs,milk} ── putReq ─────────▶│
```

The client gets back **every version** that the R replies contained, plus a **context** (a summary of their history). The next `Put` must carry that context so the store knows what the writer had seen.

### Why R + W > N means "no stale reads"

```
   3 replicas:          [ n2 ]  [ n5 ]  [ n4 ]

   write reached W=2:   [ ✔  ]  [ ✔  ]  [    ]
   read asked R=2:              [ ?  ]  [ ?  ]

   2 + 2 = 4 > 3, so the two groups MUST share at least one node (n5 here).
   That shared node has the new value, so the read sees it.

   With R=1, W=1 (1 + 1 = 2, not > 3) the read can land only on n4,
   which hasn't heard yet  →  stale read.
```

In our measurements (no failures), R = W = 1 gave **21.5% stale reads**, and every setting with R + W > 3 gave **0%** (see `REPORT.md`).

---

## 5. Component 1: vector clocks (`vclock/`)

### The story

Each friend keeps a tally on the notebook: "n2 has made 1 change, n5 has made 1 change". Compare two tallies:

* If one tally is **at least as big in every column**, it has seen everything the other saw, so it is **newer**.
* If each tally is bigger in **some** column, each saw something the other didn't. They happened **at the same time** (concurrent). Neither may replace the other.

```
   A = [n2:1  n5:1]          B = [n4:1  n5:1]

          n2   n4   n5
     A:    1    0    1
     B:    0    1    1
           ▲    ▲
           A>B  B>A    → each wins somewhere → CONCURRENT

   C = [n2:1 n4:2 n5:1]
     C vs A:  1≥1, 2≥0, 1≥1   → C is AFTER A (C saw everything A saw)
```

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

```
   client X writes "eggs"  via n1  → clock [n1:2]
   client Y writes "bread" via n1  → clock [n1:3]

   Compare: [n1:3] is AFTER [n1:2]  → "bread" silently replaces "eggs"
```

But X and Y never saw each other's write! They were concurrent, and "eggs" was lost. In our benchmark this plain scheme lost **585 of 3055** acknowledged items. That is why the code uses a sharper idea.

### Dotted version vectors

Each version stores two things separately:

* **Dot** = the write's own unique name: `(node, counter)`, like `n1:3`. "The 3rd write that n1 coordinated for this key."
* **Ctx** (context) = the vector clock of what the writer had **read** before writing.

The rule becomes: **version A replaces version B only if B's dot is inside A's context**, meaning A's author actually saw B.

```
   X: dot n1:2, ctx [n1:1]       Y: dot n1:3, ctx [n1:1]

   Does Y's ctx contain X's dot n1:2?   ctx[n1]=1, and 1 < 2  → NO
   Does X's ctx contain Y's dot n1:3?   1 < 3                 → NO
   → concurrent → keep BOTH as siblings → nothing lost  ✔
```

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

```
   pile:      [ {eggs,milk} dot n2:1 ]
   incoming:    {bread,milk} dot n4:1, ctx [n5:1]

   n2:1 covered by incoming?  ctx[n2]=0 < 1  → no
   n4:1 covered by pile?      pile ctx[n4]=0 → no
   → pile becomes [ {eggs,milk} , {bread,milk} ]   two siblings
```

Two properties make the whole system converge:

* **Order doesn't matter** (commutative): merging A then B gives the same pile as B then A.
* **Repeats don't matter** (idempotent): merging A twice is the same as once.

So it is always safe to resend data, which the repair helpers do freely. After merging, the pile is sorted so every replica holding the same versions lists them in the same order.

---

## 8. Component 4: the ring (`store/ring.go`)

### The story

Draw a big clock face. Each of the 5 friends sticks **64 small flags** with their name at random-looking spots around it. To find where a key lives, you turn the key's name into a spot on the clock face, then **walk clockwise** and write down the first **3 different** names you meet. Those 3 friends keep that key.

```
                       12 o'clock
                   n3 ●       ● n1
               n5 ●             ● n2          (in reality 5 × 64 = 320 flags,
                 ●     ring       ● n4         drawn here as just a few)
               n1 ●             ● n5
                   n2 ●   ●   ● n3
                        n4
     key "cart:alice" hashes to here ──▶ ✱
     walk clockwise from ✱:  n2, n5, (n2 again, skip), n4   → preference list [n2 n5 n4]
     keep walking:           n1, n3                         → fallbacks for sloppy quorums
```

Why 64 flags per friend instead of one? With one flag each, some friends would own big arcs and others tiny ones. Many small flags spread keys (and work) evenly.

### The code

* `NewRing(nodes, vnodes)`: places flag `i` of node `n` at `md5("n#i")` (first 8 bytes as a number). Dynamo uses MD5 too.
* `Walk(key)`: hashes the key, finds the first flag at or after it (wrapping around), and walks, skipping names already listed. Returns **all 5 nodes** in order.
* `PreferenceList(key, n)`: the first `n` (= 3) entries of `Walk`. These are the key's **home** replicas. The rest of the walk are **fallbacks**.

The ring never changes after it is built, so everyone can read it at the same time safely.

---

## 9. Component 5: the network (`store/network.go`, `store/messages.go`)

### The story

All messages go through a pretend **post office**. Each letter waits a random amount of time before delivery. We can also build a **wall** (a partition): letters between the two sides are silently thrown away. Nobody is told "your letter was lost"; the sender just waits, and gives up when its **timeout** runs out, exactly like a real network.

```
          side A                 ║               side B
   ┌──────┐  ┌──────┐  ┌───────┐ ║ ┌──────┐ ┌──────┐ ┌──────┐ ┌───────┐
   │  n2  │  │  n1  │  │clientA│ ║ │  n5  │ │  n4  │ │  n3  │ │clientB│
   └──────┘  └──────┘  └───────┘ ║ └──────┘ └──────┘ └──────┘ └───────┘
                                 ║
     n2 ── putReq ──▶ n5 :   ✉ ──╫──▶ 🗑  dropped. n2 waits 20 ms, then "timeout"
```

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
| `syncReq` / `syncResp` | node → node | Anti-entropy: "here are all keys we share" / "and here are mine" |

`ErrUnavailable` ("quorum not reached") is the error when fewer than R or W replicas answered.

---

## 10. Component 6: the node and the coordinator

### 10.1 The node (`store/node.go`)

### The story

Each friend has a notebook with **two sections**:

* **My lists** (`data`): keys this friend is a home replica for.
* **Sticky notes for others** (`hints`): writes the friend is holding on behalf of a friend who was unreachable.

The friend also keeps a little private counter per key (`issued`) so they never give two different writes the same name.

```
   ┌──────────────────────── Node n1 ────────────────────────┐
   │ data   : "cart:bob"   → [ {apples}  dot n1:4 ]           │
   │          "cart:carl"  → [ {tea} , {coffee} ]  siblings   │
   │ hints  : for n4 → "cart:alice" → [ {eggs,milk} ]         │
   │ issued : "cart:bob" → 4, "cart:alice" → 1                │
   │                                                          │
   │ handle(msg):  putReq / getReq     → touch my notebook    │
   │               coordPutReq/GetReq  → act as coordinator   │
   │               syncReq             → anti-entropy         │
   │ background:   every 5 ms try to deliver sticky notes     │
   └──────────────────────────────────────────────────────────┘
```

### The code

* `NodeConfig`: N (= 3), `Policy`, `ReplicaTimeout` (how long a coordinator waits, 20 ms), `ReadRepair` (on), `HandoffInterval` (5 ms), `ClockSkew` (for LWW experiments), `Clocks`, and `ReplicateNewOnly` (an experiment switch, see below).
* `handle(from, req)` is the node's front door. The network calls it once per incoming letter, possibly many at the same time, so the notebook is protected by a lock (`mu`). **No node ever holds its lock while sending a message**, so a busy node still answers others.
  * `putReq` with a `Hint` for another node → merge into `hints[owner][key]`; otherwise merge into `data[key]`.
  * `getReq` → `localRead`: my data **plus** any sticky notes for that key (so a stand-in can answer a sloppy read).
  * `coordPutReq` / `coordGetReq` → become the coordinator (next section).
  * `syncReq` → anti-entropy (section 11).
* `start()` launches the background loop that calls `handoffHints()` every `HandoffInterval`.
* `Snapshot`, `HintCount`, `Coordinated` are read-only peeks used by tests and benchmarks.

### 10.2 The coordinator (`store/coordinator.go`)

Any node becomes "the coordinator" for a request just by receiving it from a client. It does three jobs.

#### Job 1: decide who to contact (`targets`)

```
   Walk("cart:alice") = [ n2  n5  n4 | n1  n3 ]
                          home (N=3)   fallbacks

   STRICT quorum:  always [n2, n5, n4]. If n4 is cut off, that reply just fails.

   SLOPPY quorum, n4 unreachable:
                   [n2, n5, n1 (hint: "for n4")]
                    n1 stands in for n4 and holds the write as a sticky note
```

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

```
   ┌────────────────┬────────────────────────────┬─────────────────────────────┐
   │ Helper         │ When it runs               │ What it fixes               │
   ├────────────────┼────────────────────────────┼─────────────────────────────┤
   │ Read repair    │ after every read           │ replicas that answered the  │
   │                │ (coordinator.go)           │ read with old data          │
   ├────────────────┼────────────────────────────┼─────────────────────────────┤
   │ Hinted handoff │ every 5 ms on each node    │ writes a stand-in is        │
   │                │ (repair.go)                │ holding for a missing owner │
   ├────────────────┼────────────────────────────┼─────────────────────────────┤
   │ Anti-entropy   │ when Cluster.AntiEntropy() │ anything else: keys nobody  │
   │                │ is called (repair.go)      │ has read recently           │
   └────────────────┴────────────────────────────┴─────────────────────────────┘
```

### Read repair

After a read, the coordinator has every reply. For each replica, it computes what that replica is **missing** (versions in the merged pile that the replica's answer doesn't cover) and sends only those. A replica that's already up to date gets nothing.

### Hinted handoff

```
   during the partition                    after healing
   ┌────┐  sticky note:                    ┌────┐  putReq {eggs,milk}   ┌────┐
   │ n1 │  "cart:alice for n4"   ──────▶   │ n1 │ ────────────────────▶ │ n4 │
   └────┘                                  └────┘ ◀──── ack ─────────── └────┘
                                           n1 removes the note (only the versions it sent)
```

Every 5 ms, each node looks at its sticky notes, picks the owners that are reachable again, sends them their writes, and removes a note **only after the owner acknowledged it**. If a newer note for the same key arrived meanwhile, that one stays for the next round.

### Anti-entropy

For every pair of nodes that can reach each other, one sends "here's every key I own that you also should own", the other merges it and replies with its own half. Both sides end up with the same versions for shared keys. Real Dynamo uses Merkle trees to send only the differences; here we send the full state because the data is small. Only owned `data` is exchanged; hints are left to hinted handoff.

---

## 12. Component 8: the client, the resolver and the cluster

### 12.1 The client (`store/client.go`)

This is what an application uses. A `Client` has `R`, `W`, `Sloppy` and `Timeout`, so two clients can use different settings on the same data.

* **Choosing a coordinator**: pick a **random** home replica the client can reach. Random spreads load, but it also means a read may land on a replica the last write hasn't reached yet. (`FirstCoordinator` always picks the first one instead; the architecture benchmark compares the two.) If no home replica is reachable and `Sloppy` is on, use the first reachable node further round the ring.
* `Get(key)` → `GetResult{Versions, Context}`. `Context` is the `Merge` of every sibling's clock: "everything you just saw".
* `Put(key, value, ctx)`: pass the `Context` from your last `Get` (or `nil` for a first, blind write). The new version replaces exactly the versions that context covers.
* `GetResolved(key, resolve)`: read; if there are siblings, merge their values with `resolve` and write the result back with the merged context. That single write replaces all siblings at once, so they collapse into one.

```
   Get  ──▶ siblings: {eggs,milk} [n2:1 n5:1]   {bread,milk} [n4:1 n5:1]
            context:  [n2:1 n4:1 n5:1]
            │
   resolve  ▼  UnionResolver → "bread,eggs,milk"
            │
   Put("bread,eggs,milk", context [n2:1 n4:1 n5:1])
            │   new version's context contains both dots n2:1 and n4:1
            ▼
   every replica: [ {bread,eggs,milk} ]   one version again ✔
```

### 12.2 The resolver (`store/resolver.go`)

`UnionResolver` treats a value as a comma-separated set, like a shopping cart, and returns the sorted union of all siblings. Sorting means the same siblings always give the same answer. Its promise: **no item that any client was told "saved" is ever lost**. (It can't delete an item that one side removed; that's the known trade-off of a union cart, the same as in the Dynamo paper.)

### 12.3 The cluster (`store/cluster.go`)

`DefaultConfig()` describes the standard setup:

| Setting | Value |
|---|---|
| Nodes | 5 (`n1`..`n5`), 64 virtual nodes each |
| Replicas per key (N) | 3 |
| Policy | keep siblings |
| Replica timeout | 20 ms |
| Read repair | on |
| Hinted handoff | every 5 ms |
| Network | default latencies, seed 1 |

`NewCluster(cfg)` builds the network, the ring and the nodes, and starts each node's background loop. Then:

* `NewClient(id, R, W)`: a new client (R and W must be between 1 and N). Its timeout is 3 × the replica timeout.
* `AntiEntropy()`: one sync round for every reachable pair.
* `SetSkew(id, d)`: shift one node's clock (to show LWW going wrong).
* `WaitHintsDrained(d)`: wait until every sticky note has been delivered.
* `Close()`: stop the background loops.

---

## 13. Putting it all together: the partition story

`store/scenario.go` (`RunPartitionScenario`) plays out the shopping-list story from section 1 for real. This is the actual output of `go run ./cmd/demo`, case 1 (siblings, R = 1, W = 1, strict quorum), with each step explained.

**Step 1. Everyone agrees.** A first write `{milk}` reaches all three replicas. Its dot is `n5:1`.

```
   n2: {milk}      n5: {milk}      n4: {milk}          all hold dot n5:1
```

Both clients read it, so both have context `[n5:1]`.

**Step 2. The wall goes up.**

```
   side A: n2, n1, clientA     ║     side B: n5, n4, n3, clientB
```

**Step 3. Both sides write.**

* Client A adds eggs through n2. New version: value `{eggs,milk}`, dot `n2:1`, context `[n5:1]`. With W = 1, n2's own save is enough: **ok**.
* Client B adds bread through n4. New version: value `{bread,milk}`, dot `n4:1`, context `[n5:1]`. **ok**.

```
   n2: {eggs,milk}             ║     n5: {bread,milk}   n4: {bread,milk}
```

**Step 4. The wall comes down; repair runs.** Hinted handoff drains, then one anti-entropy round. Now each replica receives the other side's version and merges it. Neither version's context contains the other's dot, so they're **siblings**:

```
   every replica: [ {eggs,milk} clock [n2:1 n5:1] ,  {bread,milk} clock [n4:1 n5:1] ]
```

**Step 5. Read with R = 3 and reconcile.** The client gets 2 siblings, merges them with `UnionResolver` into `{bread,eggs,milk}`, and writes that back with the merged context `[n2:1 n4:1 n5:1]`. The coordinator was n4, so the new dot is `n4:2`.

**Step 6. Check.**

```
   replica n2 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]
   replica n5 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]
   replica n4 holds {bread,eggs,milk} [n2:1 n4:2 n5:1]
   converged, and every acknowledged item (milk, eggs, bread) is still there ✔
```

### The same story with other settings

| Setting | What happens | Why |
|---|---|---|
| Siblings, R = W = 2, strict | Side A's write **fails** (only 1 of 2 acks: n2 can't reach n5 or n4) but n2 already saved it, so after healing it still shows up as a sibling. Final value is still `{bread,eggs,milk}` | A failed quorum is not a rollback |
| Siblings, R = W = 2, sloppy | Side A's write **succeeds** using n1 as a stand-in with a sticky note; hinted handoff delivers it after healing | Sloppy quorum trades consistency for availability |
| Last-writer-wins | Only one version survives: `{bread,milk}`. **Eggs is lost** even though client A was told "ok" | LWW silently throws one away |
| LWW, side A's clock +50 ms | Now `{eggs,milk}` wins and **bread is lost**, even though bread was written later | Clocks on different machines disagree, so "latest timestamp" isn't really "latest" |

---

## 14. The programs in `cmd/` and what they prove

| Command | What it runs | Output file |
|---|---|---|
| `go run ./cmd/demo` | The partition story above, for 5 settings. Exits with an error if a declared rule is broken | `results/demo.txt` |
| `go run ./cmd/bench` | **E1** stale reads per (R, W); **E2** many clients writing the same keys concurrently (siblings, lost items, clock modes); **E3** availability and staleness during a partition, strict vs sloppy | `results/benchmark.md` |
| `go run ./cmd/archbench` | **A1**–**A5**: evidence for design choices (ring balance with vnodes, sending the whole pile, repair helpers, random vs first coordinator, timeout length) | `results/architecture.md` |
| `go test -race ./...` | Unit and scenario tests, with Go's race detector | |

How does the benchmark know a read was stale? `cmd/bench/oracle.go` is a **referee**. Every client tells the referee about each write that was acknowledged. Before a read starts, the client asks the referee "which writes were already confirmed?", and after the read it checks that the store returned all of them (or something newer). Missing one means **stale**.

Headline numbers (from `REPORT.md`):

* **Stale reads, no failures:** R = W = 1 → 21.5%; R = 1, W = 2 → 0.4%; any R + W > 3 → 0%.
* **Concurrent writers:** dotted version vectors lost 0 acknowledged items; plain per-node vector clocks lost 585 of 3055; LWW lost 4572 of 5426.
* **Partition:** strict R = W = 2 had 0% stale reads but the minority side could barely write; sloppy quorums at R = W ≤ 2 stayed about 100% available, but around 90% of reads were stale during the partition. No acknowledged write went missing after healing in any setting.

---

## 15. Glossary

| Word | Meaning |
|---|---|
| **Key-value store** | A big dictionary: `Put(key, value)`, `Get(key)` |
| **Replica** | One of the copies of a key (here 3 per key) |
| **Node** | One storage computer (`n1`..`n5`) |
| **Preference list** | The N nodes that are a key's home, found by walking the ring |
| **Consistent hashing / ring** | Placing nodes and keys on a circle so each key has a clear owner |
| **Virtual node (vnode)** | One of a node's many positions on the ring (64 each), for even spreading |
| **Coordinator** | The node a client sends a request to; it talks to the other replicas |
| **Quorum, R, W** | How many replicas must answer a read (R) or confirm a write (W) |
| **N** | Copies per key (3) |
| **Stale read** | A read that misses a write that was already confirmed |
| **Partition** | A network split; messages between the sides are lost |
| **Strict quorum** | Only the home replicas count toward R or W |
| **Sloppy quorum** | Stand-in nodes may count when home replicas are unreachable |
| **Hint / hinted handoff** | A write held by a stand-in for its real owner, delivered later |
| **Read repair** | After a read, sending newer versions to replicas that answered with old ones |
| **Anti-entropy** | Background syncing between pairs of replicas |
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

For the detailed design decisions and every experiment's evidence, see [ARCHITECTURE.md](../ARCHITECTURE.md) and [REPORT.md](../REPORT.md).
