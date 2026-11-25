# MESSAGES: every message in dynamo-store, explained

This guide lists **every message** that travels between the parts of the store, shows a **complete real example** of each one (nothing cut out), and explains what each field means, who sends it, when, and what the receiver does with it. After that it replays whole conversations, message by message, so you can watch a write, a read, a partition, a sloppy write, read repair, hinted handoff and anti-entropy happen.

It is a companion to [ARCHITECTURE.md](ARCHITECTURE.md). If a word is unfamiliar, that guide's glossary explains it. It describes the current design (branches `adaptive-defaults` and `ablations`): anti-entropy now compares Merkle trees, which adds **five new message types** (sections 6.5 to 6.9). The old full-state messages are kept for comparison (6.10 and 6.11). Why Merkle was chosen is measured in [ABLATION.md](ABLATION.md).

> **Where the examples come from.** Every example below was captured from a real run of the code. To capture them, a temporary print statement was added to `Network.Send` in a scratch copy of the repository (the repository itself was not changed). Coordinators are picked at random, so if you run `go run ./cmd/demo` yourself, the node names inside dots (like `n5:1`) can differ from these. The shapes and the rules are always the same. `TS` values are wall-clock times in nanoseconds since 1970 (`1795339671831953497` is 22 November 2026). The Merkle examples in sections 6.5 to 6.9 and 7.7 come from a second capture on the current code, with a small cluster setting (8 partitions, tree depth 2) so the messages fit on a page. The default is 256 partitions and depth 4, so there the first `merkleReq` of a real exchange carries about 77 refs instead of 5; the shapes and the rules are the same.

---

## Contents

1. [Who talks to whom](#1-who-talks-to-whom)
2. [How a message travels](#2-how-a-message-travels)
3. [The catalogue: all 15 message types](#3-the-catalogue-all-15-message-types)
4. [What is inside a message: Version, Dot and clock](#4-what-is-inside-a-message-version-dot-and-clock)
5. [Client to coordinator messages](#5-client-to-coordinator-messages)
6. [Node to node messages](#6-node-to-node-messages)
7. [Whole conversations, message by message](#7-whole-conversations-message-by-message)
8. [Calls that are not messages](#8-calls-that-are-not-messages)
9. [How the message layer is implemented](#9-how-the-message-layer-is-implemented)

---

## 1. Who talks to whom

There are only two kinds of conversation:

* **Client to coordinator.** The application's client asks one node to do a whole read or write on its behalf.
* **Node to node.** The coordinator (or a background helper) asks other nodes to do one small thing to their own notebook: store these versions, tell me what you have, or swap everything we share.

<div align="center">

```
             +-----------------+                                             
             |     Client      |                                             
             +-----------------+                                             
               |            ^                                                
coordPutReq    |            |   coordPutResp                                 
coordGetReq    |            |   coordGetResp                                 
               v            |                                                
             +-----------------+                                             
             |   Coordinator   |   (any node, e.g. n2)                       
             +-----------------+                                             
         putReq |  ^ putResp      getReq |  ^ getResp                        
                v  |                     v  |                                
+--------------------+               +--------------------+                  
|   Replica  n5      |               |   Replica  n4      |                  
+--------------------+               +--------------------+                  
                                                                             
Background, between any two nodes:                                           
  hinted handoff ..... putReq   ---> owner,  putResp  <---                   
  anti-entropy ....... merkleReq / leafReq / pushReq ---> peer,  replies <---
  read repair ........ putReq   ---> stale replica, putResp <---             
```

</div>

Every request gets exactly one reply type. If no reply comes back, the message or its reply was lost.

---

## 2. How a message travels

All messages go through one function, `Network.Send(ctx, from, to, req)` in `network/network.go`. Messages are ordinary Go values handed over in memory. Nothing is converted to bytes, because there is no real network on `main`.

<div align="center">

```
sender                         Network                          receiver     
  |                               |                                 |        
  | Send(ctx, from, to, req)      |                                 |        
  |------------------------------>|                                 |        
  |                               |  wait: one sampled delay        |        
  |                               |  check: same side of the wall?  |        
  |                               |    no  -> drop request          |        
  |                               |    yes -> call handler          |        
  |                               |-------------------------------->|        
  |                               |                  resp = handle(from, req)
  |                               |<--------------------------------|        
  |                               |  wait: another sampled delay    |        
  |                               |  check: same side of the wall?  |        
  |                               |    no  -> drop reply            |        
  |<------------------------------|    yes -> deliver reply         |        
  |  (resp, nil)                  |                                 |        
  |                               |                                 |        
  |  or, if ctx runs out first:   |                                 |        
  |  (nil, ErrTimeout)            |                                 |        
```

</div>

The rules, all from `Network.Send`:

| Rule | What it means |
|---|---|
| **Two legs, two delays** | The request waits one random delay, the reply waits another. Node to node: 400 µs plus random jitter averaging 800 µs, and 3% of the time an extra 5 ms. Any hop touching a client: 100 µs plus jitter averaging 100 µs. |
| **The wall is checked on arrival** | A partition that appears while a message is on its way still drops it. |
| **Each leg can be lost separately** | If only the reply is lost, the receiver **did** the work, but the sender thinks it failed. The sender can't tell the difference. |
| **Nobody says "lost"** | A dropped message just never arrives. The sender gives up when its deadline (`ctx`) runs out and gets `ErrTimeout` (`"network: request timed out"`). |
| **Talking to yourself is free** | If `from == to` (a coordinator reading its own copy), the handler is called directly with no delay and no partition check. |
| **Late replies are thrown away** | The delivery keeps going after the sender has given up; the reply then goes nowhere. |

Each sender picks its own deadline: a coordinator waits `ReplicaTimeout` for replicas, and a client waits 3 times that for its coordinator. `ReplicaTimeout` is computed from the network delays (two hops of base + 4 mean jitters + the slow-path extra, about 17 ms on the default network), so a client waits about 52 ms.

---

## 3. The catalogue: all 15 message types

All are defined in `store/messages.go`. The receiver is always a node; clients never receive requests.

| # | Message | From | To | Purpose | Reply |
|---|---|---|---|---|---|
| 1 | `coordPutReq` | client | coordinator | "Write this value with this context, wait for W acks" | `coordPutResp` |
| 2 | `coordPutResp` | coordinator | client | The new version, how many acks, and an error if fewer than W | |
| 3 | `coordGetReq` | client | coordinator | "Read this key, wait for R replies" | `coordGetResp` |
| 4 | `coordGetResp` | coordinator | client | The merged versions, how many replies, and an error if fewer than R | |
| 5 | `putReq` | node | node | "Merge these versions into your notebook" (optionally "hold them for someone else") | `putResp` |
| 6 | `putResp` | node | node | "Done." Empty: arriving at all is the acknowledgement | |
| 7 | `getReq` | node | node | "What versions do you have for this key?" | `getResp` |
| 8 | `getResp` | node | node | "This pile of versions" | |
| 9 | `merkleReq` | node | node | Anti-entropy: "Here are my fingerprints for these tree nodes" | `merkleResp` |
| 10 | `merkleResp` | node | node | "These are the ones that differ" | |
| 11 | `leafReq` | node | node | "Here are my key hashes under the leaves that differ" | `leafResp` |
| 12 | `leafResp` | node | node | "My versions of the keys that differ, and the keys I want from you" | |
| 13 | `pushReq` | node | node | "Here are the versions you asked for" | `putResp` |
| 14 | `syncReq` | node | node | Old full-state anti-entropy: "Here is every key we both should own" | `syncResp` |
| 15 | `syncResp` | node | node | "And here is my half" | |

Two errors can travel inside replies or come back from `Send`:

| Error | Text | Meaning |
|---|---|---|
| `ErrUnavailable` | `quorum not reached`, wrapped like `quorum not reached: put 1/2 acks` | Fewer than W (or R) replicas answered in time |
| `ErrTimeout` | `network: request timed out` | This one message or its reply was lost or late |

---

## 4. What is inside a message: Version, Dot and clock

Most messages carry one or more **versions**. Here is one complete version, exactly as it travelled in a real run, with every field explained.

```go
Version{
    Value:  "eggs,milk",                     // the stored value (a shopping cart)
    Dot:    Dot{Node: "n2", Counter: 1},     // this write's unique name: n2's 1st write for this key
    Ctx:    vclock.VC{"n5": 1},              // what the writer had read: the version named n5:1
    TS:     1795339671840477828,             // n2's wall clock when it made the version (ns)
    Origin: "n2",                            // the coordinator that made it
}
```

| Field | Type | Used for |
|---|---|---|
| `Value` | `string` | The data. In the demo it is a comma-separated set of items |
| `Dot` | `Dot{Node string; Counter uint64}` | Unique name of the write. A coordinator never issues the same counter twice for one key |
| `Ctx` | `vclock.VC` (a `map[string]uint64`) | The causal context: which writes the client had seen. A version replaces another only if its `Ctx` contains the other's dot |
| `TS` | `int64` | Only used by last-writer-wins. Ignored by the default sibling policy |
| `Origin` | `string` | Only used by last-writer-wins, to break a tie between equal timestamps |

The version's **clock** (what logs print, like `[n2:1 n5:1]`) is `Ctx` with the dot added: `Ctx` says `n5:1`, the dot adds `n2:1`.

In the examples below, `vclock.VC{}` means an empty clock, the context of a first, blind write.

---

## 5. Client to coordinator messages

### 5.1 `coordPutReq`: "please write this"

**Definition (complete):**

```go
coordPutReq struct {
    Key     string    // key being written
    Value   string    // new value
    Context vclock.VC // causal context from the client's last Get
    W       int       // write quorum: acknowledgements required
    Sloppy  bool      // allow fallback nodes for unreachable replicas
}
```

**Sent by** `Client.Put` (`store/client.go`), to a coordinator the client picked at random among the key's reachable home replicas.

**Real example 1: the very first write.** The scenario's `admin` client (R = 3, W = 3) writes `milk` with no context:

```go
// admin -> n5
coordPutReq{
    Key:     "cart:alice",
    Value:   "milk",
    Context: vclock.VC{},      // nil from the caller becomes an empty clock
    W:       3,
    Sloppy:  false,
}
```

**Real example 2: a write based on a read.** Client A had read `milk` (whose dot is `n5:1`), so it passes that context:

```go
// clientA -> n2
coordPutReq{
    Key:     "cart:alice",
    Value:   "eggs,milk",
    Context: vclock.VC{"n5": 1},   // "I had seen the version named n5:1"
    W:       1,
    Sloppy:  false,
}
```

**What the coordinator does** (`Node.coordPut`, `store/coordinator.go`):

1. Picks the target list (home replicas, or stand-ins if `Sloppy`).
2. Picks a fresh counter for the dot, higher than anything it has used, seen in `Context`, or stored for this key.
3. Builds the version, saves it locally (that is ack 1).
4. Sends a `putReq` with its whole local pile to every other target.
5. Counts `putResp`s until W succeed, or everyone has answered or timed out.
6. Replies with `coordPutResp`.

### 5.2 `coordPutResp`: "here's what I wrote"

**Definition (complete):**

```go
coordPutResp struct {
    Version Version // the version the coordinator created
    Acks    int     // acknowledgements collected (including its own)
    Err     error   // wraps ErrUnavailable when Acks < W
}
```

**Real example 1: success.** Reply to the first write above. All 3 replicas acknowledged:

```go
// n5 -> admin
coordPutResp{
    Version: Version{
        Value:  "milk",
        Dot:    Dot{Node: "n5", Counter: 1},
        Ctx:    vclock.VC{},
        TS:     1795339671831953497,
        Origin: "n5",
    },
    Acks: 3,
    Err:  nil,
}
```

**Real example 2: quorum failure.** A client with W = 2 is stuck on the small side of a partition with only `n2`. `n2` saved the write itself, but both `putReq`s to `n5` and `n4` were dropped:

```go
// n2 -> solo
coordPutResp{
    Version: Version{
        Value:  "eggs,milk",
        Dot:    Dot{Node: "n2", Counter: 2},
        Ctx:    vclock.VC{"n2": 1},
        TS:     1795339696735672396,
        Origin: "n2",
    },
    Acks: 1,
    Err:  "quorum not reached: put 1/2 acks",   // fmt.Errorf("%w: put %d/%d acks", ErrUnavailable, 1, 2)
}
```

Note that `Version` is filled in even on failure. **A failed write is not undone**: `n2` keeps it, and once the partition heals it spreads and can show up as a sibling.

**What the client does:** `Client.Put` returns `(Version, Err)` to the application.

### 5.3 `coordGetReq`: "please read this"

**Definition (complete):**

```go
coordGetReq struct {
    Key    string // key being read
    R      int    // read quorum: replies required
    Sloppy bool   // allow fallback nodes for unreachable replicas
}
```

**Sent by** `Client.Get`.

**Real example:**

```go
// clientA -> n4
coordGetReq{
    Key:    "cart:alice",
    R:      1,
    Sloppy: false,
}
```

**What the coordinator does** (`Node.coordGet`): sends `getReq` to **all** N targets at once (itself included, as a free local call), merges the first R successful replies, answers with `coordGetResp`, then keeps waiting for the rest in the background to do read repair.

### 5.4 `coordGetResp`: "here's what I found"

**Definition (complete):**

```go
coordGetResp struct {
    Versions []Version // merged sibling set from the first R replies
    Replies  int       // successful replies merged into Versions
    Err      error     // wraps ErrUnavailable when Replies < R
}
```

**Real example 1: one version.**

```go
// n4 -> clientA
coordGetResp{
    Versions: []Version{
        {
            Value:  "milk",
            Dot:    Dot{Node: "n5", Counter: 1},
            Ctx:    vclock.VC{},
            TS:     1795339671831953497,
            Origin: "n5",
        },
    },
    Replies: 1,
    Err:     nil,
}
```

**Real example 2: two siblings after a partition healed.** Read with R = 3:

```go
// n4 -> reader
coordGetResp{
    Versions: []Version{
        {
            Value:  "eggs,milk",
            Dot:    Dot{Node: "n2", Counter: 1},
            Ctx:    vclock.VC{"n5": 1},
            TS:     1795339671840477828,
            Origin: "n2",
        },
        {
            Value:  "bread,milk",
            Dot:    Dot{Node: "n5", Counter: 2},
            Ctx:    vclock.VC{"n5": 1},
            TS:     1795339671845477718,
            Origin: "n5",
        },
    },
    Replies: 3,
    Err:     nil,
}
```

Neither version's `Ctx` contains the other's dot (`n2:1` is not in `{n5:1}`, and `n5:2` is not either, because `1 < 2`), so both are kept.

**Real example 3: quorum failure.** The same stuck client reads with R = 2. Only `n2`'s own copy answered:

```go
// n2 -> solo
coordGetResp{
    Versions: []Version{
        {
            Value:  "eggs,milk",
            Dot:    Dot{Node: "n2", Counter: 2},
            Ctx:    vclock.VC{"n2": 1},
            TS:     1795339696735672396,
            Origin: "n2",
        },
    },
    Replies: 1,
    Err:     "quorum not reached: get 1/2 replies",
}
```

Even on failure, whatever did arrive is returned, so the caller can still look at it.

**What the client does:** `Client.Get` turns this into a `GetResult`. It keeps `Versions` and builds the **context** by merging every version's clock. For example 2 that is:

```go
GetResult{
    Versions: /* the two versions above */,
    Context:  vclock.VC{"n2": 1, "n5": 2},   // [n2:1 n5:1] merged with [n5:2]
}
```

The client passes that `Context` to its next `Put`, so the new write replaces both siblings.

---

## 6. Node to node messages

### 6.1 `putReq`: "merge these versions into your notebook"

**Definition (complete):**

```go
putReq struct {
    Key  string    // key being written
    V    []Version // versions to merge into the receiver's sibling set
    Hint string    // intended owner when stored on a fallback node
}
```

This is the busiest message. It has **four different senders**, all using the same shape:

| Used by | When | `V` holds | `Hint` |
|---|---|---|---|
| Write replication (`coordPut`) | Every write | The coordinator's **whole** local pile for the key, not just the new version | `""` for a home replica, or the owner's name for a stand-in |
| Read repair (`coordGet`) | After a read, if a replica answered with old data | Only the versions that replica is **missing** | Same hint the read used |
| Hinted handoff (`handoffHints`) | Every 5 ms, once an owner is reachable again | The versions a stand-in was holding for that owner | `""` (it's going to its real owner) |

**What the receiver does** (`Node.handle`, `store/node.go`):

<div align="center">

```
putReq arrives                                                         
     |                                                                 
     +-- Hint is "" or my own name? --> merge V into data[Key]         
     |                                                                 
     +-- Hint names another node ----> merge V into hints[Hint][Key]   
     |                                 (a sticky note to deliver later)
     v                                                                 
reply putResp{}                                                        
```

</div>

Merging uses the same rule everywhere: drop what is covered, keep what is concurrent. Receiving the same versions twice changes nothing, which is why resending is always safe.

**Real example 1: plain replication.** `n5` coordinates the first write and copies it to the other two home replicas:

```go
// n5 -> n4   (and the same to n5 -> n2)
putReq{
    Key: "cart:alice",
    V: []Version{
        {
            Value:  "milk",
            Dot:    Dot{Node: "n5", Counter: 1},
            Ctx:    vclock.VC{},
            TS:     1795339671831953497,
            Origin: "n5",
        },
    },
    Hint: "",
}
```

**Real example 2: sloppy replication with a hint.** During a partition, `n2` can't reach home replica `n5`, so it sends the write to stand-in `n1` and labels it "for n5":

```go
// n2 -> n1
putReq{
    Key: "cart:alice",
    V: []Version{
        {
            Value:  "eggs,milk",
            Dot:    Dot{Node: "n2", Counter: 1},
            Ctx:    vclock.VC{"n4": 1},
            TS:     1795339672100634772,
            Origin: "n2",
        },
    },
    Hint: "n5",                 // "you're holding this for n5"
}
```

**Real example 3: hinted handoff.** After the partition heals, `n1` delivers that sticky note to `n5`. Same versions, but no hint now, because `n5` is the real owner:

```go
// n1 -> n5
putReq{
    Key: "cart:alice",
    V: []Version{
        {
            Value:  "eggs,milk",
            Dot:    Dot{Node: "n2", Counter: 1},
            Ctx:    vclock.VC{"n4": 1},
            TS:     1795339672100634772,
            Origin: "n2",
        },
    },
    Hint: "",
}
```

**Real example 4: read repair.** `n4` was cut off when `milk` was written, so it answered a read with nothing. The coordinator `n2` then sends it exactly what it lacks:

```go
// n2 -> n4
putReq{
    Key: "cart:alice",
    V: []Version{
        {
            Value:  "milk",
            Dot:    Dot{Node: "n2", Counter: 1},
            Ctx:    vclock.VC{},
            TS:     1795339689862852294,
            Origin: "n2",
        },
    },
    Hint: "",
}
```

**Real example 5: the reconciling write.** The client merged two siblings into `bread,eggs,milk` and wrote it with context `{n2:1, n5:2}`. Coordinator `n2` replicates the result; its pile is now just this one version, because it replaced both siblings:

```go
// n2 -> n5   (and the same to n2 -> n4)
putReq{
    Key: "cart:alice",
    V: []Version{
        {
            Value:  "bread,eggs,milk",
            Dot:    Dot{Node: "n2", Counter: 2},
            Ctx:    vclock.VC{"n2": 1, "n5": 2},
            TS:     1795339671967878434,
            Origin: "n2",
        },
    },
    Hint: "",
}
```

### 6.2 `putResp`: "done"

**Definition (complete):**

```go
putResp struct{}
```

**Real example:**

```go
// n4 -> n5
putResp{}
```

It has no fields. Getting it back at all is the acknowledgement. Not getting it back (`ErrTimeout`) counts as a failed ack.

### 6.3 `getReq`: "what do you have?"

**Definition (complete):**

```go
getReq struct {
    Key string // key being read
}
```

**Sent by** `coordGet`, once to each of the N targets.

**Real example:**

```go
// n4 -> n2
getReq{
    Key: "cart:alice",
}
```

**What the receiver does:** `Node.localRead` returns its own versions for the key **plus** any sticky notes it holds for that key, merged together. Including the sticky notes lets a stand-in answer a sloppy read with the writes it is holding.

### 6.4 `getResp`: "this is what I have"

**Definition (complete):**

```go
getResp struct {
    Versions []Version // the replica's current versions of the key
}
```

**Real example 1: one version.**

```go
// n2 -> n4
getResp{
    Versions: []Version{
        {
            Value:  "milk",
            Dot:    Dot{Node: "n5", Counter: 1},
            Ctx:    vclock.VC{},
            TS:     1795339671831953497,
            Origin: "n5",
        },
    },
}
```

**Real example 2: nothing.** A replica that never received the key (this one was cut off during the write) answers with an empty list. That empty answer is what triggers read repair:

```go
// n4 -> n2
getResp{
    Versions: []Version{},
}
```

### 6.5 `merkleReq`: "here are my fingerprints for these tree nodes"

Every partition of the ring has a small **tree of fingerprints** (hashes) over its keys, and the replicas of a partition keep identical trees. A tree node is named by a `nodeRef`: which partition, and which position in that partition's tree (position 1 is the top; node `i` has children `2i` and `2i+1`).

**Definition (complete):**

```go
nodeRef struct{ P, I int }  // partition, position in the tree
merkleReq struct {
    Refs   []nodeRef // tree nodes the sender is asking about
    Hashes []uint64  // the sender's hash at each ref, same order
}
```

**Sent by** `Node.merkleSync`, the start of every anti-entropy exchange. The first request names the **top** (`I: 1`) of every partition the two nodes share. Later requests name the **children** of the nodes the peer said differ.

**Real example 1: the opening request.** `n1` and `n2` share five partitions. Four have no keys at all (hash `0x0`); partition 5 holds `cart:alice` and some other keys:

```go
// n1 -> n2
merkleReq{
    Refs: []nodeRef{
        {P: 0, I: 1}, {P: 4, I: 1}, {P: 5, I: 1}, {P: 6, I: 1}, {P: 7, I: 1},
    },
    Hashes: []uint64{
        0x0, 0x0, 0x595a5329e77e1464, 0x0, 0x0,
    },
}
```

**Real example 2: a later request.** `n2` said the top of partition 5 differs, so `n1` now sends the hashes of its two children, positions 2 and 3:

```go
// n1 -> n2
merkleReq{
    Refs:   []nodeRef{{P: 5, I: 2}, {P: 5, I: 3}},
    Hashes: []uint64{0x6e9ff504a7bfd2fe, 0x0},
}
```

**What the receiver does** (`Node.handleMerkle`): for each ref, compares the sender's hash with the hash of its own tree at that position (a missing tree counts as `0x0`), and replies with the refs that **differ**.

### 6.6 `merkleResp`: "these ones differ"

**Definition (complete):**

```go
merkleResp struct {
    Diff []nodeRef // the refs of the request whose hashes differ
}
```

**Real example:** `n2` compares the five tops and finds only partition 5 differs:

```go
// n2 -> n1
merkleResp{
    Diff: []nodeRef{{P: 5, I: 1}},
}
```

An **empty** `Diff` means "everything you asked about matches", and the exchange ends. That is the common case in a healthy cluster, which is why Merkle anti-entropy costs almost nothing when nothing has drifted.

### 6.7 `leafReq`: "here are my key hashes under the leaves that differ"

When the walk reaches the bottom of the tree (a **leaf**, a small bundle of keys), the starter stops asking about fingerprints and sends the keys.

**Definition (complete):**

```go
leafReq struct {
    Leaves []nodeRef         // the leaves whose hashes differ
    Keys   map[string]uint64 // key -> hash of the sender's versions, for every key under those leaves
}
```

**Real example:** after two more rounds of narrowing, only leaf 4 of partition 5 differs. It holds one key:

```go
// n1 -> n2
leafReq{
    Leaves: []nodeRef{{P: 5, I: 4}},
    Keys: map[string]uint64{
        "cart:alice": 0xc7f6c87af36090f9,
    },
}
```

**What the receiver does** (`Node.handleLeaf`): for every key it holds under those leaves, if the sender's hash is missing or different, it adds its own versions to the reply. For every key the sender listed that the receiver's data does not match, it adds the key to a "want" list.

### 6.8 `leafResp`: "my versions, and the keys I want"

**Definition (complete):**

```go
leafResp struct {
    Items map[string][]Version // the receiver's versions of the keys whose hashes differ
    Want  []string             // keys the sender listed that the receiver wants the sender's versions of
}
```

**Real example:** `n2` still has the old version of `cart:alice`, so it returns that and asks for `n1`'s:

```go
// n2 -> n1
leafResp{
    Items: map[string][]Version{
        "cart:alice": {
            {
                Value:  "v0",
                Dot:    Dot{Node: "n1", Counter: 1},
                Ctx:    vclock.VC{},
                TS:     100,
                Origin: "n1",
            },
        },
    },
    Want: []string{"cart:alice"},
}
```

The starter merges `Items` into its own data (merging is order-free and repeat-safe, so nothing is lost even if both sides changed the key), then sends what the peer wants.

### 6.9 `pushReq`: "here are the versions you asked for"

**Definition (complete):**

```go
pushReq struct {
    Items map[string][]Version // the versions the receiver asked for in Want
}
```

**Real example:**

```go
// n1 -> n2
pushReq{
    Items: map[string][]Version{
        "cart:alice": {
            {
                Value:  "eggs,milk",
                Dot:    Dot{Node: "n1", Counter: 9},
                Ctx:    vclock.VC{"n1": 1},
                TS:     999,
                Origin: "n1",
            },
        },
    },
}
```

**What the receiver does:** merges the items into its own data (and into its Merkle tree), then answers with an empty `putResp`. After this, both nodes hold the same versions and the same tree hashes for the key.

### 6.10 `syncReq`: "here is everything we both should own" (the old full-state way)

**Definition (complete):**

```go
syncReq struct {
    Items map[string][]Version // key -> sender's versions
}
```

**Sent by** `Node.fullSync`, used only when `AntiEntropy` is set to `FullStateSync` (the original strategy, kept so Merkle can be measured against it). It is no longer the default. `syncWith` picks between the two. `Cluster.AntiEntropy` calls it for every pair of nodes that can reach each other (10 pairs with 5 nodes).

`Items` holds every key in the sender's own `data` (sticky notes are **not** included) whose home replicas include the receiver.

**Real example 1: nothing to share.** `n1` is not a home replica of any key in this run, so it sends an empty map:

```go
// n1 -> n2
syncReq{
    Items: map[string][]Version{},
}
```

**Real example 2: real data.** Right after the partition healed, `n2` only has side A's version:

```go
// n2 -> n4
syncReq{
    Items: map[string][]Version{
        "cart:alice": {
            {
                Value:  "eggs,milk",
                Dot:    Dot{Node: "n2", Counter: 1},
                Ctx:    vclock.VC{"n5": 1},
                TS:     1795339671840477828,
                Origin: "n2",
            },
        },
    },
}
```

**What the receiver does** (`Node.handleSync`): first merges the incoming items into its own `data`, **then** builds its reply, so the reply already includes what it just learned.

### 6.11 `syncResp`: "and here is my half"

**Definition (complete):**

```go
syncResp struct {
    Items map[string][]Version // key -> receiver's versions
}
```

**Real example:** `n4` had side B's version. It merged `n2`'s version in, and now replies with both siblings:

```go
// n4 -> n2
syncResp{
    Items: map[string][]Version{
        "cart:alice": {
            {
                Value:  "eggs,milk",
                Dot:    Dot{Node: "n2", Counter: 1},
                Ctx:    vclock.VC{"n5": 1},
                TS:     1795339671840477828,
                Origin: "n2",
            },
            {
                Value:  "bread,milk",
                Dot:    Dot{Node: "n5", Counter: 2},
                Ctx:    vclock.VC{"n5": 1},
                TS:     1795339671845477718,
                Origin: "n5",
            },
        },
    },
}
```

`n2` merges this reply, so after one exchange both sides hold the same two siblings.

---

## 7. Whole conversations, message by message

The key is `cart:alice`, its home replicas are `n2, n5, n4`, and the rest of the ring walk is `n1, n3` (the stand-ins). Times go down the page. `X` marks a message dropped by the partition.

### 7.1 The first write (W = 3)

<div align="center">

```
admin              n5 (coordinator)            n4                 n2
  |                      |                      |                  |
  |  coordPutReq         |                      |                  |
  |  milk, ctx {}, W=3   |                      |                  |
  |--------------------->|                      |                  |
  |                      | new dot n5:1         |                  |
  |                      | save locally (ack 1) |                  |
  |                      |  putReq [milk n5:1]  |                  |
  |                      |--------------------->|                  |
  |                      |  putReq [milk n5:1]  |                  |
  |                      |---------------------------------------->|
  |                      |<---------------------|  putResp (ack 2) |
  |                      |<----------------------------------------|
  |                      |                         putResp (ack 3) |
  |  coordPutResp        |                      |                  |
  |  milk n5:1, Acks=3   |                      |                  |
  |<---------------------|                      |                  |
```

</div>

Messages: 1 `coordPutReq`, 2 `putReq`, 2 `putResp`, 1 `coordPutResp`.

### 7.2 A read (R = 1) and why the slow replies still matter

<div align="center">

```
clientA            n4 (coordinator)            n2                 n5
  |                      |                      |                  |
  |  coordGetReq R=1     |                      |                  |
  |--------------------->|                      |                  |
  |                      | getReq to itself     |                  |
  |                      | (free local call)    |                  |
  |                      | -> [milk n5:1]       |                  |
  |                      |      reply 1 = R     |                  |
  |  coordGetResp        |                      |                  |
  |  [milk n5:1], 1      |                      |                  |
  |<---------------------|                      |                  |
  |                      |  getReq              |                  |
  |                      |--------------------->|                  |
  |                      |  getReq              |                  |
  |                      |---------------------------------------->|
  |                      |<---- getResp --------|                  |
  |                      |<------------------------- getResp ------|
  |                      | all 3 match:         |                  |
  |                      | no read repair       |                  |
```

</div>

The client already has its answer after one reply. The coordinator keeps collecting the other two only to check whether anyone needs repair.

### 7.3 Both sides write during a partition (W = 1, strict)

The wall: side A = `n2, n1, clientA`, side B = `n5, n4, n3, clientB`.

<div align="center">

```
clientA        n2           ||        n5           n4         clientB
  |             |           ||         |            |            |   
  | coordPutReq |           ||         |            |            |   
  | eggs,milk   |           ||         |            |            |   
  | ctx {n5:1}  |           ||         |            |            |   
  |------------>|           ||         |            |            |   
  |             | dot n2:1, saved (ack 1 = W)       |            |   
  |<------------|           ||         |            |            |   
  | coordPutResp Acks=1     ||         |            |            |   
  |             |-- putReq -||-- X     |            |            |   
  |             |-- putReq -||------------- X       |            |   
  |             |           ||         |            |            |   
  |             |           ||         | coordPutReq bread,milk  |   
  |             |           ||         | ctx {n5:1}              |   
  |             |           ||         |<------------------------|   
  |             |           ||         | dot n5:2, saved (ack 1) |   
  |             |           ||         |------------------------>|   
  |             |           ||         | coordPutResp Acks=1     |   
  |             |   X ------||- putReq-|            |            |   
  |             |           ||         |-- putReq ->|            |   
  |             |           ||         |<- putResp -|            |   
```

</div>

Notice `n5` used counter **2**, not 1: it had already issued `n5:1` for this key (the first write), and it never reuses a number.

Both clients were told "ok". Now `n2` has `eggs,milk` and `n5, n4` have `bread,milk`.

### 7.4 A sloppy write with a hint, and hinted handoff

Same wall, but clients use `Sloppy = true`, W = 2. Client A's coordinator `n2` can't reach home replicas `n5` and `n4`.

<div align="center">

```
targets for n2:  n2 (itself) | n5 unreachable -> stand-in n1 with Hint "n5"        
                             | n4 unreachable -> next stand-in n3 is also          
                             |                   unreachable -> keep n4 (will fail)
                                                                                   
clientA        n2                    n1                 ||      n4                 
  |             |                     |                 ||       |                 
  | coordPutReq |                     |                 ||       |                 
  | W=2, Sloppy |                     |                 ||       |                 
  |------------>| save dot n2:1 (ack 1)                 ||       |                 
  |             |-- putReq Hint "n5" ->|                ||       |                 
  |             |                     | store as sticky ||       |                 
  |             |                     | note for n5     ||       |                 
  |             |<---- putResp -------|  (ack 2 = W)    ||       |                 
  |             |-- putReq ----------------------------X||       |                 
  |<------------|                     |                 ||       |                 
  | coordPutResp Acks=2               |                 ||       |                 
                                                                                   
...the wall comes down. Within 5 ms n1's handoff loop runs:                        
                                                                                   
                                      n1                        n5                 
                                       |  putReq (no hint)       |                 
                                       |  [eggs,milk n2:1]       |                 
                                       |------------------------>|                 
                                       |<----- putResp ----------|                 
                                       |  delete the sticky note |                 
```

</div>

On side B the same thing happens the other way round: `n4` sent `bread,milk` to stand-in `n3` with `Hint: "n2"`, and `n3` later delivered it to `n2`.

### 7.5 Read repair

`milk` was written with W = 2 while `n4` was cut off, so `n4` never got it. After the wall comes down, a client reads with R = 3:

<div align="center">

```
reader            n2 (coordinator)             n5                n4
  |                     |                       |                 |
  | coordGetReq R=3     |                       |                 |
  |-------------------->|                       |                 |
  |                     | own copy: [milk n2:1] |                 |
  |                     |---- getReq ---------->|                 |
  |                     |---- getReq ---------------------------->|
  |                     |<--- [milk n2:1] ------|                 |
  |                     |<----------------------------- [] -------|
  |                     | merged: [milk n2:1]   |                 |
  | coordGetResp        |                       |                 |
  | [milk n2:1], 3      |                       |                 |
  |<--------------------|                       |                 |
  |                     | n4 lacks milk n2:1 -> repair            |
  |                     |---- putReq [milk n2:1] ---------------->|
  |                     |<--------------------------- putResp ----|
```

</div>

Only `n4` gets a repair, and only with the version it lacks. `n5` already had everything, so it gets nothing.

### 7.6 A failed quorum

A client with R = W = 2 can only reach `n2`:

<div align="center">

```
solo               n2                     ||       n5             n4      
  |                 |                     ||        |              |      
  | coordPutReq W=2 |                     ||        |              |      
  |---------------->| save dot n2:2 (ack 1)||       |              |      
  |                 |--- putReq ----------||-- X    |              |      
  |                 |--- putReq ----------||--------------- X      |      
  |                 | wait one timeout: 2 timeouts                       |
  |<----------------|                     ||        |              |      
  | coordPutResp Acks=1                   ||        |              |      
  | Err "quorum not reached: put 1/2 acks"||        |              |      
  |                 |                     ||        |              |      
  | coordGetReq R=2 |                     ||        |              |      
  |---------------->| own copy (reply 1)  ||        |              |      
  |                 |--- getReq ----------||-- X    |              |      
  |                 |--- getReq ----------||--------------- X      |      
  |<----------------|                     ||        |              |      
  | coordGetResp Replies=1                ||        |              |      
  | Err "quorum not reached: get 1/2 replies"       |              |      
```

</div>

The write is still on `n2`. Once the wall is gone, anti-entropy or read repair will copy it to `n5` and `n4`.

### 7.7 One anti-entropy exchange (Merkle)

Every 100 ms each node picks a random reachable peer and starts one exchange (`Cluster.AntiEntropy()` runs one round over all 10 pairs on demand). This is a real capture of one exchange between `n1` and `n2` after `n1` got a newer version of `cart:alice` that `n2` had not seen yet. (Small setting: 8 partitions, tree depth 2.)

<div align="center">

```
 n1                                                         n2                    
 |-- merkleReq  tops of P0,P4,P5,P6,P7 ------------------->|                      
 |<-- merkleResp Diff: [P5 top] ----------------------------|  only P5 differs    
 |-- merkleReq  children of P5 top: pos 2, pos 3 ---------->|                     
 |<-- merkleResp Diff: [P5 pos 2] ---------------------------|  left half differs 
 |-- merkleReq  children of pos 2: pos 4, pos 5 ----------->|                     
 |<-- merkleResp Diff: [P5 pos 4] ---------------------------|  leaf 4 differs    
 |-- leafReq    leaf 4, keys {cart:alice: c7f6...} --------->|                    
 |<-- leafResp  Items {cart:alice: v0}, Want [cart:alice] ---|  n2 has the old one
 |-- pushReq    Items {cart:alice: eggs,milk} -------------->|                    
 |<-- putResp ------------------------------------------------|  n2 merges it     
 5 round trips, 2 keys on the wire, 229 estimated bytes                           
```

</div>

The five round trips are the `SyncStats` counters: `RoundTrips: 5, Hashes: 10, Keys: 2, Bytes: 229`. A full-state exchange for the same pair would have sent **every** shared key both ways. When nothing differs, the exchange is a single `merkleReq` and an empty `merkleResp`:

<div align="center">

```
 n1                                                     n2                     
 |-- merkleReq tops of every shared partition ---------->|                     
 |<-- merkleResp Diff: [] --------------------------------|  everything matches
 1 round trip, 0 keys                                                          
```

</div>

### 7.8 Reconciling the siblings

The client reads, merges, and writes back (`Client.GetResolved`):

<div align="center">

```
reader                 n4                           n2                          
  |                     |                            |                          
  | coordGetReq R=3     |                            |                          
  |-------------------->|  ... getReq x3 ...         |                          
  |<--------------------|                            |                          
  | coordGetResp: 2 siblings                         |                          
  |   eggs,milk  clock [n2:1 n5:1]                   |                          
  |   bread,milk clock [n5:2]                        |                          
  |                                                  |                          
  | UnionResolver -> "bread,eggs,milk"               |                          
  | context = merge of both clocks = {n2:1, n5:2}    |                          
  |                                                  |                          
  | coordPutReq "bread,eggs,milk", ctx {n2:1 n5:2}, W=3                         
  |------------------------------------------------->|                          
  |                                                  | dot n2:2                 
  |                                                  | ctx covers n2:1 and n5:2 
  |                                                  | -> both siblings replaced
  |                                                  | putReq to n5, n4         
  |<-------------------------------------------------|                          
  | coordPutResp Acks=3                              |                          
```

</div>

Every replica ends up with exactly one version: `bread,eggs,milk`, clock `[n2:2 n5:2]`.

---

## 8. Calls that are not messages

Some parts talk to each other through plain function calls inside the same program, not through the network. They never get lost or delayed.

| Caller | Callee | Call | Why |
|---|---|---|---|
| Client | Ring | `PreferenceList(key, N)`, `Walk(key)` | Find the key's home replicas and stand-ins |
| Client, Node | Network | `Reachable(from, to)` | "Does this node look alive?" (the failure detector) |
| Node | Ring | `Walk(key)`, `PreferenceList(key, N)` | Build the target list; decide which keys to sync with a peer |
| Network | Node | `handle(from, req)` | Deliver a request (each delivery runs on its own goroutine) |
| Node's ticker | Node | `handoffHints()` every 5 ms | Try to deliver sticky notes |
| Node's ticker | Node | `syncWith(random peer)` every 100 ms | Start one anti-entropy exchange by itself |
| Cluster | Node | `syncWith(peer)` | Start one anti-entropy exchange |
| Cluster | Network | `Partition(...)`, `Heal()`, `SetSlow(...)` | Build and remove walls, slow a node |
| Cluster | Node | `SetSkew`, `HintCount`, `Snapshot` | Experiments and checks |
| Client | Resolver | `UnionResolver(values)` | Merge sibling values |

<div align="center">

```
                                   +-----------+                                               
                                   |  Cluster  |                                               
                                   +-----------+                                               
                        NewClient /      |      \ syncWith(peer), SetSkew,                     
                                 /       |       \ HintCount, Snapshot                         
                                v        |        v                                            
+----------+  Send, Reachable  +-----------+  handle(from, req)  +----------+                  
|  Client  |------------------>|  Network  |-------------------->|   Node   |----+             
+----------+                   +-----------+<--------------------+----------+    |             
     |                               ^        Send, Reachable       |    ^       | ticker,     
     |                               |                              |    +-------+ every 5 ms: 
     |                    Partition, Heal, SetSlow                  |            handoffHints()
     |                         (from Cluster)                       |                          
     |  PreferenceList, Walk     +----------+   Walk, PreferenceList |                         
     +-------------------------->|   Ring   |<-----------------------+                         
                                 +----------+                                                  
```

</div>

---

## 9. How the message layer is implemented

**Messages are plain structs.** Each one is a small Go `struct` in `store/messages.go`. They are passed as the Go type `any`, so the network doesn't need to know what is inside.

**One front door per node.** Each node registers one function, `Node.handle`, with the network. It looks at the message's type and picks what to do:

```go
func (nd *Node) handle(from string, req any) any {
    switch r := req.(type) {
    case putReq:      // merge into data, or into hints if r.Hint names another node
    case getReq:      // return localRead(r.Key)
    case coordPutReq: // return nd.coordPut(r)
    case coordGetReq: // return nd.coordGet(r)
    case merkleReq:   // return nd.handleMerkle(r)
    case leafReq:     // return nd.handleLeaf(r)
    case pushReq:     // merge r.Items, reply putResp{}
    case syncReq:     // return nd.handleSync(from, r)  (old full-state sync)
    }
    panic(...) // an unknown type is a programming bug, not a network fault
}
```

**Fan-out with goroutines and channels.** When a coordinator contacts N replicas, it starts one goroutine per replica. Each goroutine calls `Send` and drops the result into a channel sized for every target, so no goroutine is ever stuck even after the coordinator has answered. The coordinator reads from the channel until it has W acks (or R replies) or everyone has answered:

```go
acks := make(chan bool, len(ts))            // room for every target's answer
for i, t := range ts {
    if i == selfIdx { acks <- true; continue }  // my own save is ack 1
    go func(t target) {
        _, err := nd.net.Send(rctx, nd.ID, t.node, putReq{Key: r.Key, V: snapshot, Hint: t.hint})
        acks <- err == nil
    }(t)
}
ok, done := 0, 0
for done < len(ts) && ok < r.W {            // stop at W successes
    if <-acks { ok++ }
    done++
}
```

**Copies, not shared memory.** Because messages are in-memory values, a slice of versions could be shared between the sender and the receiver. To stay safe, every receiver copies versions (`cloneVersions`) before storing them under its own lock, and no node holds its lock while it is sending.

**Deadlines with `context`.** Every `Send` gets a `context.WithTimeout`. Coordinators use `ReplicaTimeout` (computed from the network delays, about 17 ms on the default network) for replicas, clients use 3 times that for coordinators. When the deadline passes, `Send` returns `ErrTimeout`, and the coordinator counts that target as failed.
