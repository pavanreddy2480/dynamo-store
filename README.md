# dynamo-store

A Dynamo-style replicated key-value store written in Go, run as a simulation inside one process so that network failures can be created on purpose and measured. Every design choice has an experiment behind it.

## What it does

* Keeps **3 copies of every key** on a 5-node ring of **256 equal fixed partitions**.
* Lets each client choose its own **read and write quorums (R, W)**. Strict quorums are the default; sloppy quorums with hinted handoff are opt-in.
* Tracks concurrent writes with **dotted version vectors** and returns the conflicting **siblings** to the client, which merges them (set union), or uses last-writer-wins for comparison.
* Repairs copies that drifted apart with **read repair**, **hinted handoff** and **Merkle-tree anti-entropy** that runs by itself every 100 ms.
* Runs on an **in-memory network** with latency, slow nodes and partitions.

## Quick start

Requires Go 1.22 or newer. No external dependencies.

```sh
go test -race ./...                # correctness tests (about 15 s)
go run ./cmd/demo                  # partition, write on both sides, heal, verify the rule
```

```go
c := store.NewCluster(store.DefaultConfig())
cl := c.NewClient("alice", 2, 2)            // R=2, W=2
cl.Sloppy = true                            // optional: sloppy quorum

res, _ := cl.Get("cart")                    // res.Versions may hold several siblings
v := store.UnionResolver(res.Values())      // the declared merge rule
cl.Put("cart", v+",eggs", res.Context)      // context supersedes every sibling read
```

## Where to read first

| If you want to... | Read |
|---|---|
| understand the whole system from zero | [docs/guides/ARCHITECTURE.md](docs/guides/ARCHITECTURE.md) |
| see every message between nodes, with real examples | [docs/guides/MESSAGES.md](docs/guides/MESSAGES.md) |
| know why each choice was made, and what each test showed | [docs/guides/ABLATION.md](docs/guides/ABLATION.md) |
| look up a numbered design decision (D1 to D15) | [docs/reference/ARCHITECTURE_DECISIONS.md](docs/reference/ARCHITECTURE_DECISIONS.md) |
| see the results of the original three experiments | [docs/reference/REPORT.md](docs/reference/REPORT.md) |
| see which decisions have proof and which do not | [docs/reference/DECISIONS_AUDIT.md](docs/reference/DECISIONS_AUDIT.md) |

The three guides in `docs/guides/` are written in plain language, with a story, then the code, then the measurements, for each topic.

## Repository layout

```
dynamo-store/
├── README.md
├── go.mod
├── vclock/             vector clocks: compare, merge, increment
├── ring/               consistent hashing: fixed partitions (and random vnodes), preference lists
├── network/            simulated network: latency model, partitions, slow nodes
├── merkle/             the hash tree kept for each partition
├── store/              the replicated store itself
│   ├── doc.go            package overview
│   ├── version.go        versions, dots, conflict policies, clock modes
│   ├── merge.go          ordering rules: supersedes, sibling merge, last-writer-wins
│   ├── messages.go       request and response messages
│   ├── node.go           replica state, request dispatch, background loops
│   ├── coordinator.go    quorum reads and writes, sloppy fallbacks, read repair
│   ├── repair.go         hinted handoff
│   ├── antientropy.go    Merkle (and full-state) anti-entropy exchange
│   ├── client.go         Client: R and W, Get, Put, GetResolved
│   ├── resolver.go       UnionResolver, the declared merge rule
│   └── cluster.go        cluster setup, default configuration, replica timeout, AddNode
├── scenario/           the partition-and-reconcile scenario used by the demo and tests
├── cmd/                the programs
│   ├── demo/             the partition story
│   ├── bench/            experiments E1 to E3, and X1 to X11 with -ablate
│   ├── archbench/        experiments A1 to A10 behind the architecture decisions
│   └── plots/            redraws the charts from the result tables
├── chart/              small SVG chart library used by the programs
├── results/            generated output
│   ├── benchmark.md      E1 to E3
│   ├── architecture.md   A1 to A10
│   ├── ablations.md      X1 to X11
│   ├── demo.txt          output of the demo
│   └── charts/           one SVG per experiment
└── docs/
    ├── guides/           beginner-friendly: ARCHITECTURE, MESSAGES, ABLATION
    ├── reference/        ARCHITECTURE_DECISIONS, REPORT, DECISIONS_AUDIT
    └── experiments/      write-ups of single experiments and the chart index
```

## Experiments

| Command | Experiments | Writes |
|---|---|---|
| `go run ./cmd/bench` | **E1 to E3**: quorum sweep, concurrent updates, partitions (about 2 min) | `results/benchmark.md` |
| `go run ./cmd/archbench` | **A1 to A10**: hash, vnodes, clocks, repair, coordinator, timeout, Merkle, fixed partitions | `results/architecture.md` |
| `go run ./cmd/bench -ablate` | **X1 to X11**: noise, network speed, anti-entropy interval, workload, handoff, read path, failures | `results/ablations.md` |
| `go run ./cmd/plots` | redraws the charts for E1 to E3 and A1 to A5 | `results/charts/` |

Add `-only A6` (archbench) or `-only X3` (bench with `-ablate`) to run one experiment. See [docs/experiments/GRAPHS.md](docs/experiments/GRAPHS.md) for which chart belongs to which experiment.

## Headline results

All on the simulated network; compare numbers with each other, not with a real data centre.

| Question | Answer |
|---|---|
| Stale reads at R=W=1 | about 26% on the default network (8% LAN, 41% cross-region); **0% whenever R + W > N** |
| Lost writes under concurrent updates | dotted version vectors **0%**, plain per-node clocks about 20%, last-writer-wins about 85% |
| Merkle vs sending everything (1% of keys differ) | **832 keys** sent instead of 120,000 (97% fewer bytes) |
| Fixed partitions vs 64 random vnodes | spread about **1 point** vs 10; a joining node moves 19.9% of copies (ideal 20%) |
| Failures | no acknowledged write was lost in any partition shape tested |

## Branches

| Branch | What it holds |
|---|---|
| `main` | the stable store and its first experiments |
| `organize-store` | `ablations` plus the package split (ring, network, merkle, scenario), `Cluster.AddNode`, and experiments X9 to X11 |
| `ablations` | everything above: fixed partitions, Merkle anti-entropy, background repair, computed timeout, guides, ablations X1 to X11 |
| `adaptive-defaults` | the same, up to the guides |
| `fixed-merkle`, `merkle-anti-entropy`, `merkle-lazy`, `fixed-partitions` | the individual design variants, kept for comparison |
| `real-network-and-wal` | each node as its own process over TCP in Docker, plus a write-ahead log for crash recovery |
