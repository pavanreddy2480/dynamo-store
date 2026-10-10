# dynamo-store

A Dynamo-inspired replicated key-value store in Go, simulated in one process, with:

* 3 replicas per key on a consistent-hash ring (5 nodes)
* per-client read and write quorums (R, W), strict or sloppy with hinted handoff
* dotted version vectors to detect concurrent writes; siblings are returned to the client
* explicit conflict resolution: client-side set-union reconciliation, or last-writer-wins for comparison
* an in-memory network with latency, slow nodes and partitions
* read repair and anti-entropy

See [REPORT.md](REPORT.md) for the results and [ARCHITECTURE.md](ARCHITECTURE.md) for the design and the reasons behind each decision.

## Run

```sh
go test -race ./...               # unit + scenario tests
go run ./cmd/demo                 # partition -> update both sides -> heal -> verify rule
go run ./cmd/bench -duration 2s   # writes results/benchmark.md (about 2 minutes)
go run ./cmd/archbench            # writes results/architecture.md (decision experiments A1–A5)
```

Requires Go 1.22+ and has no external dependencies.

## Layout

| Path | What |
|---|---|
| `vclock/` | vector clocks: compare, merge, increment |
| `store/version.go` | versions, dots, sibling merge, LWW, clock modes |
| `store/network.go` | simulated network: latency model, partitions, slow nodes |
| `store/ring.go` | consistent hashing and preference lists |
| `store/node.go` | replica and coordinator logic, quorums, hinted handoff, read repair, anti-entropy |
| `store/cluster.go` | cluster setup and the `Client` (R/W settings, `Get`, `Put`, `GetResolved`) |
| `store/scenario.go` | the partition and reconcile scenario used by the demo and tests |
| `cmd/bench` | experiments E1–E3 (quorum sweep, concurrent updates, partition availability) |
| `cmd/archbench` | experiments A1–A5 behind the architecture decisions |

## API sketch

```go
c := store.NewCluster(store.DefaultConfig())
cl := c.NewClient("alice", 2, 2)            // R=2, W=2
cl.Sloppy = true                            // optional: sloppy quorum

res, _ := cl.Get("cart")                    // res.Versions may hold several siblings
v := store.UnionResolver(res.Values())      // declared rule
cl.Put("cart", v+",eggs", res.Context)      // context supersedes all siblings read
```
