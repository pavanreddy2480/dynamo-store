# dynamo-store

A Dynamo-inspired replicated key-value store in Go, simulated in one process, with:

* 3 replicas per key on a consistent-hash ring (5 nodes)
* per-client read and write quorums (R, W), strict or sloppy with hinted handoff
* dotted version vectors to detect concurrent writes; siblings are returned to the client
* explicit conflict resolution: client-side set-union reconciliation, or last-writer-wins for comparison
* an in-memory network with latency, slow nodes and partitions
* read repair and anti-entropy
* a write-ahead log (fsync with group commit, async, or none) so nodes survive crashes
* a real deployment mode: each node is its own process in a Docker container talking TCP, partitioned with iptables and crashed with `kill -9`

See [REPORT.md](REPORT.md) for the results and [ARCHITECTURE.md](ARCHITECTURE.md) for the design and the reasons behind each decision.

## Run

```sh
go test -race ./...               # unit + scenario tests
go run ./cmd/demo                 # partition -> update both sides -> heal -> verify rule
go run ./cmd/bench -duration 2s   # writes results/benchmark.md (about 2 minutes)
go run ./cmd/archbench            # writes results/architecture.md (decision experiments A1–A5)
```

Requires Go 1.22+ and has no external dependencies.

### Real processes in Docker

```sh
CGO_ENABLED=0 go build -o bin/ ./cmd/dynode ./cmd/dyload
sudo go run ./cmd/realbench -duration 4s   # writes results/realnet.md (about 6 minutes)
```

`realbench` builds the image from `deploy/Dockerfile`, creates the `dynstore` bridge network (172.28.0.0/24), starts n1–n5 with a volume each, and drives the experiments. Partitions are iptables rules in a `DYNSTORE` chain hooked into `DOCKER-USER`, so it needs a Linux host with Docker and root. On Docker Desktop (macOS/Windows) the host iptables is not the VM's, so run it inside a Linux VM.

To run a cluster by hand:

```sh
docker network create --subnet 172.28.0.0/24 dynstore
PEERS=n1=172.28.0.11:7000,n2=172.28.0.12:7000,n3=172.28.0.13:7000,n4=172.28.0.14:7000,n5=172.28.0.15:7000
for i in 1 2 3 4 5; do
  docker run -d --name n$i --network dynstore --ip 172.28.0.1$i -v dyn-n$i:/data \
    dynamo-store:dev -id n$i -peers $PEERS -wal fsync
done
docker run --rm --network dynstore --entrypoint /dyload dynamo-store:dev \
  run -peers $PEERS -r 2 -w 2 -duration 5s -out /dev/stdout
```

## Layout

| Path | What |
|---|---|
| `vclock/` | vector clocks: compare, merge, increment |
| `store/version.go` | versions, dots, sibling merge, LWW, clock modes |
| `store/network.go` | simulated network: latency model, partitions, slow nodes |
| `store/ring.go` | consistent hashing and preference lists |
| `store/node.go` | replica and coordinator logic, quorums, hinted handoff, read repair, anti-entropy |
| `store/cluster.go` | cluster setup and the `Client` (R/W settings, `Get`, `Put`, `GetResolved`), crash/restart |
| `store/wal.go` | write-ahead log: CRC-framed records, group-commit fsync, replay, checkpoint |
| `store/tcp.go` | TCP transport: gob framing, connection pool, heartbeat failure detector |
| `cmd/dynode` | one storage node as a process |
| `cmd/dyload` | load generator / admin client for real clusters (`run`, `dump`, `verify`) |
| `cmd/realbench` | Docker + iptables experiments R1–R4 on real processes |
| `deploy/Dockerfile` | `FROM scratch` image with the two static binaries |
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
