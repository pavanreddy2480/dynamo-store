# Architecture experiments

Default cluster unless stated: 5 nodes, N=3, replica-to-replica latency 0.4ms + Exp(0.8ms) with 3% of messages +5ms, client hop 0.1ms + Exp(0.1ms), replica timeout 20ms. Regenerate with `go run ./cmd/archbench`.

## A1. Ring hash function and virtual nodes

100,000 keys, 5 nodes, N=3. Each node would hold 60% of the keys if perfectly balanced. Max/min is the most-loaded node's share over the least-loaded node's share.

| hash | vnodes/node | min share | max share | max/min | coefficient of variation |
|---|---|---|---|---|---|
| FNV-1a | 1 | 10.4% | 93.6% | 8.98 | 0.647 |
| FNV-1a | 8 | 10.4% | 93.6% | 8.98 | 0.647 |
| FNV-1a | 64 | 27.9% | 100.0% | 3.58 | 0.401 |
| FNV-1a | 256 | 32.5% | 89.6% | 2.76 | 0.367 |
| MD5 | 1 | 42.1% | 75.3% | 1.79 | 0.217 |
| MD5 | 8 | 50.0% | 69.1% | 1.38 | 0.116 |
| MD5 | 64 | 56.7% | 66.3% | 1.17 | 0.056 |
| MD5 | 256 | 57.8% | 62.4% | 1.08 | 0.031 |

## A1b. Ablation: how many virtual nodes per node?

MD5 ring, 5 nodes, N=3, 20000 keys, averaged over 20 placements (each placement uses different node names, so tokens land in different places). Spread = most-loaded minus least-loaded node's share of replica copies, in points (ideal 0; every node should hold 60%). Moved = replica copies a node must receive when a 5th node joins a 4-node ring (ideal 20%; leaving is the same pair of rings read backwards). "Worst" is the worst of the 20 placements. Lookup is `PreferenceList` on one core (fastest of 5 runs).

| vnodes/node | tokens | mean CV | mean spread | worst spread | mean moved on join | worst moved | lookup ns | build µs |
|---|---|---|---|---|---|---|---|---|
| 1 | 5 | 0.314 | 48.9 | 92.7 | 21.7% | 31.5% | 398 | 7 |
| 2 | 10 | 0.291 | 48.1 | 84.2 | 22.2% | 30.6% | 515 | 5 |
| 4 | 20 | 0.242 | 40.4 | 73.3 | 20.9% | 30.5% | 527 | 13 |
| 8 | 40 | 0.191 | 31.3 | 52.3 | 20.3% | 25.7% | 564 | 20 |
| 16 | 80 | 0.112 | 18.1 | 36.2 | 20.0% | 24.1% | 666 | 35 |
| 32 | 160 | 0.095 | 15.4 | 29.4 | 19.6% | 22.7% | 659 | 60 |
| 64 | 320 | 0.064 | 10.6 | 22.0 | 19.9% | 23.0% | 692 | 118 |
| 128 | 640 | 0.043 | 7.1 | 13.9 | 20.1% | 21.7% | 761 | 241 |
| 256 | 1280 | 0.035 | 5.9 | 11.5 | 20.0% | 21.4% | 905 | 554 |
| 512 | 2560 | 0.025 | 4.1 | 6.9 | 20.0% | 20.9% | 903 | 1200 |
| 1024 | 5120 | 0.016 | 2.6 | 4.5 | 20.2% | 20.6% | 952 | 2470 |

![balance](a1b_balance.svg)
![moved](a1b_moved.svg)
![lookup](a1b_lookup.svg)

## A2. What a coordinator replicates, and how versions are compared

8 clients do concurrent read-modify-write on one key for 2s, adding a unique item each time. "Lost" counts acknowledged items missing after a final union merge. Each row is the sum over 3 runs.

| clocks | replication payload | R,W | acked items | lost items |
|---|---|---|---|---|
| dotted version vector | whole sibling set | 1,1 | 9476 | 0 (0.00%) |
| dotted version vector | whole sibling set | 2,2 | 5480 | 0 (0.00%) |
| dotted version vector | new version only | 1,1 | 9662 | 0 (0.00%) |
| dotted version vector | new version only | 2,2 | 5522 | 5 (0.09%) |
| per-node vector clock | whole sibling set | 1,1 | 11741 | 2303 (19.62%) |
| per-node vector clock | whole sibling set | 2,2 | 5563 | 1386 (24.91%) |

## A3. Repair mechanisms after a partition heals

Sloppy R=1 W=2 writes on both sides of a {n1,n2} | {n3,n4,n5} partition for 1s, then heal. "Under-replicated" means an acknowledged write not yet on all 3 preferred replicas. "Divergent" means the 3 replicas of a key disagree.

| repair after heal | acked writes | under-replicated | divergent keys (of 64) |
|---|---|---|---|
| none | 1130 | 1060 (93.8%) | 57 |
| hinted handoff only | 1118 | 530 (47.4%) | 64 |
| read repair only (one R=3 read per key) | 1133 | 58 (5.1%) | 0 |
| anti-entropy only (one round) | 1137 | 58 (5.1%) | 0 |
| hinted handoff + anti-entropy (default) | 1135 | 0 (0.0%) | 0 |

## A4. Choosing the coordinator

Same workload as E1 (32 keys, one writer each, 16 readers), R=1 W=1, for 2s. Load share is each node's fraction of coordinated requests; perfect balance is 20%.

| coordinator | stale reads | coordinator load share n1..n5 | busiest node |
|---|---|---|---|
| always first preferred replica | 0.0% | 22% / 12% / 19% / 31% / 16% | 31% |
| random preferred replica (default) | 25.5% | 19% / 23% / 17% / 20% / 22% | 23% |

## A5. Replica timeout

How long a coordinator waits for replicas. Strict R=W=2, 64 keys, partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 1s. A failed operation costs the full timeout.

| replica timeout | no partition: put p99 ms | no partition: put failures | partitioned: failed-op latency p50 ms | partitioned: put availability |
|---|---|---|---|---|
| 2ms | 5.45 | 82.87% | 3.70 | 10.4% |
| 5ms | 10.05 | 9.01% | 6.95 | 45.8% |
| 20ms | 11.24 | 0.00% | 22.31 | 56.9% |
| 100ms | 11.95 | 0.00% | 102.68 | 60.3% |
