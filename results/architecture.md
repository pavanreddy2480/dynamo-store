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

## A2. What a coordinator replicates, and how versions are compared

8 clients do concurrent read-modify-write on one key for 2s, adding a unique item each time. "Lost" counts acknowledged items missing after a final union merge. Each row is the sum over 3 runs.

| clocks | replication payload | R,W | acked items | lost items |
|---|---|---|---|---|
| dotted version vector | whole sibling set | 1,1 | 8627 | 0 (0.00%) |
| dotted version vector | whole sibling set | 2,2 | 5369 | 0 (0.00%) |
| dotted version vector | new version only | 1,1 | 8847 | 1 (0.01%) |
| dotted version vector | new version only | 2,2 | 5330 | 3 (0.06%) |
| per-node vector clock | whole sibling set | 1,1 | 11132 | 2180 (19.58%) |
| per-node vector clock | whole sibling set | 2,2 | 5463 | 1366 (25.00%) |

## A3. Repair mechanisms after a partition heals

Sloppy R=1 W=2 writes on both sides of a {n1,n2} | {n3,n4,n5} partition for 1s, then heal. "Under-replicated" means an acknowledged write not yet on all 3 preferred replicas. "Divergent" means the 3 replicas of a key disagree.

| repair after heal | acked writes | under-replicated | divergent keys (of 64) |
|---|---|---|---|
| none | 1130 | 1059 (93.7%) | 57 |
| hinted handoff only | 1129 | 530 (46.9%) | 64 |
| read repair only (one R=3 read per key) | 1132 | 58 (5.1%) | 0 |
| anti-entropy only (one round) | 1114 | 58 (5.2%) | 0 |
| hinted handoff + anti-entropy (default) | 1107 | 0 (0.0%) | 0 |

## A4. Choosing the coordinator

Same workload as E1 (32 keys, one writer each, 16 readers), R=1 W=1, for 2s. Load share is each node's fraction of coordinated requests; perfect balance is 20%.

| coordinator | stale reads | coordinator load share n1..n5 | busiest node |
|---|---|---|---|
| always first preferred replica | 0.1% | 22% / 12% / 19% / 31% / 15% | 31% |
| random preferred replica (default) | 25.7% | 19% / 23% / 17% / 20% / 22% | 23% |

## A5. Replica timeout

How long a coordinator waits for replicas. Strict R=W=2, 64 keys, partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 1s. A failed operation costs the full timeout.

| replica timeout | no partition: put p99 ms | no partition: put failures | partitioned: failed-op latency p50 ms | partitioned: put availability |
|---|---|---|---|---|
| 2ms | 6.05 | 84.59% | 3.87 | 10.0% |
| 5ms | 12.77 | 13.99% | 6.84 | 45.3% |
| 20ms | 14.12 | 0.00% | 22.40 | 57.2% |
| 100ms | 10.76 | 0.00% | 102.71 | 59.9% |
