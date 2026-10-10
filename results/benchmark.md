# Benchmark results

5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. 2s of load per row.

## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 0.69 | 5.99 | 0.56 | 5.52 | 28901 | 25.4% | 26.3% |
| 1,2 | false | 1.35 | 4.69 | 4.21 | 9.66 | 22034 | 0.4% | 0.6% |
| 2,1 | false | 3.53 | 8.99 | 0.61 | 4.68 | 8024 | 0.9% | 1.0% |
| 1,3 | true | 1.46 | 4.03 | 5.68 | 12.48 | 21336 | 0.0% | 0.0% |
| 3,1 | true | 5.36 | 12.80 | 0.62 | 4.60 | 5519 | 0.0% | 0.0% |
| 2,2 | true | 4.37 | 8.29 | 4.35 | 8.28 | 7107 | 0.0% | 0.0% |
| 2,3 | true | 4.32 | 7.89 | 5.63 | 12.24 | 7175 | 0.0% | 0.0% |
| 3,2 | true | 5.69 | 12.66 | 4.30 | 8.19 | 5186 | 0.0% | 0.0% |
| 3,3 | true | 5.71 | 12.52 | 5.72 | 12.69 | 5102 | 0.0% | 0.0% |

## E1b. Same sweep with one slow replica (n3: +3ms per message)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 1.03 | 10.37 | 0.76 | 9.22 | 13853 | 14.7% | 13.0% |
| 1,2 | false | 1.62 | 8.99 | 4.67 | 18.15 | 12536 | 0.2% | 0.2% |
| 2,1 | false | 4.70 | 18.86 | 1.38 | 8.49 | 4914 | 0.5% | 0.7% |
| 1,3 | true | 1.67 | 8.62 | 7.02 | 21.02 | 12231 | 0.0% | 0.0% |
| 3,1 | true | 9.10 | 21.95 | 1.36 | 8.50 | 3317 | 0.0% | 0.0% |
| 2,2 | true | 5.05 | 18.37 | 4.74 | 18.06 | 4700 | 0.0% | 0.0% |
| 2,3 | true | 5.05 | 18.18 | 6.92 | 21.09 | 4716 | 0.0% | 0.0% |
| 3,2 | true | 9.36 | 22.04 | 4.78 | 17.95 | 3269 | 0.0% | 0.0% |
| 3,3 | true | 9.32 | 22.15 | 6.88 | 21.32 | 3298 | 0.0% | 0.0% |

## E2. Concurrent read-modify-write on one key (8 writers)

| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |
|---|---|---|---|---|---|---|---|---|---|---|
| siblings+client-reconcile | dotted version vector | 1,1 | 2.00 | 1.44 | 2693 | 5.76 | 99.5% | 11 | 2693 | 0 |
| siblings+client-reconcile | dotted version vector | 2,2 | 4.00 | 3.93 | 1729 | 4.84 | 97.2% | 8 | 1729 | 0 |
| siblings+client-reconcile | dotted version vector | 3,3 | 5.66 | 5.47 | 1267 | 3.66 | 94.7% | 8 | 1267 | 0 |
| siblings+client-reconcile | per-node vector clock | 1,1 | 1.68 | 1.22 | 3216 | 2.97 | 93.4% | 7 | 3216 | 653 |
| siblings+client-reconcile | per-node vector clock | 2,2 | 4.07 | 4.00 | 1784 | 2.46 | 89.7% | 5 | 1784 | 492 |
| siblings+client-reconcile | per-node vector clock | 3,3 | 5.68 | 5.50 | 1287 | 2.23 | 81.1% | 4 | 1287 | 328 |
| last-writer-wins | dotted version vector | 1,1 | 1.31 | 1.21 | 5453 | 1.00 | 0.0% | 1 | 5453 | 4621 |
| last-writer-wins | dotted version vector | 2,2 | 4.66 | 4.66 | 1724 | 1.00 | 0.0% | 1 | 1724 | 1321 |
| last-writer-wins | dotted version vector | 3,3 | 6.16 | 6.05 | 1236 | 0.99 | 0.0% | 1 | 1236 | 917 |

## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys

Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.

| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| strict | 1,1 | 88.9% | 88.8% | 100.0% | 100.0% | 1.47 | 1.48 | 97.4% | 85.8% | 57/64 | 0 | true |
| strict | 1,3 | 88.3% | 0.0% | 100.0% | 12.7% | 2.24 | 2.21 | 0.0% | 0.0% | 57/64 | 0 | true |
| strict | 2,2 | 27.7% | 28.3% | 71.5% | 73.5% | 5.04 | 5.04 | 0.0% | 0.0% | 54/64 | 0 | true |
| strict | 3,1 | 0.0% | 86.8% | 11.4% | 100.0% | - | 6.11 | - | 0.0% | 54/64 | 0 | true |
| strict | 3,3 | 0.0% | 0.0% | 11.4% | 13.0% | - | 6.29 | - | 0.0% | 51/64 | 0 | true |
| sloppy+hinted handoff | 1,1 | 100.0% | 100.0% | 100.0% | 100.0% | 1.53 | 1.53 | 97.3% | 96.2% | 64/64 | 0 | true |
| sloppy+hinted handoff | 1,3 | 100.0% | 0.0% | 100.0% | 100.0% | 1.64 | 1.84 | 93.7% | 0.0% | 64/64 | 0 | true |
| sloppy+hinted handoff | 2,2 | 100.0% | 100.0% | 100.0% | 100.0% | 5.09 | 4.78 | 92.4% | 87.7% | 64/64 | 0 | true |
| sloppy+hinted handoff | 3,1 | 0.0% | 100.0% | 100.0% | 100.0% | - | 6.18 | - | 66.7% | 60/64 | 0 | true |
| sloppy+hinted handoff | 3,3 | 0.0% | 0.0% | 100.0% | 100.0% | - | 6.14 | - | 0.0% | 55/64 | 0 | true |
