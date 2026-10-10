# Benchmark results

5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. 2s of load per row.

## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 0.69 | 6.16 | 0.67 | 5.75 | 28637 | 21.5% | 21.8% |
| 1,2 | false | 1.04 | 4.92 | 3.92 | 9.94 | 23393 | 0.4% | 0.6% |
| 2,1 | false | 3.29 | 9.49 | 0.68 | 5.42 | 8157 | 0.8% | 0.8% |
| 1,3 | true | 1.25 | 4.40 | 5.41 | 12.58 | 23713 | 0.0% | 0.0% |
| 3,1 | true | 5.31 | 12.76 | 0.74 | 4.68 | 5533 | 0.0% | 0.0% |
| 2,2 | true | 4.29 | 7.81 | 4.29 | 7.88 | 7317 | 0.0% | 0.0% |
| 2,3 | true | 4.24 | 7.84 | 5.59 | 12.46 | 7379 | 0.0% | 0.0% |
| 3,2 | true | 5.74 | 12.80 | 4.31 | 8.58 | 5190 | 0.0% | 0.0% |
| 3,3 | true | 5.66 | 12.17 | 5.65 | 12.32 | 5261 | 0.0% | 0.0% |

## E1b. Same sweep with one slow replica (n3: +3ms per message)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 1.28 | 10.16 | 1.11 | 9.28 | 13438 | 11.1% | 10.6% |
| 1,2 | false | 1.59 | 8.89 | 4.59 | 18.29 | 12806 | 0.2% | 0.3% |
| 2,1 | false | 4.67 | 19.06 | 1.37 | 9.01 | 4893 | 0.3% | 0.3% |
| 1,3 | true | 1.61 | 8.81 | 7.06 | 21.59 | 12497 | 0.0% | 0.0% |
| 3,1 | true | 9.02 | 22.50 | 1.38 | 8.62 | 3305 | 0.0% | 0.0% |
| 2,2 | true | 4.91 | 18.39 | 4.62 | 18.03 | 4859 | 0.0% | 0.0% |
| 2,3 | true | 4.95 | 18.36 | 6.88 | 21.49 | 4785 | 0.0% | 0.0% |
| 3,2 | true | 9.33 | 23.39 | 4.66 | 18.65 | 3127 | 0.0% | 0.0% |
| 3,3 | true | 9.33 | 22.05 | 6.93 | 21.30 | 3274 | 0.0% | 0.0% |

## E2. Concurrent read-modify-write on one key (8 writers)

| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |
|---|---|---|---|---|---|---|---|---|---|---|
| siblings+client-reconcile | dotted version vector | 1,1 | 2.15 | 1.68 | 2528 | 5.65 | 99.2% | 11 | 2528 | 0 |
| siblings+client-reconcile | dotted version vector | 2,2 | 4.08 | 3.94 | 1727 | 4.51 | 97.7% | 8 | 1727 | 0 |
| siblings+client-reconcile | dotted version vector | 3,3 | 5.46 | 5.44 | 1283 | 3.58 | 91.3% | 8 | 1283 | 0 |
| siblings+client-reconcile | per-node vector clock | 1,1 | 1.72 | 1.46 | 3055 | 2.94 | 94.2% | 6 | 3055 | 585 |
| siblings+client-reconcile | per-node vector clock | 2,2 | 4.02 | 3.99 | 1803 | 2.44 | 89.4% | 5 | 1803 | 508 |
| siblings+client-reconcile | per-node vector clock | 3,3 | 5.49 | 5.48 | 1302 | 2.28 | 84.7% | 4 | 1302 | 339 |
| last-writer-wins | dotted version vector | 1,1 | 1.27 | 1.23 | 5426 | 1.00 | 0.0% | 1 | 5426 | 4572 |
| last-writer-wins | dotted version vector | 2,2 | 4.59 | 4.63 | 1726 | 1.00 | 0.0% | 1 | 1726 | 1318 |
| last-writer-wins | dotted version vector | 3,3 | 6.12 | 6.05 | 1228 | 0.99 | 0.0% | 1 | 1228 | 903 |

## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys

Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.

| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| strict | 1,1 | 88.8% | 88.7% | 100.0% | 100.0% | 1.48 | 1.45 | 97.4% | 85.8% | 57/64 | 0 | true |
| strict | 1,3 | 88.3% | 0.0% | 100.0% | 12.7% | 2.21 | 2.25 | 0.0% | 0.0% | 57/64 | 0 | true |
| strict | 2,2 | 27.8% | 28.4% | 71.8% | 73.8% | 5.11 | 5.12 | 0.0% | 0.0% | 54/64 | 0 | true |
| strict | 3,1 | 0.0% | 86.8% | 11.2% | 100.0% | - | 6.15 | - | 0.0% | 54/64 | 0 | true |
| strict | 3,3 | 0.0% | 0.0% | 11.6% | 13.0% | - | 5.90 | - | 0.0% | 51/64 | 0 | true |
| sloppy+hinted handoff | 1,1 | 100.0% | 100.0% | 100.0% | 100.0% | 1.51 | 1.49 | 97.4% | 96.4% | 64/64 | 0 | true |
| sloppy+hinted handoff | 1,3 | 100.0% | 0.0% | 100.0% | 100.0% | 1.66 | 1.71 | 93.6% | 0.0% | 64/64 | 0 | true |
| sloppy+hinted handoff | 2,2 | 99.7% | 99.8% | 99.4% | 99.9% | 5.08 | 4.81 | 92.1% | 86.9% | 64/64 | 0 | true |
| sloppy+hinted handoff | 3,1 | 0.0% | 100.0% | 100.0% | 100.0% | - | 6.13 | - | 66.6% | 60/64 | 0 | true |
| sloppy+hinted handoff | 3,3 | 0.0% | 0.0% | 100.0% | 100.0% | - | 6.17 | - | 0.0% | 55/64 | 0 | true |
