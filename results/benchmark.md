# Benchmark results

5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. 2s of load per row.

## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 0.66 | 5.96 | 0.55 | 5.29 | 28617 | 25.5% | 26.6% |
| 1,2 | false | 1.39 | 3.89 | 4.12 | 8.28 | 22829 | 0.3% | 0.5% |
| 2,1 | false | 3.60 | 8.35 | 0.62 | 3.81 | 8366 | 1.0% | 1.0% |
| 1,3 | true | 1.51 | 3.72 | 5.69 | 12.31 | 20948 | 0.0% | 0.0% |
| 3,1 | true | 5.17 | 12.24 | 0.60 | 3.77 | 5745 | 0.0% | 0.0% |
| 2,2 | true | 4.40 | 8.60 | 4.41 | 8.40 | 7013 | 0.0% | 0.0% |
| 2,3 | true | 4.41 | 8.66 | 5.73 | 12.72 | 6997 | 0.0% | 0.0% |
| 3,2 | true | 5.72 | 12.37 | 4.36 | 8.09 | 5206 | 0.0% | 0.0% |
| 3,3 | true | 5.85 | 12.59 | 5.82 | 12.78 | 5062 | 0.0% | 0.0% |

## E1b. Same sweep with one slow replica (n3: +3ms per message)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 1.33 | 9.51 | 0.97 | 8.64 | 13620 | 13.7% | 13.1% |
| 1,2 | false | 1.67 | 8.92 | 4.74 | 18.47 | 11736 | 0.1% | 0.2% |
| 2,1 | false | 4.80 | 18.79 | 1.38 | 8.35 | 4708 | 0.4% | 0.4% |
| 1,3 | true | 1.67 | 8.79 | 7.21 | 21.81 | 11787 | 0.0% | 0.0% |
| 3,1 | true | 9.43 | 22.56 | 1.35 | 8.49 | 3223 | 0.0% | 0.0% |
| 2,2 | true | 5.18 | 18.69 | 4.89 | 18.16 | 4572 | 0.0% | 0.0% |
| 2,3 | true | 5.12 | 18.43 | 7.25 | 21.29 | 4599 | 0.0% | 0.0% |
| 3,2 | true | 9.70 | 21.90 | 4.85 | 18.00 | 3187 | 0.0% | 0.0% |
| 3,3 | true | 9.82 | 22.26 | 7.28 | 21.45 | 3124 | 0.0% | 0.0% |

## E2. Concurrent read-modify-write on one key (8 writers)

| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |
|---|---|---|---|---|---|---|---|---|---|---|
| siblings+client-reconcile | dotted version vector | 1,1 | 2.11 | 1.75 | 2457 | 5.43 | 99.3% | 10 | 2457 | 0 |
| siblings+client-reconcile | dotted version vector | 2,2 | 3.73 | 3.80 | 1855 | 4.44 | 97.8% | 8 | 1855 | 0 |
| siblings+client-reconcile | dotted version vector | 3,3 | 5.25 | 5.31 | 1328 | 3.55 | 93.2% | 8 | 1328 | 0 |
| siblings+client-reconcile | per-node vector clock | 1,1 | 1.63 | 1.33 | 3278 | 2.86 | 92.2% | 6 | 3278 | 665 |
| siblings+client-reconcile | per-node vector clock | 2,2 | 3.99 | 3.99 | 1806 | 2.41 | 89.4% | 5 | 1806 | 490 |
| siblings+client-reconcile | per-node vector clock | 3,3 | 5.45 | 5.63 | 1284 | 2.26 | 83.7% | 4 | 1283 | 303 |
| last-writer-wins | dotted version vector | 1,1 | 1.38 | 1.28 | 5196 | 1.00 | 0.0% | 1 | 5196 | 4397 |
| last-writer-wins | dotted version vector | 2,2 | 4.61 | 4.61 | 1714 | 1.00 | 0.0% | 1 | 1713 | 1304 |
| last-writer-wins | dotted version vector | 3,3 | 6.04 | 6.10 | 1238 | 0.99 | 0.0% | 1 | 1238 | 916 |

## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys

Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.

| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| strict | 1,1 | 83.9% | 84.0% | 100.0% | 100.0% | 1.52 | 1.50 | 97.4% | 83.3% | 56/64 | 0 | true |
| strict | 1,3 | 84.4% | 0.0% | 100.0% | 15.0% | 2.27 | 2.26 | 0.0% | 0.0% | 54/64 | 0 | true |
| strict | 2,2 | 40.7% | 39.1% | 58.8% | 57.8% | 5.22 | 5.07 | 0.0% | 0.0% | 53/64 | 0 | true |
| strict | 3,1 | 0.0% | 85.7% | 14.5% | 100.0% | - | 6.21 | - | 0.0% | 52/64 | 0 | true |
| strict | 3,3 | 0.0% | 0.0% | 14.1% | 13.8% | - | 6.09 | - | 0.0% | 50/64 | 0 | true |
| sloppy+hinted handoff | 1,1 | 100.0% | 100.0% | 100.0% | 100.0% | 1.54 | 1.55 | 97.3% | 96.2% | 64/64 | 0 | true |
| sloppy+hinted handoff | 1,3 | 100.0% | 0.0% | 100.0% | 100.0% | 2.02 | 2.10 | 93.9% | 0.0% | 64/64 | 0 | true |
| sloppy+hinted handoff | 2,2 | 100.0% | 100.0% | 100.0% | 100.0% | 5.16 | 4.81 | 92.6% | 87.6% | 64/64 | 0 | true |
| sloppy+hinted handoff | 3,1 | 0.0% | 100.0% | 100.0% | 100.0% | - | 6.19 | - | 66.3% | 60/64 | 0 | true |
| sloppy+hinted handoff | 3,3 | 0.0% | 0.0% | 100.0% | 100.0% | - | 6.18 | - | 0.0% | 55/64 | 0 | true |
