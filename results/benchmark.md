# Benchmark results

5 nodes, N=3 replicas per key. One-way replica-to-replica latency 0.4ms + Exp(mean 0.8ms), 3% of messages +5ms; client-to-node 0.1ms + Exp(0.1ms). Replica timeout 20ms. 2s of load per row.

## E1. Quorum sweep, no failures (32 keys, 1 writer/key, 16 readers)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 0.64 | 5.25 | 0.54 | 4.64 | 31142 | 25.9% | 26.4% |
| 1,2 | false | 1.40 | 4.12 | 4.25 | 8.95 | 22558 | 0.4% | 0.4% |
| 2,1 | false | 3.49 | 7.90 | 0.59 | 3.91 | 8571 | 0.8% | 0.8% |
| 1,3 | true | 1.50 | 3.71 | 5.71 | 12.27 | 21093 | 0.0% | 0.0% |
| 3,1 | true | 5.15 | 12.23 | 0.62 | 3.65 | 5725 | 0.0% | 0.0% |
| 2,2 | true | 4.38 | 8.11 | 4.37 | 8.18 | 7114 | 0.0% | 0.0% |
| 2,3 | true | 4.40 | 7.81 | 5.71 | 12.26 | 7053 | 0.0% | 0.0% |
| 3,2 | true | 5.68 | 12.63 | 4.35 | 8.22 | 5203 | 0.0% | 0.0% |
| 3,3 | true | 5.71 | 12.18 | 5.69 | 12.36 | 5197 | 0.0% | 0.0% |

## E1b. Same sweep with one slow replica (n3: +3ms per message)

| R,W | R+W>N | get p50 ms | get p99 ms | put p50 ms | put p99 ms | reads | stale reads (read repair on) | stale reads (read repair off) |
|---|---|---|---|---|---|---|---|---|
| 1,1 | false | 1.33 | 9.42 | 0.99 | 8.54 | 13622 | 14.1% | 13.7% |
| 1,2 | false | 1.65 | 8.87 | 4.73 | 18.30 | 11774 | 0.2% | 0.2% |
| 2,1 | false | 4.83 | 18.77 | 1.42 | 8.34 | 4672 | 0.3% | 0.5% |
| 1,3 | true | 1.67 | 8.68 | 7.23 | 21.38 | 11926 | 0.0% | 0.0% |
| 3,1 | true | 9.51 | 21.43 | 1.43 | 8.31 | 3232 | 0.0% | 0.0% |
| 2,2 | true | 5.08 | 18.52 | 4.80 | 18.24 | 4572 | 0.0% | 0.0% |
| 2,3 | true | 5.11 | 18.67 | 7.06 | 21.59 | 4543 | 0.0% | 0.0% |
| 3,2 | true | 9.65 | 22.32 | 4.81 | 17.98 | 3195 | 0.0% | 0.0% |
| 3,3 | true | 9.81 | 21.80 | 7.22 | 21.21 | 3174 | 0.0% | 0.0% |

## E2. Concurrent read-modify-write on one key (8 writers)

| policy | clocks | R,W | get p50 ms | put p50 ms | reads | avg versions/read | reads with siblings | max siblings | acked items | acked items lost |
|---|---|---|---|---|---|---|---|---|---|---|
| siblings+client-reconcile | dotted version vector | 1,1 | 2.10 | 1.71 | 2551 | 5.36 | 99.0% | 10 | 2551 | 0 |
| siblings+client-reconcile | dotted version vector | 2,2 | 3.79 | 3.78 | 1827 | 4.66 | 97.6% | 8 | 1827 | 0 |
| siblings+client-reconcile | dotted version vector | 3,3 | 5.30 | 5.26 | 1327 | 3.69 | 92.2% | 8 | 1327 | 0 |
| siblings+client-reconcile | per-node vector clock | 1,1 | 1.61 | 1.29 | 3454 | 2.89 | 93.1% | 6 | 3454 | 670 |
| siblings+client-reconcile | per-node vector clock | 2,2 | 3.86 | 3.84 | 1884 | 2.49 | 89.8% | 5 | 1884 | 496 |
| siblings+client-reconcile | per-node vector clock | 3,3 | 5.41 | 5.47 | 1319 | 2.19 | 82.7% | 5 | 1319 | 337 |
| last-writer-wins | dotted version vector | 1,1 | 1.35 | 1.23 | 5377 | 1.00 | 0.0% | 1 | 5377 | 4553 |
| last-writer-wins | dotted version vector | 2,2 | 4.77 | 4.73 | 1694 | 1.00 | 0.0% | 1 | 1694 | 1303 |
| last-writer-wins | dotted version vector | 3,3 | 6.11 | 6.10 | 1240 | 0.99 | 0.0% | 1 | 1240 | 909 |

## E3. Partition {n1,n2} | {n3,n4,n5}, 6 clients per side, 64 keys

Side A is the 2-node minority, side B the 3-node majority. Availability = successful ops / attempted ops.

| quorum | R,W | A get avail | A put avail | B get avail | B put avail | A get p50 ms | B get p50 ms | stale reads A | stale reads B | keys with siblings after heal | acked writes missing after heal | replicas converged |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| strict | 1,1 | 83.8% | 83.7% | 100.0% | 100.0% | 1.47 | 1.48 | 97.5% | 83.4% | 54/64 | 0 | true |
| strict | 1,3 | 84.4% | 0.0% | 100.0% | 15.0% | 2.28 | 2.25 | 0.0% | 0.0% | 54/64 | 0 | true |
| strict | 2,2 | 40.6% | 38.9% | 59.0% | 58.1% | 5.26 | 5.03 | 0.0% | 0.0% | 53/64 | 0 | true |
| strict | 3,1 | 0.0% | 85.9% | 14.4% | 100.0% | - | 6.09 | - | 0.0% | 51/64 | 0 | true |
| strict | 3,3 | 0.0% | 0.0% | 14.0% | 13.8% | - | 6.13 | - | 0.0% | 49/64 | 0 | true |
| sloppy+hinted handoff | 1,1 | 100.0% | 100.0% | 100.0% | 100.0% | 1.53 | 1.53 | 97.4% | 96.1% | 64/64 | 0 | true |
| sloppy+hinted handoff | 1,3 | 100.0% | 0.0% | 100.0% | 100.0% | 1.66 | 1.80 | 93.8% | 0.0% | 64/64 | 0 | true |
| sloppy+hinted handoff | 2,2 | 100.0% | 100.0% | 100.0% | 100.0% | 5.05 | 4.76 | 92.7% | 87.5% | 64/64 | 0 | true |
| sloppy+hinted handoff | 3,1 | 0.0% | 100.0% | 99.9% | 100.0% | - | 6.13 | - | 66.7% | 60/64 | 0 | true |
| sloppy+hinted handoff | 3,3 | 0.0% | 0.0% | 100.0% | 100.0% | - | 6.10 | - | 0.0% | 55/64 | 0 | true |
