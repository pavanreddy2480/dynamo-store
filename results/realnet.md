# Real-network results

Each node is a separate `dynode` process in its own Docker container (image built `FROM scratch` with static Go binaries), talking gob over TCP on a Docker bridge network. Partitions are iptables DROP rules on the bridge, crashes are SIGKILL. Host: Linux 6.18.44-fc-v114, 4 CPUs shared by all containers. Regenerate with `go run ./cmd/realbench` (Linux, root).

## R1. Quorum sweep on real processes (no failures)

One load generator container, 8 clients, 64 keys, every read followed by a write half the time, 4s per row, WAL in fsync mode. "LAN" is the plain Docker bridge. "Cross-zone" adds an emulated 0.4ms + Exp(0.8ms) delay to each node-to-node message inside the transport, because this host has no `tc netem`.

| links | R,W | R+W>N | get p50 / p99 ms | put p50 / p99 ms | ops/s | stale reads | reads with siblings |
|---|---|---|---|---|---|---|---|
| LAN | 1,1 | false | 0.92 / 13.61 | 2.58 / 17.91 | 3166 | 0.4% | 3.7% |
| LAN | 1,2 | false | 0.79 / 10.10 | 3.17 / 15.79 | 3399 | 0.1% | 3.0% |
| LAN | 2,1 | false | 1.53 / 14.17 | 2.34 / 15.16 | 2919 | 0.0% | 3.1% |
| LAN | 2,2 | true | 1.37 / 12.45 | 3.30 / 16.55 | 2796 | 0.0% | 2.3% |
| LAN | 3,3 | true | 1.81 / 8.16 | 3.99 / 12.93 | 2647 | 0.0% | 1.8% |
| cross-zone | 1,1 | false | 0.80 / 12.66 | 3.11 / 19.99 | 3059 | 0.8% | 5.3% |
| cross-zone | 1,2 | false | 0.57 / 6.06 | 5.23 / 13.64 | 3116 | 0.0% | 3.0% |
| cross-zone | 2,1 | false | 3.74 / 8.69 | 1.87 / 8.03 | 2308 | 0.0% | 4.2% |
| cross-zone | 2,2 | true | 3.60 / 7.18 | 4.51 / 8.74 | 1951 | 0.0% | 4.2% |
| cross-zone | 3,3 | true | 4.72 / 9.42 | 5.63 / 10.51 | 1513 | 0.0% | 1.8% |

## R2. Real network partition with iptables

Containers n1,n2 + load generator A on one side; n3,n4,n5 + load generator B on the other. Every packet between the sides is dropped by iptables rules on the Docker bridge. 6 clients per side, 64 keys, 4s. Then the rules are removed, the cluster runs anti-entropy, and a verifier reads every key with R=3.

| quorum | R,W | A get / put avail | B get / put avail | A get p50 ms | stale A / B | keys with siblings after heal | acked writes lost | replicas converged |
|---|---|---|---|---|---|---|---|---|
| strict | 1,1 | 89.2% / 89.9% | 99.8% / 99.7% | 0.77 | 98.9% / 86.1% | 57/64 | 0 of 5968 | true (0 divergent) |
| strict | 2,2 | 27.0% / 26.8% | 71.8% / 70.3% | 0.68 | 0.0% / 0.0% | 57/64 | 0 of 932 | true (0 divergent) |
| strict | 3,3 | 0.0% / 0.0% | 11.1% / 9.7% | - | - / 0.0% | 57/64 | 0 of 42 | true (0 divergent) |
| sloppy | 1,1 | 99.6% / 99.4% | 99.5% / 99.6% | 0.93 | 97.0% / 97.0% | 64/64 | 0 of 4763 | true (0 divergent) |
| sloppy | 2,2 | 99.3% / 98.4% | 99.5% / 99.3% | 1.89 | 96.6% / 97.0% | 64/64 | 0 of 4014 | true (0 divergent) |

## R3. Crash and restart (kill -9) with each WAL mode

8 clients, R=1, 64 keys, 4s of load. Partway through, n3 is killed with `docker kill -s KILL` (no clean shutdown, its in-flight replication dies with it). It is restarted from its volume while iptables still isolates it, so its recovered state can be inspected before any repair. The isolation is then lifted and the run finishes; a verifier finally reads every key with R=3.

* **n3 lost locally**: writes n3 coordinated and acknowledged before the kill that are missing from n3's state after restart (before repair).
* **lost cluster-wide**: acknowledged writes that no replica has after repair, so they are gone for good.

| WAL mode | W | records replayed | n3 coordinated & acked before kill | n3 lost locally | acked writes lost cluster-wide | put p50 ms | restart to serving |
|---|---|---|---|---|---|---|---|
| fsync (group commit) | 1 | 1212 | 246 | 0 (0.0%) | 0 of 4114 | 2.66 | 570ms |
| fsync (group commit) | 2 | 1048 | 202 | 0 (0.0%) | 0 of 4086 | 3.66 | 609ms |
| async, flush every 10ms | 1 | 1808 | 385 | 1 (0.3%) | 0 of 6983 | 1.01 | 642ms |
| async, flush every 10ms | 2 | 1995 | 402 | 0 (0.0%) | 0 of 6973 | 1.45 | 596ms |
| async, flush every 1s | 1 | 138 | 460 | 426 (92.6%) | 0 of 7129 | 0.96 | 548ms |
| async, flush every 1s | 2 | 197 | 423 | 381 (90.1%) | 0 of 6894 | 1.52 | 590ms |
| none (memory only) | 1 | 0 | 511 | 511 (100.0%) | 0 of 8045 | 0.81 | 597ms |
| none (memory only) | 2 | 0 | 434 | 434 (100.0%) | 0 of 7236 | 1.31 | 563ms |

## R3b. Whole-cluster crash (all 5 nodes killed at once)

8 clients, R=1 W=2, 64 keys. Partway through the run all five containers get SIGKILL at the same moment, then all restart from their volumes. Note that SIGKILL kills the process but not the machine: data already handed to the OS survives it, so "async" only loses what was still in the process's buffer. A real power cut would also lose the OS page cache, which only fsync protects against.

| WAL mode | acked writes | lost after restart + repair | records replayed (all nodes) |
|---|---|---|---|
| fsync (group commit) | 1651 | 0 (0.0%) | 7881 |
| async, flush every 10ms | 3069 | 2 (0.1%) | 14901 |
| async, flush every 1s | 2321 | 491 (21.2%) | 6433 |
| none (memory only) | 2523 | 2523 (100.0%) | 0 |

## R4. Cost of durability

R=W=2, 8 clients, 64 keys, no failures, 4s.

| WAL mode | put p50 / p99 ms | get p50 ms | ops/s |
|---|---|---|---|
| none (memory only) | 1.10 / 8.47 | 1.09 | 5048 |
| async, flush every 1s | 1.23 / 7.30 | 1.07 | 5372 |
| async, flush every 10ms | 1.14 / 6.05 | 1.02 | 5727 |
| fsync (group commit) | 3.09 / 14.00 | 1.31 | 3062 |
