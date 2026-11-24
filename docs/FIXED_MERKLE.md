# The chosen design: 256 fixed partitions, one eager Merkle tree per partition

Branch `fixed-merkle`. This is the combination chosen after the A6 to A8 experiments ([MERKLE_EXPERIMENTS.md](MERKLE_EXPERIMENTS.md)).

```
ring (256 equal partitions, dealt round-robin to 5 nodes)
 p0   p1   p2   p3   p4   p5 ...            partition p = the gap between two tokens
 n1   n2   n3   n4   n5   n1 ...            every key of p has the same 3 replicas
 |
 +--> replicas of p0 = {n1,n2,n3}: each keeps an identical Merkle tree of p0's keys

sync(a, b):  shared = partitions where both a and b are replicas
  round 1:  a sends the root hash of every shared partition      (all equal? done)
  then:     descend only into partitions/subtrees whose hash differs, one level per round trip
  last:     exchange keys under differing leaves (hash list, then versions)
```

## What changed from the per-peer design

| | per-peer trees (branch `merkle-anti-entropy`) | per-partition trees (this branch) |
|---|---|---|
| trees per node | one per peer (4) | one per partition it replicates (about 154) |
| tree updates per put | 2 (one per other replica) | 1 |
| tree depth | 10 | 4 (16 leaves, about 5 keys each at 20k keys) |
| KB sent per round, 20k keys, 1% / 10% diverged | 236 / 1,428 | 151 / 1,145 |
| round trips per pair | 13 | 7 |
| after a node joins | trees for the peers whose key sets changed must be rebuilt | partition boundaries do not move, so a key keeps its partition and tree; only a new replica's tree starts empty and fills by sync |

The depth was picked by sweep (A6): at 1% divergence KB sent falls from 496 (depth 0) to 151 (depth 4) and 146 (depth 5), then rises (149 at depth 6, 166 at depth 8) as hash traffic grows. Depth 4 is within 4% of the best and uses fewer round trips than depth 5.

## Code

* `store/ring.go`: `NewFixedRing`, `WithNode`, `WithoutNode`, `Partition`, `PartitionList`; each partition's walk is precomputed, so a lookup no longer allocates.
* `store/merkle.go`: one tree (about 170 lines with comments).
* `store/antientropy.go`: per-partition tree updates in `put` and the tree-walk protocol.
* Default config: `Partitions: 256`, `MerkleDepth: 4`. `Partitions: 0` falls back to random vnodes, where each gap between two tokens is a partition and the same code works.

## Proof

`go test -race ./...`: randomized equivalence with full-state sync (25 seeds), partition-heal repair, the same on random vnodes, trees equal trees rebuilt from the data, a key never changes partition across a join. Breaking the leaf exchange or the incremental hash update makes the tests fail.

## What the results now show on this default

Results were regenerated on this ring (`go run ./cmd/bench`, `go run ./cmd/archbench`, `go run ./cmd/plots`). E1 to E3 and A1 to A5 shift by a few points with the different key placement (see the note at the top of ARCHITECTURE_DECISIONS.md); the conclusions are unchanged.
