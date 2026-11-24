// Package store is a Dynamo-style replicated key-value store that runs as a
// simulation inside one process.
//
// # Model
//
// A Cluster has a set of nodes on a consistent-hash ring (package ring: 256
// equal fixed partitions by default). Each key is
// stored on N nodes (its preference list), the first N distinct nodes found
// walking the ring clockwise from the key's hash. Nodes and clients exchange
// messages over an in-memory network (package network) that adds sampled
// latency and can be
// partitioned; a message across a partition is dropped, so the sender only
// learns about the failure through a timeout.
//
// # Requests
//
// A Client sends every Get and Put to a coordinator, one of the key's
// preferred replicas. The coordinator fans the request out to all N replicas
// and answers as soon as R replies (reads) or W acknowledgements (writes)
// arrive. The remaining replicas keep receiving the write in the background;
// that lag is why R+W <= N can return stale reads and R+W > N cannot.
//
// With Sloppy set, a coordinator that believes a preferred replica is down
// sends its copy to the next live node on the ring instead, tagged with a
// hint naming the intended owner. The hint is delivered once the owner is
// reachable again (hinted handoff).
//
// # Versions and conflicts
//
// Each write gets a Dot (coordinating node, per-key counter) and carries the
// causal context the client read before writing. A version supersedes
// another only if its context contains the other's dot; otherwise both are
// kept as siblings and returned together by Get. The client reconciles them
// explicitly (UnionResolver is the rule used here) and writes the result
// back under the merged context, which collapses the siblings. The
// LastWriterWins policy is provided for comparison: it keeps one version
// per key and silently drops the rest.
//
// # Repair
//
// Three mechanisms bring replicas back in line: hinted handoff (writes held
// for an unreachable owner), read repair (after a read, replicas that
// answered with older data are sent the merged result) and anti-entropy
// (a Merkle-tree comparison, package merkle, between two replicas; each node
// starts one with a random peer every AntiEntropyInterval, and
// Cluster.AntiEntropy runs one round over every reachable pair).
//
// # Related packages
//
//   - vclock: vector clocks.
//   - ring: consistent hashing and fixed partitions.
//   - network: the simulated network (latency, partitions, slow nodes).
//   - merkle: the hash tree kept for each partition.
//   - scenario: the partition-and-reconcile scenario used by the demo and tests.
//
// See docs/reference/ARCHITECTURE_DECISIONS.md for the reasons behind each choice and the experiments
// that support them.
package store
