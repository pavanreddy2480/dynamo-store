// Package store is a Dynamo-style replicated key-value store that runs as a
// simulation inside one process.
//
// # Model
//
// A Cluster has a fixed set of nodes on a consistent-hash Ring. Each key is
// stored on N nodes (its preference list), the first N distinct nodes found
// walking the ring clockwise from the key's hash. Nodes and clients exchange
// messages over an in-memory Network that adds sampled latency and can be
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
// (Cluster.AntiEntropy, a full-state exchange between every reachable pair).
//
// See ARCHITECTURE.md for the reasons behind each choice and the experiments
// that support them.
package store
