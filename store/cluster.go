// This file wires a whole simulated cluster together: Config and its
// defaults, and Cluster, which owns the network, the ring and every replica
// and exposes cluster-wide operations (anti-entropy, clock skew, waiting
// for hinted handoff) plus the constructor for clients.

package store

import (
	"fmt"
	"time"
)

// Config describes a whole simulated cluster.
//
// Start from DefaultConfig and override the fields an experiment needs.
type Config struct {
	Nodes   int          // number of physical nodes, named n1..nNodes
	VNodes  int          // virtual nodes (ring tokens) per physical node
	Node    NodeConfig   // settings shared by every replica
	Latency LatencyModel // replica-to-replica message delay
	Client  LatencyModel // delay of hops that start or end at a client
	Seed    int64        // seeds the network's latency sampling
}

// DefaultConfig is 5 nodes with 3 replicas per key.
//
// Replicas keep siblings, read repair is on, hinted handoff runs every
// 5ms and coordinators wait 20ms for replicas.
//
// Returns:
//   - Config: a fresh configuration the caller may modify.
func DefaultConfig() Config {
	return Config{
		Nodes:  5,
		VNodes: 64,
		Node: NodeConfig{
			N:               3,
			Policy:          KeepSiblings,
			ReplicaTimeout:  20 * time.Millisecond,
			ReadRepair:      true,
			HandoffInterval: 5 * time.Millisecond,
		},
		Latency: DefaultLatency,
		Client:  DefaultClientLatency,
		Seed:    1,
	}
}

// Cluster owns the network, the ring and the replicas.
//
// Create one with NewCluster, talk to it through clients from NewClient,
// and call Close when done to stop the nodes' background loops.
type Cluster struct {
	Net   *Network         // message bus between nodes and clients
	Ring  *Ring            // consistent-hash ring placing keys on nodes
	Nodes map[string]*Node // by node id
	IDs   []string         // node ids in a fixed order
	cfg   Config           // configuration the cluster was built from
}

// NewCluster builds and starts a cluster.
//
// It creates cfg.Nodes nodes named n1..nN, places them on a ring with
// cfg.VNodes tokens each, registers them on a new network and starts each
// node's hinted-handoff goroutine.
//
// Parameters:
//   - cfg: the cluster configuration, usually derived from DefaultConfig.
//
// Returns:
//   - *Cluster: the running cluster; the caller must call Close.
func NewCluster(cfg Config) *Cluster {
	ids := make([]string, cfg.Nodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	c := &Cluster{
		Net:   NewNetwork(cfg.Latency, cfg.Client, cfg.Seed),
		Ring:  NewRing(ids, cfg.VNodes),
		Nodes: map[string]*Node{},
		IDs:   ids,
		cfg:   cfg,
	}
	for _, id := range ids {
		c.Nodes[id] = newNode(id, cfg.Node, c.Net, c.Ring)
	}
	for _, nd := range c.Nodes {
		nd.start()
	}
	return c
}

// SetSkew changes one node's wall-clock skew (used by LWW experiments).
//
// The skew is added to the timestamp of every version the node coordinates
// from now on. Safe to call while the cluster is serving requests.
//
// Parameters:
//   - id: the node to change; must be one of c.IDs.
//   - d: the offset added to the node's wall clock (may be negative).
func (c *Cluster) SetSkew(id string, d time.Duration) {
	nd := c.Nodes[id]
	nd.mu.Lock()
	nd.cfg.ClockSkew = d
	nd.mu.Unlock()
}

// Close stops background work.
//
// It stops every node's hinted-handoff loop and waits for it to exit. The
// cluster must not be used afterwards.
func (c *Cluster) Close() {
	for _, nd := range c.Nodes {
		nd.close()
	}
}

// PreferenceList returns the replicas for key.
//
// Parameters:
//   - key: the key to look up.
//
// Returns:
//   - []string: the ids of the N nodes responsible for key, in ring order.
func (c *Cluster) PreferenceList(key string) []string {
	return c.Ring.PreferenceList(key, c.cfg.Node.N)
}

// AntiEntropy runs one round of pairwise synchronisation between every pair
// of replicas that can currently reach each other.
//
// Each pair is synchronised once, sequentially; errors from individual
// exchanges (timeouts) are ignored, since the next round retries them.
func (c *Cluster) AntiEntropy() {
	for i, a := range c.IDs {
		for _, b := range c.IDs[i+1:] {
			if c.Net.Reachable(a, b) {
				_ = c.Nodes[a].syncWith(b)
			}
		}
	}
}

// WaitHintsDrained blocks until no node holds hints or the deadline passes.
//
// It polls every millisecond; delivery itself is done by the nodes'
// hinted-handoff loops.
//
// Parameters:
//   - d: the maximum time to wait.
//
// Returns:
//   - bool: true if every hint was delivered within d, false on timeout.
func (c *Cluster) WaitHintsDrained(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		n := 0
		for _, nd := range c.Nodes {
			n += nd.HintCount()
		}
		if n == 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// NewClient registers a client endpoint. R and W are per-client settings.
//
// The client's request timeout defaults to three replica timeouts, enough
// for a coordinator to wait out slow replicas and still answer.
//
// Parameters:
//   - id: the client's network endpoint id; must be unique in the cluster.
//   - r: read quorum, in [1, N].
//   - w: write quorum, in [1, N].
//
// Returns:
//   - *Client: the registered client.
//
// It panics if r or w is outside [1, N].
func (c *Cluster) NewClient(id string, r, w int) *Client {
	if r < 1 || w < 1 || r > c.cfg.Node.N || w > c.cfg.Node.N {
		panic(fmt.Sprintf("R=%d W=%d must be in [1,%d]", r, w, c.cfg.Node.N))
	}
	c.Net.Register(id, nil)
	return &Client{
		ID: id, R: r, W: w,
		Timeout: 3 * c.cfg.Node.ReplicaTimeout,
		net:     c.Net, ring: c.Ring, n: c.cfg.Node.N,
	}
}
