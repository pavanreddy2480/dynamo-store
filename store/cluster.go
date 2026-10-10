package store

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"dynamostore/vclock"
)

// Config describes a whole simulated cluster.
type Config struct {
	Nodes   int
	VNodes  int
	Node    NodeConfig
	Latency LatencyModel
	Client  LatencyModel
	Seed    int64
	// DataDir holds each node's write-ahead log when WAL is not SyncNone.
	DataDir string
	WAL     SyncMode
	// WALFlush is the background flush interval for SyncAsync.
	WALFlush time.Duration
}

// DefaultConfig is 5 nodes with 3 replicas per key.
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
		Latency:  DefaultLatency,
		Client:   DefaultClientLatency,
		Seed:     1,
		WALFlush: 10 * time.Millisecond,
	}
}

// Cluster owns the network, the ring and the replicas.
type Cluster struct {
	Net   *Network
	Ring  *Ring
	Nodes map[string]*Node
	IDs   []string
	cfg   Config
}

// NewCluster builds and starts a cluster.
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
		c.Nodes[id] = c.bootNode(id)
	}
	return c
}

func (c *Cluster) bootNode(id string) *Node {
	var wal *WAL
	if c.cfg.WAL != SyncNone {
		var err error
		if wal, err = OpenWAL(c.cfg.DataDir, id, c.cfg.WAL, c.cfg.WALFlush); err != nil {
			panic(err)
		}
	}
	nd, err := NewNode(id, c.cfg.Node, c.Net, c.Ring, wal)
	if err != nil {
		panic(err)
	}
	c.Net.Register(id, nd.Handle)
	nd.Start()
	return nd
}

// Crash kills node id like kill -9: it stops answering, its memory is
// discarded and unflushed WAL records are lost.
func (c *Cluster) Crash(id string) {
	c.Net.SetDown(id, true)
	c.Nodes[id].crash()
}

// Restart boots node id again from its WAL (empty if WAL is SyncNone).
func (c *Cluster) Restart(id string) *Node {
	nd := c.bootNode(id)
	c.Nodes[id] = nd
	c.Net.SetDown(id, false)
	return nd
}

// SetSkew changes one node's wall-clock skew (used by LWW experiments).
func (c *Cluster) SetSkew(id string, d time.Duration) {
	nd := c.Nodes[id]
	nd.mu.Lock()
	nd.cfg.ClockSkew = d
	nd.mu.Unlock()
}

// Close stops background work.
func (c *Cluster) Close() {
	for _, nd := range c.Nodes {
		nd.Close()
	}
}

// PreferenceList returns the replicas for key.
func (c *Cluster) PreferenceList(key string) []string {
	return c.Ring.PreferenceList(key, c.cfg.Node.N)
}

// AntiEntropy runs one round of pairwise synchronisation between every pair
// of replicas that can currently reach each other.
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
func (c *Cluster) NewClient(id string, r, w int) *Client {
	if r < 1 || w < 1 || r > c.cfg.Node.N || w > c.cfg.Node.N {
		panic(fmt.Sprintf("R=%d W=%d must be in [1,%d]", r, w, c.cfg.Node.N))
	}
	c.Net.Register(id, nil)
	return NewClient(id, c.Net, c.Ring, c.cfg.Node.N, r, w, 3*c.cfg.Node.ReplicaTimeout)
}

// NewClient creates a client over any transport (used by real processes).
func NewClient(id string, tr Transport, ring *Ring, n, r, w int, timeout time.Duration) *Client {
	return &Client{ID: id, R: r, W: w, Timeout: timeout, net: tr, ring: ring, n: n}
}

// Client talks to the cluster. R and W are exposed as client settings and
// travel with every request; Sloppy enables sloppy quorums + hinted handoff.
type Client struct {
	ID      string
	R, W    int
	Sloppy  bool
	Timeout time.Duration
	// FirstCoordinator always routes to the first reachable preferred
	// replica instead of a random one (compared in cmd/archbench).
	FirstCoordinator bool

	net  Transport
	ring *Ring
	n    int
}

// GetResult holds every concurrent version found plus the context (merged
// clock) to pass back on the next Put.
type GetResult struct {
	Versions []Version
	Context  vclock.VC
}

// Values returns just the sibling values.
func (g GetResult) Values() []string {
	out := make([]string, len(g.Versions))
	for i, v := range g.Versions {
		out[i] = v.Value
	}
	return out
}

// coordinator picks a random preferred replica this client can reach (load
// spreads across the N replicas, so a read may land on a replica the last
// write has not reached yet). With sloppy quorums, if no preferred replica
// is reachable, the first reachable node further along the ring is used.
func (cl *Client) coordinator(key string) (string, error) {
	if cl.FirstCoordinator {
		for _, id := range cl.ring.PreferenceList(key, cl.n) {
			if cl.net.Reachable(cl.ID, id) {
				return id, nil
			}
		}
	}
	var live []string
	for _, id := range cl.ring.PreferenceList(key, cl.n) {
		if cl.net.Reachable(cl.ID, id) {
			live = append(live, id)
		}
	}
	if len(live) > 0 {
		return live[rand.Intn(len(live))], nil
	}
	if cl.Sloppy {
		for _, id := range cl.ring.Walk(key) {
			if cl.net.Reachable(cl.ID, id) {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("%w: no reachable coordinator", ErrUnavailable)
}

// Get reads key with read quorum R.
func (cl *Client) Get(key string) (GetResult, error) {
	coord, err := cl.coordinator(key)
	if err != nil {
		return GetResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cl.Timeout)
	defer cancel()
	resp, err := cl.net.Send(ctx, cl.ID, coord, coordGetReq{Key: key, R: cl.R, Sloppy: cl.Sloppy})
	if err != nil {
		return GetResult{}, err
	}
	r := resp.(coordGetResp)
	res := GetResult{Versions: r.Versions, Context: vclock.New()}
	for _, v := range r.Versions {
		res.Context = vclock.Merge(res.Context, v.Clock())
	}
	return res, quorumErr(r.Err)
}

func quorumErr(msg string) error {
	if msg == "" {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrUnavailable, msg)
}

// Put writes value with write quorum W. ctx must be the Context from a
// previous Get (or nil for a blind first write); the new version supersedes
// every version that context covers.
func (cl *Client) Put(key, value string, ctx vclock.VC) (Version, error) {
	if ctx == nil {
		ctx = vclock.New()
	}
	coord, err := cl.coordinator(key)
	if err != nil {
		return Version{}, err
	}
	cctx, cancel := context.WithTimeout(context.Background(), cl.Timeout)
	defer cancel()
	req := coordPutReq{Key: key, Value: value, Context: ctx, W: cl.W, Sloppy: cl.Sloppy}
	resp, err := cl.net.Send(cctx, cl.ID, coord, req)
	if err != nil {
		return Version{}, err
	}
	r := resp.(coordPutResp)
	return r.Version, quorumErr(r.Err)
}

// Resolver merges sibling values into one.
type Resolver func(values []string) string

// GetResolved reads key and, if siblings came back, merges them with
// resolve and writes the result back under the merged context so the
// siblings collapse into a single version.
func (cl *Client) GetResolved(key string, resolve Resolver) (string, int, error) {
	res, err := cl.Get(key)
	if err != nil {
		return "", 0, err
	}
	switch len(res.Versions) {
	case 0:
		return "", 0, nil
	case 1:
		return res.Versions[0].Value, 1, nil
	}
	merged := resolve(res.Values())
	_, err = cl.Put(key, merged, res.Context)
	return merged, len(res.Versions), err
}
