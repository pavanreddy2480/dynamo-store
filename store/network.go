// This file implements the simulated network every node and client talks
// through: an in-memory message bus with a sampled latency model, per-node
// slowdowns and partitions. Messages that cross a partition are dropped
// silently, so senders only learn about failures through timeouts, as on a
// real network. It also provides the failure-detector view (Reachable)
// that coordinators use to choose live nodes.

package store

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ErrTimeout is returned when a message (or its reply) is lost or late.
var ErrTimeout = errors.New("network: request timed out")

// Handler processes one request delivered to an endpoint.
//
// It is called on its own goroutine for every delivered message, so
// implementations must be safe for concurrent use. from is the sender's
// endpoint id; the return value is sent back as the reply.
type Handler func(from string, req any) any

// LatencyModel draws one-way message delays.
//
// A delay is Base plus an exponentially distributed jitter with mean
// JitterMean, plus SlowExtra with probability SlowProb (a heavy tail).
type LatencyModel struct {
	Base       time.Duration // fixed propagation delay
	JitterMean time.Duration // mean of an exponential jitter added to Base
	SlowProb   float64       // probability a message hits a slow path
	SlowExtra  time.Duration // extra delay on the slow path
}

// DefaultLatency models replicas spread across availability zones:
// replica-to-replica hops are a few hundred microseconds with a heavy tail.
var DefaultLatency = LatencyModel{
	Base:       400 * time.Microsecond,
	JitterMean: 800 * time.Microsecond,
	SlowProb:   0.03,
	SlowExtra:  5 * time.Millisecond,
}

// DefaultClientLatency models a client sitting next to the node it talks to
// (same zone), so the client hop is short compared to replication.
var DefaultClientLatency = LatencyModel{
	Base:       100 * time.Microsecond,
	JitterMean: 100 * time.Microsecond,
}

// Network is an in-memory message bus between endpoints (replicas and
// clients). Each endpoint belongs to a partition group; messages between
// different groups are silently dropped, so the sender only learns about
// the failure through a timeout, as on a real network.
//
// All methods are safe for concurrent use.
type Network struct {
	mu       sync.RWMutex             // guards every field below except rngMu and rng
	handlers map[string]Handler       // endpoint id -> request handler (nil for clients)
	group    map[string]int           // endpoint id -> partition group; same group = connected
	latency  LatencyModel             // replica-to-replica delay
	clientLt LatencyModel             // delay for hops touching a client
	isClient map[string]bool          // endpoints that only send requests
	extra    map[string]time.Duration // per-endpoint added delay (slow nodes)

	rngMu sync.Mutex // rand.Rand is not safe for concurrent use
	rng   *rand.Rand // seeded source for latency sampling
}

// NewNetwork creates an empty network. lat applies between replicas,
// clientLat to hops that start or end at a client.
//
// Parameters:
//   - lat: latency model for replica-to-replica hops.
//   - clientLat: latency model for hops that start or end at a client.
//   - seed: seed for latency sampling, so runs are repeatable.
//
// Returns:
//   - *Network: a network with no endpoints and no partitions.
func NewNetwork(lat, clientLat LatencyModel, seed int64) *Network {
	return &Network{
		clientLt: clientLat,
		isClient: map[string]bool{},
		handlers: map[string]Handler{},
		group:    map[string]int{},
		extra:    map[string]time.Duration{},
		latency:  lat,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register attaches an endpoint. Clients register a nil handler.
//
// The endpoint joins partition group 0 (connected to every other endpoint
// in group 0). Registering an existing id replaces it.
//
// Parameters:
//   - id: the endpoint's unique id.
//   - h: the request handler, or nil for a client that only sends.
func (n *Network) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.group[id] = 0
	n.isClient[id] = h == nil
}

// Partition splits endpoints into groups; endpoints not listed join group 0.
// Partition(nil) or Heal() reconnects everything.
//
// The listed groups are numbered from 1, so unlisted endpoints form one
// further group of their own. Messages already in flight are dropped if
// they cross the new partition when delivered.
//
// Parameters:
//   - groups: the sets of endpoint ids that can still reach each other.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

// SetSlow adds d to every message delivered to or from endpoint id.
//
// Parameters:
//   - id: the endpoint to slow down.
//   - d: extra one-way delay per leg; zero removes the slowdown.
func (n *Network) SetSlow(id string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.extra[id] = d
}

// Heal removes all partitions.
func (n *Network) Heal() { n.Partition() }

// Reachable is the failure detector's view: whether from can currently talk
// to to. Coordinators use it to pick live nodes (as Dynamo's gossip-based
// failure detector would); actual sends to unreachable nodes still time out.
//
// Parameters:
//   - from: the sending endpoint.
//   - to: the receiving endpoint.
//
// Returns:
//   - bool: true if both endpoints are in the same partition group.
func (n *Network) Reachable(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.group[from] == n.group[to]
}

// delay samples one one-way message delay from l.
//
// Parameters:
//   - l: the latency model to sample from.
//
// Returns:
//   - time.Duration: the sampled delay, never negative.
func (n *Network) delay(l LatencyModel) time.Duration {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	d := l.Base + time.Duration(n.rng.ExpFloat64()*float64(l.JitterMean))
	if l.SlowProb > 0 && n.rng.Float64() < l.SlowProb {
		d += l.SlowExtra
	}
	// Clamp so a misconfigured negative Base cannot produce a negative
	// sleep.
	return time.Duration(math.Max(float64(d), 0))
}

// Send delivers req from -> to and waits for the reply or ctx expiry.
// Requests and replies each pay one sampled one-way delay; a partition that
// exists when either leg is delivered drops that leg.
//
// Delivery runs on its own goroutine, which keeps running (and the handler
// still executes) even after Send has returned ErrTimeout; a late reply is
// simply discarded.
//
// Parameters:
//   - ctx: bounds how long Send waits for the reply.
//   - from: the sending endpoint's id.
//   - to: the receiving endpoint's id.
//   - req: the request message, passed to the receiver's Handler.
//
// Returns:
//   - any: the receiver's reply.
//   - error: nil on success; ErrTimeout if either leg was dropped or the
//     reply did not arrive before ctx was done.
func (n *Network) Send(ctx context.Context, from, to string, req any) (any, error) {
	if from == to { // local call, no network hop
		n.mu.RLock()
		h := n.handlers[to]
		n.mu.RUnlock()
		return h(from, req), nil
	}
	// Buffered so the delivery goroutine can always finish, even if the
	// caller has stopped waiting.
	out := make(chan any, 1)
	go func() {
		n.mu.RLock()
		slow := n.extra[from] + n.extra[to]
		lat := n.latency
		if n.isClient[from] || n.isClient[to] {
			lat = n.clientLt
		}
		n.mu.RUnlock()
		time.Sleep(n.delay(lat) + slow)
		// Check the partition at delivery time, not send time: a partition
		// that forms while the message is in flight drops it, as it would
		// on a real network. A nil handler means the target is a client,
		// which accepts no requests.
		n.mu.RLock()
		h := n.handlers[to]
		ok := n.group[from] == n.group[to]
		n.mu.RUnlock()
		if !ok || h == nil {
			return // dropped
		}
		resp := h(from, req)
		time.Sleep(n.delay(lat) + slow)
		// The reply leg is checked separately: the request may have been
		// processed (with its side effects) and only the answer lost, so
		// the sender cannot tell a lost request from a lost reply.
		if !n.Reachable(to, from) {
			return // reply dropped
		}
		out <- resp
	}()
	select {
	case r := <-out:
		return r, nil
	case <-ctx.Done():
		return nil, ErrTimeout
	}
}
