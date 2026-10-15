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
type Handler func(from string, req any) any

// LatencyModel draws one-way message delays.
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
type Network struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	group    map[string]int
	latency  LatencyModel
	clientLt LatencyModel
	isClient map[string]bool
	extra    map[string]time.Duration // per-endpoint added delay (slow nodes)

	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewNetwork creates an empty network. lat applies between replicas,
// clientLat to hops that start or end at a client.
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
func (n *Network) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.group[id] = 0
	n.isClient[id] = h == nil
}

// Partition splits endpoints into groups; endpoints not listed join group 0.
// Partition(nil) or Heal() reconnects everything.
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
func (n *Network) Reachable(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.group[from] == n.group[to]
}

func (n *Network) delay(l LatencyModel) time.Duration {
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	d := l.Base + time.Duration(n.rng.ExpFloat64()*float64(l.JitterMean))
	if l.SlowProb > 0 && n.rng.Float64() < l.SlowProb {
		d += l.SlowExtra
	}
	return time.Duration(math.Max(float64(d), 0))
}

// Send delivers req from -> to and waits for the reply or ctx expiry.
// Requests and replies each pay one sampled one-way delay; a partition that
// exists when either leg is delivered drops that leg.
func (n *Network) Send(ctx context.Context, from, to string, req any) (any, error) {
	if from == to { // local call, no network hop
		n.mu.RLock()
		h := n.handlers[to]
		n.mu.RUnlock()
		return h(from, req), nil
	}
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
		n.mu.RLock()
		h := n.handlers[to]
		ok := n.group[from] == n.group[to]
		n.mu.RUnlock()
		if !ok || h == nil {
			return // dropped
		}
		resp := h(from, req)
		time.Sleep(n.delay(lat) + slow)
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
