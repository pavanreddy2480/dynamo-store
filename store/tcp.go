package store

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

func init() {
	for _, m := range []any{
		putReq{}, putResp{}, getReq{}, getResp{},
		coordPutReq{}, coordPutResp{}, coordGetReq{}, coordGetResp{},
		syncReq{}, syncResp{}, pingReq{}, pingResp{},
		syncAllReq{}, syncAllResp{}, dumpReq{}, dumpResp{},
	} {
		gob.Register(m)
	}
}

type envelope struct {
	From string
	Req  any
}

type tcpReply struct {
	Resp any
	OK   bool
}

type tconn struct {
	c   net.Conn
	enc *gob.Encoder
	dec *gob.Decoder
}

// TCPTransport carries requests between real processes over TCP using gob.
// Each process (node or client) has one. It keeps a small pool of
// persistent connections per peer and runs a heartbeat failure detector.
type TCPTransport struct {
	self    string
	addrs   map[string]string // node id -> host:port
	handler Handler           // nil for client processes

	// LinkDelay, if set, is added to every node-to-node message to emulate
	// links between availability zones (this machine has no tc/netem).
	LinkDelay LatencyModel

	ln   net.Listener
	mu   sync.Mutex
	idle map[string][]*tconn

	fdMu    sync.RWMutex
	lastOK  map[string]time.Time
	suspect time.Duration

	rngMu sync.Mutex
	rng   *rand.Rand

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewTCPTransport creates a transport for endpoint self. addrs lists every
// storage node. Call Listen for a node process, then StartDetector.
func NewTCPTransport(self string, addrs map[string]string) *TCPTransport {
	t := &TCPTransport{
		self: self, addrs: addrs,
		idle:    map[string][]*tconn{},
		lastOK:  map[string]time.Time{},
		suspect: 600 * time.Millisecond,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		stop:    make(chan struct{}),
	}
	now := time.Now()
	for id := range addrs {
		t.lastOK[id] = now // optimistic until a probe says otherwise
	}
	return t
}

// Listen serves requests for handler on addr.
func (t *TCPTransport) Listen(addr string, h Handler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.ln, t.handler = ln, h
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go t.serve(c)
		}
	}()
	return nil
}

func (t *TCPTransport) serve(c net.Conn) {
	defer c.Close()
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	dec, enc := gob.NewDecoder(c), gob.NewEncoder(c)
	for {
		var env envelope
		if err := dec.Decode(&env); err != nil {
			return
		}
		resp := t.handler(env.From, env.Req)
		if t.isNode(env.From) {
			t.markAlive(env.From) // any request is proof of life
		}
		if err := enc.Encode(&tcpReply{Resp: resp, OK: resp != nil}); err != nil {
			return
		}
	}
}

func (t *TCPTransport) isNode(id string) bool { _, ok := t.addrs[id]; return ok }

func (t *TCPTransport) markAlive(id string) {
	t.fdMu.Lock()
	t.lastOK[id] = time.Now()
	t.fdMu.Unlock()
}

// StartDetector probes every node every interval; a node that has not
// answered for 600ms is considered unreachable.
func (t *TCPTransport) StartDetector(interval time.Duration) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-t.stop:
				return
			case <-tk.C:
				for id := range t.addrs {
					if id == t.self {
						continue
					}
					go func(id string) {
						ctx, cancel := context.WithTimeout(context.Background(), 2*interval)
						defer cancel()
						t.Send(ctx, t.self, id, pingReq{})
					}(id)
				}
			}
		}
	}()
}

// Reachable reports the failure detector's view of node to.
func (t *TCPTransport) Reachable(_, to string) bool {
	if to == t.self {
		return true
	}
	t.fdMu.RLock()
	defer t.fdMu.RUnlock()
	last, ok := t.lastOK[to]
	return ok && time.Since(last) < t.suspect
}

func (t *TCPTransport) linkDelay() time.Duration {
	l := t.LinkDelay
	if l.Base == 0 && l.JitterMean == 0 {
		return 0
	}
	t.rngMu.Lock()
	defer t.rngMu.Unlock()
	d := l.Base + time.Duration(t.rng.ExpFloat64()*float64(l.JitterMean))
	if l.SlowProb > 0 && t.rng.Float64() < l.SlowProb {
		d += l.SlowExtra
	}
	return d
}

func (t *TCPTransport) getConn(ctx context.Context, to string) (*tconn, error) {
	t.mu.Lock()
	if p := t.idle[to]; len(p) > 0 {
		c := p[len(p)-1]
		t.idle[to] = p[:len(p)-1]
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()
	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", to)
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return &tconn{c: c, enc: gob.NewEncoder(c), dec: gob.NewDecoder(c)}, nil
}

func (t *TCPTransport) putConn(to string, c *tconn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.idle[to]) < 32 {
		t.idle[to] = append(t.idle[to], c)
		return
	}
	c.c.Close()
}

// Send implements Transport.
func (t *TCPTransport) Send(ctx context.Context, from, to string, req any) (any, error) {
	if to == t.self && t.handler != nil {
		if resp := t.handler(from, req); resp != nil {
			return resp, nil
		}
		return nil, ErrTimeout
	}
	delay := time.Duration(0)
	if t.handler != nil && t.isNode(to) { // node-to-node link
		delay = t.linkDelay()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ErrTimeout
		}
	}
	c, err := t.getConn(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		c.c.SetDeadline(dl)
	} else {
		c.c.SetDeadline(time.Time{})
	}
	var rep tcpReply
	if err := c.enc.Encode(&envelope{From: from, Req: req}); err == nil {
		err = c.dec.Decode(&rep)
		if err != nil {
			c.c.Close() // a timed-out connection may still deliver a stale reply
			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
	} else {
		c.c.Close()
		return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	t.putConn(to, c)
	if t.isNode(to) {
		t.markAlive(to)
	}
	if delay > 0 { // the reply crosses the same slow link
		time.Sleep(t.linkDelay())
	}
	if !rep.OK {
		return nil, errors.New("remote handler failed")
	}
	return rep.Resp, nil
}

// Close stops the listener, detector and pooled connections.
func (t *TCPTransport) Close() {
	close(t.stop)
	if t.ln != nil {
		t.ln.Close()
	}
	t.mu.Lock()
	for _, p := range t.idle {
		for _, c := range p {
			c.c.Close()
		}
	}
	t.idle = map[string][]*tconn{}
	t.mu.Unlock()
	t.wg.Wait()
}

// Admin helpers used by the command-line tools.

// TriggerSync asks node id to run anti-entropy with every reachable peer.
func (t *TCPTransport) TriggerSync(ctx context.Context, id string) (int, error) {
	r, err := t.Send(ctx, t.self, id, syncAllReq{})
	if err != nil {
		return 0, err
	}
	return r.(syncAllResp).Peers, nil
}

// NodeState is a node's full owned state as returned by Dump.
type NodeState struct {
	Data      map[string][]Version
	HintCount int
	Recovered int
}

// Dump fetches node id's full state.
func (t *TCPTransport) Dump(ctx context.Context, id string) (NodeState, error) {
	r, err := t.Send(ctx, t.self, id, dumpReq{})
	if err != nil {
		return NodeState{}, err
	}
	d := r.(dumpResp)
	return NodeState{Data: d.Data, HintCount: d.HintCount, Recovered: d.Recovered}, nil
}
