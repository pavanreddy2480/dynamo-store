// This file implements experiment E1 (and its variant E1b): a sweep over
// read/write quorum settings on a healthy cluster. For each setting it
// measures read and write latency and how often reads are stale, optionally
// with read repair disabled and optionally with one artificially slow
// replica, so the cost and benefit of overlapping quorums can be compared.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
	"dynamostore/vclock"
)

// quorumSweep runs experiment E1 for one quorum setting: one writer per key,
// many readers on the same hot keys, no failures.
//
// The cluster uses the default configuration (5 nodes, N=3). 32 writer
// goroutines each own one key and 16 reader goroutines pick keys at random;
// all run for the -duration flag's worth of time. Every read is checked
// against the oracle to count stale reads.
//
// Parameters:
//   - q: the read and write quorum to test.
//   - readRepair: whether replicas repair stale copies after reads.
//   - slowNode: extra delay added to every message of node n3 (0 = none).
//
// Returns:
//   - *stats: latency, success and stale-read counts for the run.
func quorumSweep(q rw, readRepair bool, slowNode time.Duration) *stats {
	cfg := store.DefaultConfig()
	cfg.Node.ReadRepair = readRepair
	c := store.NewCluster(cfg)
	defer c.Close()
	if slowNode > 0 {
		c.Net.SetSlow("n3", slowNode)
	}
	const keys, readers = 32, 16
	or := newOracle()
	st := &stats{}
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writers: exactly one per key, so writes to a key are totally ordered.
	// Each write passes the clock of the previous one as its context, so
	// every new version supersedes the last and the oracle's frontier for a
	// key is always a single version. Any staleness measured is therefore
	// caused by quorum choice, not by concurrent-write siblings.
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", k)
			cl := c.NewClient(fmt.Sprintf("w%d", k), q.R, q.W)
			var ctx vclock.VC
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				v, err := cl.Put(key, fmt.Sprint(i), ctx)
				st.put(time.Since(t0), err == nil)
				if err == nil {
					// Only acknowledged writes advance the context and are
					// reported to the oracle; a failed write may or may not
					// have reached some replicas, so it promises nothing.
					ctx = v.Clock()
					or.ack(key, v)
				}
			}
		}(k)
	}

	// Readers: each picks random keys with its own deterministic RNG. The
	// oracle snapshot is taken before the read starts, so only writes that
	// were acknowledged before the read began can make it count as stale.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			cl := c.NewClient(fmt.Sprintf("r%d", r), q.R, q.W)
			for !stop.Load() {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				before := or.snapshot(key)
				t0 := time.Now()
				res, err := cl.Get(key)
				st.get(time.Since(t0), err == nil, stale(before, res.Versions), len(res.Versions))
			}
		}(r)
	}
	time.Sleep(*dur)
	stop.Store(true)
	wg.Wait()
	return st
}
