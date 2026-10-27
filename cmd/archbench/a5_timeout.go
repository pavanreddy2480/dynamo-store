// This file implements experiment A5, which justifies the default replica
// timeout (how long a coordinator waits for replica acknowledgements). It
// sweeps several timeouts and, for each, measures the healthy-cluster p99
// put latency and failure rate, and the latency and availability of puts
// while the cluster is partitioned.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// timeouts runs experiment A5 and appends its table to the report.
//
// The trade-off it exposes: a short timeout fails healthy requests whenever
// a replica message hits the slow tail of the latency distribution, while a
// long timeout makes every request that cannot reach a quorum wait the full
// timeout before failing.
func timeouts() {
	w("\n## A5. Replica timeout\n\n")
	w("How long a coordinator waits for replicas. Strict R=W=2, 64 keys, partition {n1,n2} | {n3,n4,n5}, 6 clients per side, %v. A failed operation costs the full timeout.\n\n", *dur/2)
	w("| replica timeout | no partition: put p99 ms | no partition: put failures | partitioned: failed-op latency p50 ms | partitioned: put availability |\n|---|---|---|---|---|\n")
	for _, to := range []time.Duration{2 * time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond} {
		healthy := loadWith(to, false)
		part := loadWith(to, true)
		w("| %v | %.2f | %.2f%% | %.2f | %.1f%% |\n", to, healthy.p99ok, 100*healthy.failRate, part.p50fail, 100*(1-part.failRate))
	}
}

// loadStats summarises one loadWith run.
type loadStats struct {
	p99ok    float64 // p99 latency of successful puts, in ms (NaN if none)
	p50fail  float64 // median latency of failed puts, in ms (NaN if none)
	failRate float64 // failed puts as a fraction of all puts, in [0, 1]
}

// loadWith runs a strict-quorum put workload with the given replica timeout.
//
// Twelve R=W=2 clients (named a0..a5 and b0..b5) write random keys out of 64
// for half of the -duration flag. When partition is set, the a-clients share
// {n1,n2} and the b-clients share {n3,n4,n5}. Strict quorums never fall back
// to other nodes, so when a client reaches a coordinator but fewer than W of
// the key's replicas are on its side, the coordinator cannot tell a
// partitioned replica from a slow one: it waits the full replica timeout
// before reporting failure. That is why failed-op latency tracks the timeout.
//
// Parameters:
//   - timeout: the coordinator's replica timeout (Config.Node.ReplicaTimeout).
//   - partition: whether to split the cluster before the load starts.
//
// Returns:
//   - loadStats: success p99 latency, failure p50 latency and failure rate.
func loadWith(timeout time.Duration, partition bool) loadStats {
	cfg := store.DefaultConfig()
	cfg.Node.ReplicaTimeout = timeout
	c := store.NewCluster(cfg)
	defer c.Close()
	var clients []*store.Client
	var a, b []string
	for i := 0; i < 6; i++ {
		ca, cb := c.NewClient(fmt.Sprintf("a%d", i), 2, 2), c.NewClient(fmt.Sprintf("b%d", i), 2, 2)
		clients = append(clients, ca, cb)
		a, b = append(a, ca.ID), append(b, cb.ID)
	}
	if partition {
		c.Net.Partition(append([]string{"n1", "n2"}, a...), append([]string{"n3", "n4", "n5"}, b...))
	}
	var mu sync.Mutex
	// ok and fail collect the end-to-end latency of every put, by outcome.
	var ok, fail []time.Duration
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(cl *store.Client, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				t0 := time.Now()
				// A nil context writes blind; this experiment measures
				// latency and availability, not conflict handling.
				_, err := cl.Put(fmt.Sprintf("k%d", rng.Intn(64)), "v", nil)
				d := time.Since(t0)
				mu.Lock()
				if err == nil {
					ok = append(ok, d)
				} else {
					fail = append(fail, d)
				}
				mu.Unlock()
			}
		}(cl, int64(i))
	}
	time.Sleep(*dur / 2)
	stop.Store(true)
	wg.Wait()
	return loadStats{
		p99ok:    pctMs(ok, .99),
		p50fail:  pctMs(fail, .5),
		failRate: float64(len(fail)) / float64(len(ok)+len(fail)),
	}
}
