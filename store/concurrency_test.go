package store

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Eight clients concurrently read-modify-write one key with R=W=1 (the
// setting with the most stale reads). With sibling reconciliation by set
// union, every acknowledged item must survive.
func TestNoLostUpdatesUnderConcurrency(t *testing.T) {
	c := NewCluster(DefaultConfig())
	defer c.Close()
	var mu sync.Mutex
	var acked []string
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("w%d", w), 1, 1)
			for i := 0; !stop.Load(); i++ {
				res, err := cl.Get("hot")
				if err != nil {
					continue
				}
				item := fmt.Sprintf("w%d.%d", w, i)
				if _, err := cl.Put("hot", UnionResolver(append(res.Values(), item)), res.Context); err == nil {
					mu.Lock()
					acked = append(acked, item)
					mu.Unlock()
				}
			}
		}(w)
	}
	time.Sleep(700 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	c.AntiEntropy()
	res, err := c.NewClient("final", 3, 3).Get("hot")
	if err != nil {
		t.Fatal(err)
	}
	final := "," + UnionResolver(res.Values()) + ","
	lost := 0
	for _, it := range acked {
		if !strings.Contains(final, ","+it+",") {
			lost++
		}
	}
	if lost > 0 {
		t.Fatalf("%d of %d acknowledged items lost", lost, len(acked))
	}
	t.Logf("%d acknowledged items, none lost; %d siblings at end", len(acked), len(res.Versions))
}
