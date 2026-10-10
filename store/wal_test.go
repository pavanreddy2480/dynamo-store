package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func walCluster(t *testing.T, mode SyncMode) *Cluster {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.WAL = mode
	return NewCluster(cfg)
}

// writeKeys writes n keys with W=3 so every replica has every write, then
// returns n1's state.
func writeKeys(t *testing.T, c *Cluster, n int) map[string]string {
	cl := c.NewClient(fmt.Sprintf("w%d", time.Now().UnixNano()), 1, 3)
	for i := 0; i < n; i++ {
		if _, err := cl.Put(fmt.Sprintf("k%d", i), fmt.Sprint(i), nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	return state(c.Nodes["n1"])
}

func state(nd *Node) map[string]string {
	out := map[string]string{}
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for k, vs := range nd.data {
		out[k] = fmt.Sprint(vs)
	}
	return out
}

func TestWALRecoversStateAfterCrash(t *testing.T) {
	for _, mode := range []SyncMode{SyncAlways, SyncAsync} {
		c := walCluster(t, mode)
		before := writeKeys(t, c, 200)
		c.Crash("n1")
		nd := c.Restart("n1")
		after := state(nd)
		if len(before) == 0 || fmt.Sprint(before) != fmt.Sprint(after) {
			t.Errorf("%v: n1 had %d keys before crash, %d after replay", mode, len(before), len(after))
		}
		if nd.Recovered() == 0 {
			t.Errorf("%v: no records replayed", mode)
		}
		c.Close()
	}
}

func TestNoWALLosesLocalStateButReplicationRepairs(t *testing.T) {
	c := walCluster(t, SyncNone)
	defer c.Close()
	before := writeKeys(t, c, 200)
	c.Crash("n1")
	nd := c.Restart("n1")
	if len(state(nd)) != 0 {
		t.Fatal("memory-only node should restart empty")
	}
	c.AntiEntropy()
	if after := state(nd); fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("anti-entropy restored %d of %d keys", len(after), len(before))
	}
}

// A crash in the middle of a write leaves a torn record at the end of the
// log; recovery must keep every complete record and ignore the tail.
func TestWALIgnoresTornTail(t *testing.T) {
	c := walCluster(t, SyncAlways)
	before := writeKeys(t, c, 50)
	c.Crash("n1")
	f, err := os.OpenFile(c.Nodes["n1"].wal.Path(), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 1, 0, 0xde, 0xad}) // header promising 256 bytes, then nothing
	f.Close()
	nd := c.Restart("n1")
	defer c.Close()
	if fmt.Sprint(before) != fmt.Sprint(state(nd)) {
		t.Fatal("torn tail corrupted recovery")
	}
	// The node must still accept writes after recovering.
	if _, err := c.NewClient("after", 1, 3).Put("k0", "new", nil); err != nil {
		t.Fatal(err)
	}
}

// The coordinator's dot counter must survive a restart; otherwise it would
// reissue a dot and a new write could be mistaken for an old one.
func TestIssuedCountersSurviveRestart(t *testing.T) {
	c := walCluster(t, SyncAlways)
	defer c.Close()
	writeKeys(t, c, 20)
	c.Nodes["n1"].mu.Lock()
	want := fmt.Sprint(c.Nodes["n1"].issued)
	c.Nodes["n1"].mu.Unlock()
	c.Crash("n1")
	nd := c.Restart("n1")
	if got := fmt.Sprint(nd.issued); got != want {
		t.Fatalf("issued counters: got %s want %s", got, want)
	}
}
