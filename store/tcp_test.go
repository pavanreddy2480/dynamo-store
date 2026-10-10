package store

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Five nodes in one test process, each with its own TCP listener, talking
// only over real sockets.
func TestTCPClusterRoundTrip(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = freeAddr(t)
	}
	ring := NewRing(ids, 64)
	cfg := DefaultConfig().Node
	dir := t.TempDir()
	var trs []*TCPTransport
	for _, id := range ids {
		tr := NewTCPTransport(id, addrs)
		wal, _ := OpenWAL(dir, id, SyncAlways, 0)
		nd, err := NewNode(id, cfg, tr, ring, wal)
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.Listen(addrs[id], nd.Handle); err != nil {
			t.Fatal(err)
		}
		tr.StartDetector(100 * time.Millisecond)
		nd.Start()
		defer nd.Close()
		trs = append(trs, tr)
	}
	ctr := NewTCPTransport("client", addrs)
	defer ctr.Close()
	a := NewClient("a", ctr, ring, 3, 2, 2, time.Second)
	b := NewClient("b", ctr, ring, 3, 2, 2, time.Second)

	v, err := a.Put("cart", "milk", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.Get("cart")
	if err != nil || len(got.Versions) != 1 || got.Versions[0].Value != "milk" {
		t.Fatalf("read after write: %v %v", got.Values(), err)
	}
	// Two writes from the same context are concurrent: both come back.
	a.Put("cart", "milk,eggs", v.Clock())
	b.Put("cart", "bread,milk", v.Clock())
	got, _ = b.Get("cart")
	if len(got.Versions) != 2 {
		t.Fatalf("want 2 siblings over TCP, got %v", got.Values())
	}
	if _, n, err := b.GetResolved("cart", UnionResolver); err != nil || n != 2 {
		t.Fatalf("resolve: %d %v", n, err)
	}
	time.Sleep(50 * time.Millisecond)
	got, _ = a.Get("cart")
	if fmt.Sprint(got.Values()) != "[bread,eggs,milk]" {
		t.Fatalf("after reconcile: %v", got.Values())
	}
	st, err := ctr.Dump(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("n1 holds %d keys", len(st.Data))
	for _, tr := range trs {
		tr.Close()
	}
}
