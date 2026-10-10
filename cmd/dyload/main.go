// Command dyload is the client side of the multi-process cluster.
//
//	dyload run    -peers ... -r 2 -w 2 -clients 8 -duration 5s -out hist.jsonl
//	dyload dump   -peers ... -node n3 -out n3.json
//	dyload verify -peers ... -keys 64 -out final.json
//
// "run" drives a read / read-modify-write workload and records every
// operation (start, end, outcome, versions seen or written) as JSON lines.
// Stale reads, availability and lost writes are computed offline from these
// histories by cmd/realbench, so several load generators in different
// network locations can be analysed together.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamostore/store"
)

// HV is a version as recorded in histories: its dot and causal context.
type HV struct {
	N string            `json:"n"`
	C uint64            `json:"c"`
	X map[string]uint64 `json:"x,omitempty"`
}

// Op is one client operation.
type Op struct {
	C   string `json:"c"`
	Op  string `json:"op"`
	K   string `json:"k"`
	T0  int64  `json:"t0"`
	T1  int64  `json:"t1"`
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
	V   []HV   `json:"v,omitempty"`
}

func toHV(vs ...store.Version) []HV {
	out := make([]HV, len(vs))
	for i, v := range vs {
		out[i] = HV{N: v.Dot.Node, C: v.Dot.Counter, X: v.Ctx}
	}
	return out
}

func parsePeers(s string) (map[string]string, *store.Ring) {
	out := map[string]string{}
	var ids []string
	for _, p := range strings.Split(s, ",") {
		if id, addr, ok := strings.Cut(strings.TrimSpace(p), "="); ok {
			out[id] = addr
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return out, store.NewRing(ids, 64)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: dyload run|dump|verify [flags]")
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	peers := fs.String("peers", "", "all nodes: id=host:port,...")
	id := fs.String("id", "load", "client id prefix")
	out := fs.String("out", "", "output file (default stdout)")
	r := fs.Int("r", 2, "read quorum R")
	w := fs.Int("w", 2, "write quorum W")
	n := fs.Int("n", 3, "replicas per key")
	sloppy := fs.Bool("sloppy", false, "sloppy quorums")
	clients := fs.Int("clients", 8, "concurrent clients")
	keys := fs.Int("keys", 64, "number of keys")
	writeFrac := fs.Float64("write-frac", 0.5, "fraction of reads followed by a write")
	dur := fs.Duration("duration", 5*time.Second, "how long to run")
	timeout := fs.Duration("timeout", 60*time.Millisecond, "client request timeout")
	startAt := fs.Int64("start-at", 0, "unix ms to start at (aligns several load generators)")
	node := fs.String("node", "", "node to dump")
	fs.Parse(os.Args[2:])

	addrs, ring := parsePeers(*peers)
	tr := store.NewTCPTransport(*id, addrs)
	tr.StartDetector(100 * time.Millisecond)
	defer tr.Close()
	time.Sleep(300 * time.Millisecond) // let the detector see who is up

	var dst *os.File = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		dst = f
	}
	bw := bufio.NewWriterSize(dst, 1<<20)
	defer bw.Flush()

	switch cmd {
	case "run":
		if *startAt > 0 {
			time.Sleep(time.Until(time.UnixMilli(*startAt)))
		}
		run(tr, ring, bw, *id, *clients, *keys, *n, *r, *w, *sloppy, *writeFrac, *dur, *timeout)
	case "dump":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		st, err := tr.Dump(ctx, *node)
		if err != nil {
			log.Fatalf("dump %s: %v", *node, err)
		}
		json.NewEncoder(bw).Encode(nodeJSON(st))
	case "verify":
		verify(tr, ring, bw, addrs, *keys, *n)
	default:
		log.Fatalf("unknown command %q", cmd)
	}
}

func run(tr *store.TCPTransport, ring *store.Ring, bw *bufio.Writer, id string, clients, keys, n, r, w int,
	sloppy bool, writeFrac float64, dur, timeout time.Duration) {
	ops := make(chan Op, 4096)
	done := make(chan struct{})
	go func() {
		enc := json.NewEncoder(bw)
		for op := range ops {
			enc.Encode(op)
		}
		close(done)
	}()
	var stop atomic.Bool
	var wg sync.WaitGroup
	var nGet, nPut, okGet, okPut atomic.Int64
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cid := fmt.Sprintf("%s-%d", id, i)
			cl := store.NewClient(cid, tr, ring, n, r, w, timeout)
			cl.Sloppy = sloppy
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
			for seq := 0; !stop.Load(); seq++ {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				t0 := time.Now().UnixNano()
				g, err := cl.Get(key)
				op := Op{C: cid, Op: "get", K: key, T0: t0, T1: time.Now().UnixNano(), OK: err == nil, V: toHV(g.Versions...)}
				if err != nil {
					op.Err = err.Error()
				} else {
					okGet.Add(1)
				}
				nGet.Add(1)
				ops <- op
				if rng.Float64() >= writeFrac {
					continue
				}
				// Write a new value superseding what was read (a blind
				// write if the read failed).
				t0 = time.Now().UnixNano()
				v, err := cl.Put(key, fmt.Sprintf("%s.%d", cid, seq), g.Context)
				op = Op{C: cid, Op: "put", K: key, T0: t0, T1: time.Now().UnixNano(), OK: err == nil}
				if err != nil {
					op.Err = err.Error()
				} else {
					op.V = toHV(v)
					okPut.Add(1)
				}
				nPut.Add(1)
				ops <- op
			}
		}(i)
	}
	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	close(ops)
	<-done
	log.Printf("%s: gets %d/%d ok, puts %d/%d ok", id, okGet.Load(), nGet.Load(), okPut.Load(), nPut.Load())
}

type nodeOut struct {
	Keys      int             `json:"keys"`
	Hints     int             `json:"hints"`
	Recovered int             `json:"recovered"`
	Data      map[string][]HV `json:"data"`
}

func nodeJSON(st store.NodeState) nodeOut {
	o := nodeOut{Keys: len(st.Data), Hints: st.HintCount, Recovered: st.Recovered, Data: map[string][]HV{}}
	for k, vs := range st.Data {
		o.Data[k] = toHV(vs...)
	}
	return o
}

// verify runs anti-entropy everywhere, reads every key with R=N and dumps
// every node, so convergence and lost writes can be checked.
func verify(tr *store.TCPTransport, ring *store.Ring, bw *bufio.Writer, addrs map[string]string, keys, n int) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for round := 0; round < 2; round++ {
		for id := range addrs {
			tr.TriggerSync(ctx, id)
		}
	}
	time.Sleep(300 * time.Millisecond) // hinted handoff / read repair in flight
	cl := store.NewClient("verify", tr, ring, n, n, n, 2*time.Second)
	res := struct {
		Final  map[string][]HV    `json:"final"`
		Nodes  map[string]nodeOut `json:"nodes"`
		Unread []string           `json:"unread,omitempty"`
	}{Final: map[string][]HV{}, Nodes: map[string]nodeOut{}}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		g, err := cl.Get(key)
		if err != nil {
			res.Unread = append(res.Unread, key)
			continue
		}
		res.Final[key] = toHV(g.Versions...)
	}
	time.Sleep(200 * time.Millisecond)
	for id := range addrs {
		st, err := tr.Dump(ctx, id)
		if err != nil {
			log.Printf("dump %s: %v", id, err)
			continue
		}
		res.Nodes[id] = nodeJSON(st)
	}
	json.NewEncoder(bw).Encode(res)
}
