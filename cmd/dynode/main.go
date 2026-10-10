// Command dynode runs one storage node as its own process, talking to its
// peers over TCP and logging every mutation to a write-ahead log.
//
//	dynode -id n1 -listen :7000 -peers n1=10.0.0.11:7000,n2=10.0.0.12:7000,... \
//	       -data /data -wal fsync
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"dynamostore/store"
)

// ParsePeers turns "n1=host:port,n2=host:port" into a map.
func parsePeers(s string) map[string]string {
	out := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		if id, addr, ok := strings.Cut(strings.TrimSpace(p), "="); ok {
			out[id] = addr
		}
	}
	return out
}

func main() {
	id := flag.String("id", "", "node id (must appear in -peers)")
	listen := flag.String("listen", ":7000", "listen address")
	peers := flag.String("peers", "", "all nodes: id=host:port,...")
	dataDir := flag.String("data", "/data", "WAL directory")
	walMode := flag.String("wal", "fsync", "WAL mode: fsync | async | none")
	walFlush := flag.Duration("wal-flush", 10*time.Millisecond, "flush interval for -wal async")
	n := flag.Int("n", 3, "replicas per key")
	timeout := flag.Duration("timeout", 20*time.Millisecond, "replica timeout")
	handoff := flag.Duration("handoff", 50*time.Millisecond, "hinted handoff interval")
	entropy := flag.Duration("entropy", time.Second, "anti-entropy interval (0 = off)")
	readRepair := flag.Bool("read-repair", true, "repair stale replicas after reads")
	delayBase := flag.Duration("link-delay", 0, "emulated extra one-way delay on node-to-node links")
	delayJitter := flag.Duration("link-jitter", 0, "mean of exponential jitter added to -link-delay")
	flag.Parse()

	addrs := parsePeers(*peers)
	if _, ok := addrs[*id]; !ok {
		log.Fatalf("node %q not in -peers", *id)
	}
	mode, err := store.ParseSyncMode(*walMode)
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, 0, len(addrs))
	for k := range addrs {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	ring := store.NewRing(ids, 64)

	tr := store.NewTCPTransport(*id, addrs)
	tr.LinkDelay = store.LatencyModel{Base: *delayBase, JitterMean: *delayJitter}
	var wal *store.WAL
	if mode != store.SyncNone {
		if wal, err = store.OpenWAL(*dataDir, *id, mode, *walFlush); err != nil {
			log.Fatal(err)
		}
	}
	t0 := time.Now()
	nd, err := store.NewNode(*id, store.NodeConfig{
		N:               *n,
		Policy:          store.KeepSiblings,
		ReplicaTimeout:  *timeout,
		ReadRepair:      *readRepair,
		HandoffInterval: *handoff,
		EntropyInterval: *entropy,
	}, tr, ring, wal)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: wal=%s recovered %d records in %v", *id, mode, nd.Recovered(), time.Since(t0).Round(time.Millisecond))
	if err := tr.Listen(*listen, nd.Handle); err != nil {
		log.Fatal(err)
	}
	tr.StartDetector(100 * time.Millisecond)
	nd.Start()
	log.Printf("%s: serving on %s, ring %v", *id, *listen, ids)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("%s: shutting down cleanly", *id)
	tr.Close()
	nd.Close()
}
