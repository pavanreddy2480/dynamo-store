// Command realbench runs the store as real processes: every node is its own
// Docker container talking TCP, partitions are iptables rules on the Docker
// bridge, and crashes are `docker kill -s KILL`. Load generators record
// operation histories which are analysed here. Results go to
// results/realnet.md.
//
// Needs a Linux host with Docker and iptables (run as root). Build first:
//
//	CGO_ENABLED=0 go build -o bin/ ./cmd/dynode ./cmd/dyload
//	go run ./cmd/realbench
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dynamostore/store"
)

var (
	image  = flag.String("image", "dynamo-store:dev", "image with /dynode and /dyload")
	binDir = flag.String("bin", "bin", "directory with static dynode and dyload binaries")
	outDir = flag.String("out", "results", "results directory")
	dur    = flag.Duration("duration", 4*time.Second, "load duration per run")
	only   = flag.String("only", "", "comma-separated experiments to run (R1,R2,R3,R4); empty = all")
)

const (
	netName = "dynstore"
	subnet  = "172.28.0.0/24"
	chain   = "DYNSTORE"
)

var nodeIDs = []string{"n1", "n2", "n3", "n4", "n5"}

func nodeIP(id string) string { return "172.28.0.1" + id[1:] } // n1 -> 172.28.0.11

func peers() string {
	var p []string
	for _, id := range nodeIDs {
		p = append(p, fmt.Sprintf("%s=%s:7000", id, nodeIP(id)))
	}
	return strings.Join(p, ",")
}

var md strings.Builder

func w(format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	fmt.Print(s)
	md.WriteString(s)
}

func sh(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func must(name string, args ...string) string {
	out, err := sh(name, args...)
	if err != nil {
		log.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

// ------------------------------------------------------------------ cluster

type clusterOpts struct {
	wal       string // fsync | async | none
	walFlush  time.Duration
	linkDelay time.Duration
	linkJit   time.Duration
}

func buildImage() {
	ctx, err := os.MkdirTemp("", "dynimg")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(ctx)
	for _, f := range []string{"dynode", "dyload"} {
		b, err := os.ReadFile(filepath.Join(*binDir, f))
		if err != nil {
			log.Fatalf("missing %s: build with CGO_ENABLED=0 go build -o bin/ ./cmd/dynode ./cmd/dyload", f)
		}
		os.WriteFile(filepath.Join(ctx, f), b, 0o755)
	}
	df, _ := os.ReadFile("deploy/Dockerfile")
	os.WriteFile(filepath.Join(ctx, "Dockerfile"), df, 0o644)
	must("docker", "build", "-q", "-t", *image, ctx)
}

func teardown() {
	var names []string
	for _, id := range nodeIDs {
		names = append(names, id)
	}
	sh("docker", append([]string{"rm", "-f"}, append(names, "loadA", "loadB", "verifier")...)...)
	for _, id := range nodeIDs {
		sh("docker", "volume", "rm", "-f", "dynvol-"+id)
	}
	sh("iptables", "-F", chain)
}

func startNode(id string, o clusterOpts) {
	must("docker", "run", "-d", "--name", id, "--network", netName, "--ip", nodeIP(id),
		"-v", "dynvol-"+id+":/data", *image,
		"-id", id, "-peers", peers(), "-wal", o.wal, "-wal-flush", flushOf(o).String(),
		"-link-delay", o.linkDelay.String(), "-link-jitter", o.linkJit.String())
}

func flushOf(o clusterOpts) time.Duration {
	if o.walFlush == 0 {
		return 10 * time.Millisecond
	}
	return o.walFlush
}

// walModes are the durability settings compared in R3.
var walModes = []struct {
	name string
	o    clusterOpts
}{
	{"fsync (group commit)", clusterOpts{wal: "fsync"}},
	{"async, flush every 10ms", clusterOpts{wal: "async", walFlush: 10 * time.Millisecond}},
	{"async, flush every 1s", clusterOpts{wal: "async", walFlush: time.Second}},
	{"none (memory only)", clusterOpts{wal: "none"}},
}

func startCluster(o clusterOpts) {
	teardown()
	for _, id := range nodeIDs {
		startNode(id, o)
	}
	waitServing(nodeIDs...)
}

func waitServing(ids ...string) {
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range ids {
		for !strings.Contains(logs(id), "serving on") {
			if time.Now().After(deadline) {
				log.Fatalf("%s did not start:\n%s", id, logs(id))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	time.Sleep(300 * time.Millisecond)
}

func logs(id string) string { out, _ := sh("docker", "logs", id); return out }

// partition drops all traffic between the two groups of IPs.
func partition(a, b []string) {
	for _, x := range a {
		for _, y := range b {
			must("iptables", "-A", chain, "-s", x, "-d", y, "-j", "DROP")
			must("iptables", "-A", chain, "-s", y, "-d", x, "-j", "DROP")
		}
	}
}

func heal() { must("iptables", "-F", chain) }

type loadOpts struct {
	name, ip  string
	r, w      int
	sloppy    bool
	clients   int
	writeFrac float64
	dur       time.Duration
}

// runLoad runs a load generator container and returns its parsed history.
func runLoad(raw string, o loadOpts) []Op {
	file := o.name + ".jsonl"
	out, err := sh("docker", "run", "--rm", "--name", o.name, "--network", netName, "--ip", o.ip,
		"-v", raw+":/out", "--entrypoint", "/dyload", *image, "run",
		"-peers", peers(), "-id", o.name, "-r", fmt.Sprint(o.r), "-w", fmt.Sprint(o.w),
		fmt.Sprintf("-sloppy=%v", o.sloppy), "-clients", fmt.Sprint(o.clients),
		"-write-frac", fmt.Sprint(o.writeFrac), "-duration", o.dur.String(), "-out", "/out/"+file)
	if err != nil {
		log.Fatalf("load %s: %v\n%s", o.name, err, out)
	}
	return readOps(filepath.Join(raw, file))
}

func verify(raw, name string) verifyOut {
	must("docker", "run", "--rm", "--name", "verifier", "--network", netName, "--ip", "172.28.0.110",
		"-v", raw+":/out", "--entrypoint", "/dyload", *image, "verify",
		"-peers", peers(), "-keys", "64", "-out", "/out/"+name)
	var v verifyOut
	b, _ := os.ReadFile(filepath.Join(raw, name))
	json.Unmarshal(b, &v)
	return v
}

func dump(raw, node, name string) nodeOut {
	must("docker", "run", "--rm", "--name", "verifier", "--network", netName, "--ip", "172.28.0.110",
		"-v", raw+":/out", "--entrypoint", "/dyload", *image, "dump",
		"-peers", peers(), "-node", node, "-out", "/out/"+name)
	var n nodeOut
	b, _ := os.ReadFile(filepath.Join(raw, name))
	json.Unmarshal(b, &n)
	return n
}

// ------------------------------------------------------------------ history

type HV struct {
	N string            `json:"n"`
	C uint64            `json:"c"`
	X map[string]uint64 `json:"x,omitempty"`
}

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

type nodeOut struct {
	Keys      int             `json:"keys"`
	Hints     int             `json:"hints"`
	Recovered int             `json:"recovered"`
	Data      map[string][]HV `json:"data"`
}

type verifyOut struct {
	Final  map[string][]HV    `json:"final"`
	Nodes  map[string]nodeOut `json:"nodes"`
	Unread []string           `json:"unread"`
}

func readOps(path string) []Op {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var ops []Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var op Op
		if json.Unmarshal(sc.Bytes(), &op) == nil {
			ops = append(ops, op)
		}
	}
	return ops
}

// covers: v's writer had seen a (or v is a).
func covers(v, a HV) bool { return (v.N == a.N && v.C == a.C) || v.X[a.N] >= a.C }

func coveredBy(vs []HV, a HV) bool {
	for _, v := range vs {
		if covers(v, a) {
			return true
		}
	}
	return false
}

type stats struct {
	gets, getOK, puts, putOK int
	getLat, putLat           []float64 // ms, successful ops
	stale, multi             int
}

func analyse(ops []Op) stats {
	var s stats
	// Acknowledged puts per key, sorted by completion time.
	acked := map[string][]Op{}
	for _, op := range ops {
		if op.Op == "put" && op.OK && len(op.V) == 1 {
			acked[op.K] = append(acked[op.K], op)
		}
	}
	for _, ps := range acked {
		sort.Slice(ps, func(i, j int) bool { return ps[i].T1 < ps[j].T1 })
	}
	for _, op := range ops {
		ms := float64(op.T1-op.T0) / 1e6
		switch op.Op {
		case "put":
			s.puts++
			if op.OK {
				s.putOK++
				s.putLat = append(s.putLat, ms)
			}
		case "get":
			s.gets++
			if !op.OK {
				continue
			}
			s.getOK++
			s.getLat = append(s.getLat, ms)
			if len(op.V) > 1 {
				s.multi++
			}
			// Stale: some write acknowledged before this read started is
			// neither returned nor superseded by what was returned.
			for _, p := range acked[op.K] {
				if p.T1 >= op.T0 {
					break
				}
				if !coveredBy(op.V, p.V[0]) {
					s.stale++
					break
				}
			}
		}
	}
	return s
}

func pct(xs []float64, p float64) string {
	if len(xs) == 0 {
		return "-"
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	return fmt.Sprintf("%.2f", c[int(p*float64(len(c)-1))])
}

func ratio(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(a)/float64(b))
}

// lost counts acknowledged puts (completed before cutoff, if non-zero) that
// no final version covers.
func lost(ops []Op, final map[string][]HV, cutoff int64) (n, total int) {
	for _, op := range ops {
		if op.Op != "put" || !op.OK || len(op.V) != 1 || (cutoff > 0 && op.T1 >= cutoff) {
			continue
		}
		total++
		if !coveredBy(final[op.K], op.V[0]) {
			n++
		}
	}
	return
}

// converged: every key's preferred replicas hold the same versions.
func converged(v verifyOut) (bool, int) {
	ring := store.NewRing(nodeIDs, 64)
	div := 0
	for k := 0; k < 64; k++ {
		key := fmt.Sprintf("k%d", k)
		var first string
		for i, id := range ring.PreferenceList(key, 3) {
			s := dotSet(v.Nodes[id].Data[key])
			if i == 0 {
				first = s
			} else if s != first {
				div++
				break
			}
		}
	}
	return div == 0 && len(v.Unread) == 0, div
}

func dotSet(vs []HV) string {
	var s []string
	for _, v := range vs {
		s = append(s, fmt.Sprintf("%s:%d", v.N, v.C))
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

// ------------------------------------------------------------------ experiments

func rawDir(name string) string {
	d, _ := filepath.Abs(filepath.Join(*outDir, "raw", name))
	os.RemoveAll(d)
	os.MkdirAll(d, 0o777)
	os.Chmod(d, 0o777)
	return d
}

var allNodes = func() []string {
	var ips []string
	for _, id := range nodeIDs {
		ips = append(ips, nodeIP(id))
	}
	return ips
}()

func r1QuorumSweep() {
	w("## R1. Quorum sweep on real processes (no failures)\n\n")
	w("One load generator container, 8 clients, 64 keys, every read followed by a write half the time, %v per row, WAL in fsync mode. ", *dur)
	w("\"LAN\" is the plain Docker bridge. \"Cross-zone\" adds an emulated 0.4ms + Exp(0.8ms) delay to each node-to-node message inside the transport, because this host has no `tc netem`.\n\n")
	w("| links | R,W | R+W>N | get p50 / p99 ms | put p50 / p99 ms | ops/s | stale reads | reads with siblings |\n|---|---|---|---|---|---|---|---|\n")
	for _, link := range []struct {
		name   string
		d, jit time.Duration
	}{{"LAN", 0, 0}, {"cross-zone", 400 * time.Microsecond, 800 * time.Microsecond}} {
		for _, q := range [][2]int{{1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 3}} {
			startCluster(clusterOpts{wal: "fsync", linkDelay: link.d, linkJit: link.jit})
			raw := rawDir(fmt.Sprintf("r1-%s-r%dw%d", link.name, q[0], q[1]))
			ops := runLoad(raw, loadOpts{name: "loadA", ip: "172.28.0.101", r: q[0], w: q[1], clients: 8, writeFrac: 0.5, dur: *dur})
			s := analyse(ops)
			w("| %s | %d,%d | %v | %s / %s | %s / %s | %.0f | %s | %s |\n", link.name, q[0], q[1], q[0]+q[1] > 3,
				pct(s.getLat, .5), pct(s.getLat, .99), pct(s.putLat, .5), pct(s.putLat, .99),
				float64(s.getOK+s.putOK)/dur.Seconds(), ratio(s.stale, s.getOK), ratio(s.multi, s.getOK))
		}
	}
}

func r2Partition() {
	w("\n## R2. Real network partition with iptables\n\n")
	w("Containers n1,n2 + load generator A on one side; n3,n4,n5 + load generator B on the other. Every packet between the sides is dropped by iptables rules on the Docker bridge. 6 clients per side, 64 keys, %v. Then the rules are removed, the cluster runs anti-entropy, and a verifier reads every key with R=3.\n\n", *dur)
	w("| quorum | R,W | A get / put avail | B get / put avail | A get p50 ms | stale A / B | keys with siblings after heal | acked writes lost | replicas converged |\n|---|---|---|---|---|---|---|---|---|\n")
	sideA := []string{nodeIP("n1"), nodeIP("n2"), "172.28.0.101"}
	sideB := []string{nodeIP("n3"), nodeIP("n4"), nodeIP("n5"), "172.28.0.102"}
	for _, c := range []struct {
		r, w   int
		sloppy bool
	}{{1, 1, false}, {2, 2, false}, {3, 3, false}, {1, 1, true}, {2, 2, true}} {
		startCluster(clusterOpts{wal: "fsync"})
		mode := "strict"
		if c.sloppy {
			mode = "sloppy"
		}
		raw := rawDir(fmt.Sprintf("r2-%s-r%dw%d", mode, c.r, c.w))
		partition(sideA, sideB)
		var a, b []Op
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			a = runLoad(raw, loadOpts{name: "loadA", ip: "172.28.0.101", r: c.r, w: c.w, sloppy: c.sloppy, clients: 6, writeFrac: 0.5, dur: *dur})
		}()
		go func() {
			defer wg.Done()
			b = runLoad(raw, loadOpts{name: "loadB", ip: "172.28.0.102", r: c.r, w: c.w, sloppy: c.sloppy, clients: 6, writeFrac: 0.5, dur: *dur})
		}()
		wg.Wait()
		heal()
		time.Sleep(time.Second) // failure detectors notice the healed links
		v := verify(raw, "verify.json")
		all := append(append([]Op{}, a...), b...)
		sa, sb := analyse(a), analyse(b)
		// Stale reads across sides: analyse the merged history so that a
		// read on A that misses a write acknowledged on B counts as stale.
		sall := analyse(all)
		_ = sall
		nl, nt := lost(all, v.Final, 0)
		ok, div := converged(v)
		sib := 0
		for _, vs := range v.Final {
			if len(vs) > 1 {
				sib++
			}
		}
		staleA, staleB := staleAcross(a, all), staleAcross(b, all)
		w("| %s | %d,%d | %s / %s | %s / %s | %s | %s / %s | %d/64 | %d of %d | %v (%d divergent) |\n", mode, c.r, c.w,
			ratio(sa.getOK, sa.gets), ratio(sa.putOK, sa.puts), ratio(sb.getOK, sb.gets), ratio(sb.putOK, sb.puts),
			pct(sa.getLat, .5), staleA, staleB, sib, nl, nt, ok, div)
	}
}

// staleAcross: stale-read rate of reads in mine against writes acknowledged
// anywhere (all).
func staleAcross(mine, all []Op) string {
	acked := map[string][]Op{}
	for _, op := range all {
		if op.Op == "put" && op.OK && len(op.V) == 1 {
			acked[op.K] = append(acked[op.K], op)
		}
	}
	for _, ps := range acked {
		sort.Slice(ps, func(i, j int) bool { return ps[i].T1 < ps[j].T1 })
	}
	reads, stale := 0, 0
	for _, op := range mine {
		if op.Op != "get" || !op.OK {
			continue
		}
		reads++
		for _, p := range acked[op.K] {
			if p.T1 >= op.T0 {
				break
			}
			if !coveredBy(op.V, p.V[0]) {
				stale++
				break
			}
		}
	}
	return ratio(stale, reads)
}

func r3Crash() {
	w("\n## R3. Crash and restart (kill -9) with each WAL mode\n\n")
	w("8 clients, R=1, 64 keys, %v of load. Partway through, n3 is killed with `docker kill -s KILL` (no clean shutdown, its in-flight replication dies with it). ", *dur)
	w("It is restarted from its volume while iptables still isolates it, so its recovered state can be inspected before any repair. The isolation is then lifted and the run finishes; a verifier finally reads every key with R=3.\n\n")
	w("* **n3 lost locally**: writes n3 coordinated and acknowledged before the kill that are missing from n3's state after restart (before repair).\n")
	w("* **lost cluster-wide**: acknowledged writes that no replica has after repair, so they are gone for good.\n\n")
	w("| WAL mode | W | records replayed | n3 coordinated & acked before kill | n3 lost locally | acked writes lost cluster-wide | put p50 ms | restart to serving |\n|---|---|---|---|---|---|---|---|\n")
	for mi, m := range walModes {
		mode := m.name
		for _, wq := range []int{1, 2} {
			startCluster(m.o)
			raw := rawDir(fmt.Sprintf("r3-%d-w%d", mi, wq))
			var ops []Op
			done := make(chan struct{})
			go func() {
				ops = runLoad(raw, loadOpts{name: "loadA", ip: "172.28.0.101", r: 1, w: wq, clients: 8, writeFrac: 0.7, dur: *dur})
				close(done)
			}()
			time.Sleep(*dur * 2 / 5)
			must("docker", "kill", "-s", "KILL", "n3")
			// Isolate n3 from every other node and the load generator, but
			// not from the verifier, then restart it.
			others := []string{"172.28.0.101"}
			for _, id := range nodeIDs {
				if id != "n3" {
					others = append(others, nodeIP(id))
				}
			}
			partition([]string{nodeIP("n3")}, others)
			t0 := time.Now()
			must("docker", "start", "n3")
			waitServing("n3")
			restart := time.Since(t0)
			local := dump(raw, "n3", "n3-recovered.json")
			heal()
			<-done
			time.Sleep(time.Second)
			v := verify(raw, "verify.json")
			// n3-coordinated, acknowledged before the kill.
			var mine, missing int
			for _, op := range ops {
				// Anything n3 coordinated and acknowledged happened before it died.
				if op.Op == "put" && op.OK && len(op.V) == 1 && op.V[0].N == "n3" && op.T1 < t0.UnixNano() {
					mine++
					if !coveredBy(local.Data[op.K], op.V[0]) {
						missing++
					}
				}
			}
			nl, nt := lost(ops, v.Final, 0)
			s := analyse(ops)
			w("| %s | %d | %d | %d | %d (%s) | %d of %d | %s | %v |\n", mode, wq, local.Recovered, mine, missing, ratio(missing, mine),
				nl, nt, pct(s.putLat, .5), restart.Round(time.Millisecond))
		}
	}
}

// r3bPowerLoss kills every node at the same instant (a power cut or a bad
// deploy), restarts them all, and counts acknowledged writes that are gone.
// Replication cannot help here; only the WAL can.
func r3bPowerLoss() {
	w("\n## R3b. Whole-cluster crash (all 5 nodes killed at once)\n\n")
	w("8 clients, R=1 W=2, 64 keys. Partway through the run all five containers get SIGKILL at the same moment, then all restart from their volumes. ")
	w("Note that SIGKILL kills the process but not the machine: data already handed to the OS survives it, so \"async\" only loses what was still in the process's buffer. A real power cut would also lose the OS page cache, which only fsync protects against.\n\n")
	w("| WAL mode | acked writes | lost after restart + repair | records replayed (all nodes) |\n|---|---|---|---|\n")
	for mi, m := range walModes {
		startCluster(m.o)
		raw := rawDir(fmt.Sprintf("r3b-%d", mi))
		var ops []Op
		done := make(chan struct{})
		go func() {
			ops = runLoad(raw, loadOpts{name: "loadA", ip: "172.28.0.101", r: 1, w: 2, clients: 8, writeFrac: 0.7, dur: *dur})
			close(done)
		}()
		time.Sleep(*dur / 2)
		killT := time.Now().UnixNano()
		must("docker", append([]string{"kill", "-s", "KILL"}, nodeIDs...)...)
		<-done
		must("docker", append([]string{"start"}, nodeIDs...)...)
		waitServing(nodeIDs...)
		time.Sleep(500 * time.Millisecond)
		v := verify(raw, "verify.json")
		replayed := 0
		for _, n := range v.Nodes {
			replayed += n.Recovered
		}
		_ = killT
		// Every put acknowledged at all was acknowledged before the nodes
		// died (afterwards nothing can succeed), so no cutoff is needed.
		nl, nt := lost(ops, v.Final, 0)
		w("| %s | %d | %d (%s) | %d |\n", m.name, nt, nl, ratio(nl, nt), replayed)
	}
}

func r4WALCost() {
	w("\n## R4. Cost of durability\n\n")
	w("R=W=2, 8 clients, 64 keys, no failures, %v.\n\n", *dur)
	w("| WAL mode | put p50 / p99 ms | get p50 ms | ops/s |\n|---|---|---|---|\n")
	for mi := len(walModes) - 1; mi >= 0; mi-- {
		mode := walModes[mi].name
		startCluster(walModes[mi].o)
		raw := rawDir(fmt.Sprintf("r4-%d", mi))
		s := analyse(runLoad(raw, loadOpts{name: "loadA", ip: "172.28.0.101", r: 2, w: 2, clients: 8, writeFrac: 0.5, dur: *dur}))
		w("| %s | %s / %s | %s | %.0f |\n", mode, pct(s.putLat, .5), pct(s.putLat, .99), pct(s.getLat, .5), float64(s.getOK+s.putOK)/dur.Seconds())
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	buildImage()
	sh("docker", "network", "create", "--subnet", subnet, netName)
	sh("iptables", "-N", chain)
	if _, err := sh("iptables", "-C", "DOCKER-USER", "-j", chain); err != nil {
		must("iptables", "-I", "DOCKER-USER", "-j", chain)
	}
	defer teardown()

	w("# Real-network results\n\n")
	w("Each node is a separate `dynode` process in its own Docker container (image built `FROM scratch` with static Go binaries), talking gob over TCP on a Docker bridge network. ")
	w("Partitions are iptables DROP rules on the bridge, crashes are SIGKILL. Host: %s, %d CPUs shared by all containers. Regenerate with `go run ./cmd/realbench` (Linux, root).\n\n", strings.TrimSpace(must("uname", "-sr")), numCPU())
	run := func(name string, f func()) {
		if *only == "" || strings.Contains(*only, name) {
			f()
		}
	}
	run("R1", r1QuorumSweep)
	run("R2", r2Partition)
	run("R3", r3Crash)
	run("R3b", r3bPowerLoss)
	run("R4", r4WALCost)
	path := filepath.Join(*outDir, "realnet.md")
	if *only != "" {
		path = filepath.Join(*outDir, "realnet-"+strings.ReplaceAll(*only, ",", "-")+".md")
	}
	os.WriteFile(path, []byte(md.String()), 0o644)
	fmt.Println("\nwrote", path)
}

func numCPU() int {
	out, _ := sh("nproc")
	var n int
	fmt.Sscan(out, &n)
	return int(math.Max(float64(n), 1))
}
