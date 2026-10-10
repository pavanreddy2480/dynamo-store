package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SyncMode says when a write-ahead-log record is durable relative to the
// acknowledgement a replica sends.
type SyncMode int

const (
	// SyncNone keeps state only in memory; a crash loses everything.
	SyncNone SyncMode = iota
	// SyncAlways writes and fsyncs every record before the replica acks.
	// Concurrent appends share one fsync (group commit).
	SyncAlways
	// SyncAsync buffers records in memory and flushes them to the OS every
	// FlushInterval. Replicas ack before the record is written, so a crash
	// can lose the last few acknowledged writes on that node.
	SyncAsync
)

func (m SyncMode) String() string {
	return [...]string{"none", "fsync", "async"}[m]
}

// ParseSyncMode parses "none", "fsync" or "async".
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "none", "":
		return SyncNone, nil
	case "fsync", "sync", "always":
		return SyncAlways, nil
	case "async":
		return SyncAsync, nil
	}
	return 0, fmt.Errorf("unknown wal mode %q", s)
}

// walRecord is one logged mutation. Replaying records in any order rebuilds
// the same state because every mutation is a commutative merge.
type walRecord struct {
	Kind   byte // recData, recHint, recHintDone, recIssued
	Key    string
	Owner  string // hint owner
	V      []Version
	Issued uint64
}

const (
	recData byte = iota + 1
	recHint
	recHintDone
	recIssued
)

// WAL is an append-only log of node mutations, framed as
// [length uint32][crc32 uint32][gob record] so a torn tail is detected and
// ignored on recovery.
type WAL struct {
	path  string
	mode  SyncMode
	flush time.Duration

	mu      sync.Mutex
	f       *os.File
	bw      *bufio.Writer
	written uint64 // records appended
	synced  uint64 // records known to be fsynced
	syncing bool
	cond    *sync.Cond

	stop chan struct{}
	done chan struct{}
}

// OpenWAL opens (creating if needed) the log for node id under dir.
func OpenWAL(dir, id string, mode SyncMode, flush time.Duration) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{path: filepath.Join(dir, id+".wal"), mode: mode, flush: flush}
	w.cond = sync.NewCond(&w.mu)
	return w, nil
}

// Path returns the log file path.
func (w *WAL) Path() string { return w.path }

// replay reads every intact record and stops at the first torn or corrupt one.
func (w *WAL) replay(apply func(walRecord)) (int, error) {
	f, err := os.Open(w.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	n := 0
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return n, nil // clean end or torn header
		}
		size := binary.BigEndian.Uint32(hdr[:4])
		sum := binary.BigEndian.Uint32(hdr[4:])
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil || crc32.ChecksumIEEE(buf) != sum {
			return n, nil // torn or corrupt tail: everything before it is valid
		}
		var rec walRecord
		if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&rec); err != nil {
			return n, nil
		}
		apply(rec)
		n++
	}
}

// rewrite atomically replaces the log with recs (a compacted checkpoint)
// and opens it for appending.
func (w *WAL) rewrite(recs []walRecord) error {
	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, r := range recs {
		if err := writeFrame(bw, r); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(w.path)); err == nil {
		d.Sync()
		d.Close()
	}
	w.f, err = os.OpenFile(w.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.bw = bufio.NewWriterSize(w.f, 1<<16)
	if w.mode == SyncAsync {
		w.stop, w.done = make(chan struct{}), make(chan struct{})
		go w.flushLoop()
	}
	return nil
}

func writeFrame(bw *bufio.Writer, rec walRecord) error {
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(rec); err != nil {
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(body.Len()))
	binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(body.Bytes()))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	_, err := bw.Write(body.Bytes())
	return err
}

func (w *WAL) flushLoop() {
	defer close(w.done)
	t := time.NewTicker(w.flush)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.mu.Lock()
			w.bw.Flush()
			w.mu.Unlock()
		}
	}
}

// append logs recs. With SyncAlways it returns only after they are on disk;
// concurrent callers are batched into a single fsync (group commit).
func (w *WAL) append(recs ...walRecord) error {
	if w == nil || w.mode == SyncNone || len(recs) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range recs {
		if err := writeFrame(w.bw, r); err != nil {
			return err
		}
	}
	w.written++
	if w.mode != SyncAlways {
		return nil
	}
	my := w.written
	for w.synced < my {
		if w.syncing {
			w.cond.Wait()
			continue
		}
		// Become the leader: flush and fsync everything written so far.
		w.syncing = true
		target := w.written
		err := w.bw.Flush()
		f := w.f
		w.mu.Unlock()
		if err == nil {
			err = f.Sync()
		}
		w.mu.Lock()
		w.syncing = false
		if err != nil {
			w.cond.Broadcast()
			return err
		}
		w.synced = target
		w.cond.Broadcast()
	}
	return nil
}

// Close flushes and closes the log (a clean shutdown).
func (w *WAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	if w.stop != nil {
		close(w.stop)
		<-w.done
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bw.Flush()
	w.f.Sync()
	return w.f.Close()
}

// abandon simulates a process crash: buffered records are discarded and
// the file is closed without flushing.
func (w *WAL) abandon() {
	if w == nil || w.f == nil {
		return
	}
	if w.stop != nil {
		close(w.stop)
		<-w.done
	}
	w.mu.Lock()
	w.f.Close()
	w.mu.Unlock()
}
