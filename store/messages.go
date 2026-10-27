// This file defines the wire messages exchanged over the simulated Network:
// replica-level requests (putReq, getReq, syncReq) that only touch one
// node's local state, and coordinator-level requests (coordPutReq,
// coordGetReq) that a client sends to a coordinator, which then fans them
// out to the key's replicas. It also defines ErrUnavailable, the error
// reported when a quorum cannot be reached.

package store

import (
	"errors"

	"dynamostore/vclock"
)

// ErrUnavailable means fewer than R (or W) replicas answered in time.
//
// Callers should test for it with errors.Is: coordinators wrap it with the
// number of replies or acknowledgements they actually collected.
var ErrUnavailable = errors.New("quorum not reached")

// Messages exchanged between clients and replicas. Every request is
// answered by exactly one response type; a missing response means the
// message or its reply was dropped.
//
// Messages are passed by value through Network.Send. Slices of Version
// inside them may be shared with the sender, so receivers clone them
// (cloneVersions) before storing them under their own lock.
type (
	// putReq asks a replica to merge V into what it stores for Key. With
	// Hint set, the receiver is a fallback node holding the versions on
	// behalf of Hint until hinted handoff can deliver them.
	putReq struct {
		Key  string    // key being written
		V    []Version // versions to merge into the receiver's sibling set
		Hint string    // intended owner when stored on a fallback node
	}

	// putResp acknowledges a putReq. It carries no data: receiving it at
	// all is the acknowledgement.
	putResp struct{}

	// getReq asks a replica for its current versions of Key.
	getReq struct {
		Key string // key being read
	}

	// getResp answers a getReq with the replica's sibling set, including
	// any versions it holds as hints for other nodes.
	getResp struct {
		Versions []Version // the replica's current versions of the key
	}

	// coordPutReq is a client write sent to a coordinator. Context is the
	// clock from the client's last Get; the new version supersedes exactly
	// the versions that context covers.
	coordPutReq struct {
		Key     string    // key being written
		Value   string    // new value
		Context vclock.VC // causal context from the client's last Get
		W       int       // write quorum: acknowledgements required
		Sloppy  bool      // allow fallback nodes for unreachable replicas
	}

	// coordPutResp is the coordinator's answer to a coordPutReq.
	coordPutResp struct {
		Version Version // the version the coordinator created
		Acks    int     // acknowledgements collected (including its own)
		Err     error   // wraps ErrUnavailable when Acks < W
	}

	// coordGetReq is a client read sent to a coordinator.
	coordGetReq struct {
		Key    string // key being read
		R      int    // read quorum: replies required
		Sloppy bool   // allow fallback nodes for unreachable replicas
	}

	// coordGetResp is the coordinator's answer to a coordGetReq.
	coordGetResp struct {
		Versions []Version // merged sibling set from the first R replies
		Replies  int       // successful replies merged into Versions
		Err      error     // wraps ErrUnavailable when Replies < R
	}

	// syncReq carries one side of an anti-entropy exchange: every key the
	// sender owns that the receiver should also hold. The reply carries the
	// receiver's half.
	syncReq struct {
		Items map[string][]Version // key -> sender's versions
	}

	// syncResp is the receiver's half of an anti-entropy exchange.
	syncResp struct {
		Items map[string][]Version // key -> receiver's versions
	}
)
