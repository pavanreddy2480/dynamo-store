// This file defines the client API of the store: Client, which routes Get
// and Put requests to a coordinator chosen among a key's replicas,
// GetResult, which carries the siblings and causal context returned by a
// read, and GetResolved, which reconciles siblings with a Resolver and
// writes the result back.

package store

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"dynamostore/vclock"
)

// Client talks to the cluster. R and W are exposed as client settings and
// travel with every request; Sloppy enables sloppy quorums + hinted handoff.
//
// Create clients with Cluster.NewClient. A Client holds no per-request
// state, so one client may issue requests from several goroutines; its
// exported settings should be set before it is shared.
type Client struct {
	ID      string        // network endpoint id of this client
	R, W    int           // read and write quorums, each in [1, N]
	Sloppy  bool          // use fallback nodes when preferred replicas are unreachable
	Timeout time.Duration // how long to wait for the coordinator's answer
	// FirstCoordinator always routes to the first reachable preferred
	// replica instead of a random one (compared in cmd/archbench).
	FirstCoordinator bool

	net  *Network // network the client sends on
	ring *Ring    // ring used to find a key's replicas
	n    int      // replicas per key
}

// GetResult holds every concurrent version found plus the context (merged
// clock) to pass back on the next Put.
type GetResult struct {
	Versions []Version // concurrent versions (siblings); empty if the key is absent
	Context  vclock.VC // merged clock of all Versions; pass it to the next Put
}

// Values returns just the sibling values.
//
// Returns:
//   - []string: the Value of each version, in the same order as Versions.
func (g GetResult) Values() []string {
	out := make([]string, len(g.Versions))
	for i, v := range g.Versions {
		out[i] = v.Value
	}
	return out
}

// coordinator picks a random preferred replica this client can reach (load
// spreads across the N replicas, so a read may land on a replica the last
// write has not reached yet). With sloppy quorums, if no preferred replica
// is reachable, the first reachable node further along the ring is used.
//
// Parameters:
//   - key: the key the request is for.
//
// Returns:
//   - string: the id of the chosen coordinator.
//   - error: nil on success; wraps ErrUnavailable if no suitable node is
//     reachable from this client.
func (cl *Client) coordinator(key string) (string, error) {
	if cl.FirstCoordinator {
		for _, id := range cl.ring.PreferenceList(key, cl.n) {
			if cl.net.Reachable(cl.ID, id) {
				return id, nil
			}
		}
		// No preferred replica reachable: fall through, so the sloppy
		// fallback below still applies.
	}
	var live []string
	for _, id := range cl.ring.PreferenceList(key, cl.n) {
		if cl.net.Reachable(cl.ID, id) {
			live = append(live, id)
		}
	}
	if len(live) > 0 {
		return live[rand.Intn(len(live))], nil
	}
	if cl.Sloppy {
		// Walk starts with the (unreachable) preferred replicas, so the
		// first reachable entry is the nearest fallback on the ring.
		for _, id := range cl.ring.Walk(key) {
			if cl.net.Reachable(cl.ID, id) {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("%w: no reachable coordinator", ErrUnavailable)
}

// Get reads key from the cluster using the client's read quorum R.
//
// The request goes to one coordinator, which asks all N replicas and
// answers once R of them reply. Concurrent versions come back as siblings.
//
// Parameters:
//   - key: the key to read.
//
// Returns:
//   - GetResult: every concurrent version found, plus the merged context to
//     pass to the next Put. When the error wraps ErrUnavailable it still
//     holds whatever the replies that did arrive contained.
//   - error: nil on success; wraps ErrUnavailable if no coordinator was
//     reachable or fewer than R replicas answered, or is ErrTimeout if the
//     coordinator did not answer in time.
func (cl *Client) Get(key string) (GetResult, error) {
	coord, err := cl.coordinator(key)
	if err != nil {
		return GetResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cl.Timeout)
	defer cancel()
	resp, err := cl.net.Send(ctx, cl.ID, coord, coordGetReq{Key: key, R: cl.R, Sloppy: cl.Sloppy})
	if err != nil {
		return GetResult{}, err
	}
	r := resp.(coordGetResp)
	// The context is the union of every sibling's clock, so a Put made with
	// it supersedes all of them at once.
	res := GetResult{Versions: r.Versions, Context: vclock.New()}
	for _, v := range r.Versions {
		res.Context = vclock.Merge(res.Context, v.Clock())
	}
	return res, r.Err
}

// Put writes value with write quorum W. ctx must be the Context from a
// previous Get (or nil for a blind first write); the new version supersedes
// every version that context covers.
//
// A failed quorum is not a rollback: the coordinator may already have
// stored the version, and it can resurface later as a sibling.
//
// Parameters:
//   - key: the key to write.
//   - value: the new value.
//   - ctx: the causal context from a previous Get, or nil.
//
// Returns:
//   - Version: the version the coordinator created (zero if the request
//     never reached a coordinator or its reply was lost).
//   - error: nil on success; wraps ErrUnavailable if no coordinator was
//     reachable or fewer than W replicas acknowledged, or is ErrTimeout if
//     the coordinator did not answer in time.
func (cl *Client) Put(key, value string, ctx vclock.VC) (Version, error) {
	if ctx == nil {
		ctx = vclock.New()
	}
	coord, err := cl.coordinator(key)
	if err != nil {
		return Version{}, err
	}
	cctx, cancel := context.WithTimeout(context.Background(), cl.Timeout)
	defer cancel()
	req := coordPutReq{Key: key, Value: value, Context: ctx, W: cl.W, Sloppy: cl.Sloppy}
	resp, err := cl.net.Send(cctx, cl.ID, coord, req)
	if err != nil {
		return Version{}, err
	}
	r := resp.(coordPutResp)
	return r.Version, r.Err
}

// Resolver merges sibling values into one.
//
// It must be deterministic and should not drop information the application
// cares about; UnionResolver is the rule used in this package.
type Resolver func(values []string) string

// GetResolved reads key and, if siblings came back, merges them with
// resolve and writes the result back under the merged context so the
// siblings collapse into a single version.
//
// Parameters:
//   - key: the key to read and, if needed, reconcile.
//   - resolve: the rule that merges sibling values into one.
//
// Returns:
//   - string: the resolved value ("" if the key is absent).
//   - int: the number of versions the read returned (0, 1, or the number
//     of siblings that were merged).
//   - error: the error from Get, or from the reconciling Put if one was
//     needed.
func (cl *Client) GetResolved(key string, resolve Resolver) (string, int, error) {
	res, err := cl.Get(key)
	if err != nil {
		return "", 0, err
	}
	switch len(res.Versions) {
	case 0:
		return "", 0, nil
	case 1:
		return res.Versions[0].Value, 1, nil
	}
	merged := resolve(res.Values())
	_, err = cl.Put(key, merged, res.Context)
	return merged, len(res.Versions), err
}
