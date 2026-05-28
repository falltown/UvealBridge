package replication

import "context"

// Replicator is the boundary between "I want this mutation to happen"
// (the HTTP layer) and "make sure a majority of replicas agree"
// (Raft, eventually). It is the only place that knows whether the
// system is replicated or single-node.
//
// Propose takes a Command, gets it accepted by whatever consensus
// scheme is in use, waits for the apply on the local node, and returns
// the apply result. Errors are real: not-leader, context expired,
// transport failure, node shutting down. The caller (HTTP layer) is
// expected to map them to status codes.
//
// Reads do NOT go through Replicator. They hit the store directly
// because they don't need quorum (yet — linearizable reads are a
// later, opt-in concern).
//
// Implementations:
//   - LocalReplicator in local.go (no consensus; trivial pass-through)
//   - RaftReplicator  in raft.go  (etcd/raft-backed; single- or multi-node)
type Replicator interface {
	Propose(ctx context.Context, cmd Command) (result any, err error)
}
