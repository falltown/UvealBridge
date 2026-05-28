package replication

import "context"

// LocalReplicator is the trivial Replicator: it just calls the local
// Applier and returns the result. No network, no consensus.
//
// Two reasons it exists:
//  1. Lets us refactor the HTTP and store layers to the Replicator
//     contract before any Raft code is in the tree, so adding Raft
//     becomes a one-package change.
//  2. Is the "Raft off" build flavor — flip a flag in main.go and
//     the binary runs as a single-node KV with no consensus overhead,
//     useful for benchmarks, local dev, and quickly bisecting whether
//     a bug is in Raft or in the store.
type LocalReplicator struct {
	a Applier
}

// NewLocalReplicator wraps a as a no-op Replicator.
func NewLocalReplicator(a Applier) *LocalReplicator {
	return &LocalReplicator{a: a}
}

// Propose forwards cmd to the underlying Applier. Honors context
// cancellation so callers with a deadline get back fast even though
// the local path is synchronous and very fast.
func (l *LocalReplicator) Propose(ctx context.Context, cmd Command) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.a.Apply(cmd)
}

// Compile-time check.
var _ Replicator = (*LocalReplicator)(nil)
