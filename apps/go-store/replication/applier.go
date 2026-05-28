package replication

// Applier is the finite-state-machine interface that concrete stores
// must satisfy so the replication layer can drive them.
//
// Exactly one Apply call happens per committed Command, in the order
// Raft committed them. The implementation MUST be deterministic:
// given the same prior state and the same Command, every replica
// must reach the same next state and return the same result. This is
// what makes the replicated log equivalent to direct invocation.
//
// Result is whatever the caller (typically the HTTP layer) needs to
// turn into a response — e.g. CmdDelete returns a bool that becomes
// 204 vs 404. Apply returns an error only for truly invalid commands
// (unknown kind, etc.); business outcomes like "key didn't exist"
// belong in the result, not the error.
type Applier interface {
	Apply(cmd Command) (result any, err error)
}
