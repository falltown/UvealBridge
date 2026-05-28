# go-store: Replicated KV Design

A learning project: a small key-value store that grows into a Raft-replicated,
leader-elected, fault-tolerant cluster — built up one layer at a time.

This document is the running design. It explains **what we are building, why
each piece exists, and where we are right now**. Update it as decisions
change.

---

## 1. Goal

Run **N pods** (StatefulSet on Kubernetes), each holding a copy of the same
key-value map, such that:

- **Writes survive any minority failure.** Up to `floor((N-1)/2)` pods can die
  or be partitioned without losing acknowledged data.
- **Reads see a consistent view.** Eventually all live replicas agree;
  reads from the leader can be made linearizable on demand.
- **One pod at a time is "in charge"** (the leader). Followers forward writes
  to it. If the leader dies, the remaining pods automatically elect a new one.

The replication algorithm is **Raft**, implemented via `go.etcd.io/raft/v3`
(the lower-level library — we own the storage, transport, and FSM; the
library owns election, log matching, and safety rules).

---

## 2. Where we are today

A single-binary KV with:

- `store.SafeStore` — in-memory map + RWMutex.
- `store.WALStore` — same, but every mutation appended to a local
  write-ahead log so a single pod survives restart.
- `httpapi` — REST front (`PUT/GET/DELETE /kv/{key}`, `GET /healthz`).
- 3-replica StatefulSet on AKS. Each pod has its own PVC and its own
  independent WAL. **There is no replication yet** — hitting the ClusterIP
  Service round-robins across pods and gives inconsistent reads. That is
  expected and is what Raft will fix.

Every response carries an `X-Pod` header identifying which replica served
the request — handy for observing what's happening during the rollout.

---

## 3. Target architecture

```
                       ┌───────────────────────────────┐
                       │           Client              │
                       └───────────────┬───────────────┘
                                       │ HTTP
                          ┌────────────┼────────────┐
                          │            │            │
                     ┌────▼───┐   ┌────▼───┐   ┌────▼───┐
                     │ pod-0  │   │ pod-1  │   │ pod-2  │
                     │(leader)│   │(follow)│   │(follow)│
                     └────────┘   └────────┘   └────────┘
                          │            │            │
                          └────raft RPCs (HTTP)─────┘
                                       │
                          per-pod PVC (raft log + KV snapshot)
```

### Layering inside each pod

```
              ┌───────────────────────────────────────────┐
              │                httpapi                    │   transport in
              │   PUT/DELETE → Replicator.Propose(cmd)    │
              │   GET        → Store.Get / Keys / Len     │
              └───────────────────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────────┐
              │              replication                  │
              │   Command (wire format for mutations)     │
              │   Replicator interface                    │
              │   ── LocalReplicator   (no consensus)     │   step 1
              │   ── RaftReplicator    (etcd/raft node)   │   step 2+
              │   FSM glue: bytes → Command → Applier     │
              │   Transport: HTTP between peers           │
              │   Raft storage (log + HardState + snap)   │
              └───────────────────────────────────────────┘
                                  │
                                  ▼  Applier.Apply(cmd)
              ┌───────────────────────────────────────────┐
              │                 store                     │
              │   SafeStore / WALStore                    │
              │   Pure deterministic KV state machine     │
              └───────────────────────────────────────────┘
```

Key rule: **arrows only point downward.** `replication/` never imports
`store/`; the store satisfies an interface declared in `replication/`.
This keeps the storage layer ignorant of consensus.

---

## 4. Core concepts

### Command — the unit of replication
Every state-changing operation is encoded into a `Command{Kind, Key, Value}`
and serialized to a self-describing length-prefixed binary blob
(`version | kind | uvarint(len) | key | uvarint(len) | value`). The blob is
opaque to Raft. Determinism is mandatory: the same `Command` always produces
the same bytes, and applying the same bytes to the same prior state always
produces the same next state. No clocks, no randomness, no map iteration.

Reads are *not* Commands — they don't go through Raft (until we want
linearizable reads, which is a later, optional step).

### Replicator — "make this command happen on a majority"
An interface with one job:
```go
Propose(ctx, cmd Command) (any, error)
```
- `LocalReplicator` (step 1): calls `Applier.Apply(cmd)` directly. No
  network. Lets the HTTP layer + store be refactored and tested without
  Raft in the picture.
- `RaftReplicator` (step 2+): hands the marshaled bytes to `raft.Node`,
  waits for the commit to apply, returns the apply result. If this node
  isn't the leader, it returns a redirect error containing the leader's
  address (handled by the HTTP layer).

### Applier — "apply this committed command to local state"
The FSM interface the store implements:
```go
Apply(cmd Command) (any, error)
```
Both `SafeStore` and `WALStore` will implement it. For `WALStore`, the WAL
becomes redundant with the Raft log once Raft is in place — we'll keep it
for now and revisit in step 7/8.

### Leader, followers, election
Raft guarantees there is at most one leader per term. Followers forward
writes to the current leader (we'll start with a simple HTTP redirect; can
upgrade to in-cluster proxying later). On leader failure, followers time
out and start a new election. Election timeout > heartbeat interval, both
configurable.

### Quorum
A write is "committed" once a **majority** of nodes have durably appended
it to their Raft log. With N=3, quorum=2: any single pod can be down with
no impact. With N=5, quorum=3: any two can be down. Even N is wasteful —
N=4 still tolerates only one failure, same as N=3.

---

## 5. Step-by-step roadmap

Each step is independently testable. We don't move on until the current
step is green end-to-end.

| Step | What changes | Done? |
|------|--------------|-------|
| 1a | `replication.Command` + length-prefixed binary encoding | ✅ |
| 1b | `replication.Applier` interface; `Apply` on SafeStore/WALStore | ✅ |
| 1c | `replication.Replicator` interface + `LocalReplicator` (no consensus) | ✅ |
| 1d | Wire HTTP PUT/DELETE through Replicator; GET stays direct | ✅ |
| 1e | Update `main.go` + tests; cluster still behaves identically | ✅ |
| 2  | Add `go.etcd.io/raft/v3` dep. Single-node `RaftReplicator`. Prove the Ready loop drives our FSM. | ✅ |
| 3  | HTTP transport between peers (RPCs: AppendEntries, RequestVote, etc.) | ✅ |
| 4  | 3-node bootstrap. Watch elections in logs. | ✅ |
| 5  | Route writes through Raft; reads stay local (stale but fast) | ✅ |
| 6  | Leader-only writes: followers 307-redirect to current leader | |
| 7  | Persistent Raft storage (replace MemoryStorage with WAL+HardState) | |
| 8  | Snapshots — truncate the Raft log, rebuild followers from snapshots | |
| 9  | (optional) Linearizable reads via ReadIndex | |
| 10 | (optional) Dynamic membership via ConfChange | |

### Why Step 1 exists before any Raft code

Raft will hand us **ordered, opaque bytes** and expect us to apply them.
Three properties must already be true *before* Raft shows up, or we'll spend
days debugging Raft for what are actually our own bugs:

1. **Every mutation is serializable.** No direct method calls. → `Command`.
2. **Every mutation is deterministic.** Same input + same prior state →
   same next state on every replica. → enforced by the apply path.
3. **Every mutation flows through one choke point.** → `Applier.Apply`.

Step 1 establishes those three properties using a no-op replicator, so we
prove the HTTP → Command → Apply path works end-to-end. Step 2 then just
swaps the no-op for Raft — nothing in `httpapi` or `store` changes.

---

## 6. Non-goals (for now)

- Multi-key transactions / batches.
- Per-key TTL / expiration.
- Authentication / authorization.
- Cross-datacenter replication or non-voting learners.
- Compaction or value compression.
- Pluggable storage engines (BoltDB/Pebble). The WAL is intentionally tiny.

We may revisit any of these once Raft is solid. Adding them earlier would
muddy the consensus work.

---

## 7. Operational shape (target)

- StatefulSet of N pods (3 to start), each with its own PVC.
- Headless Service `go-store-headless` gives stable pod DNS:
  `go-store-{0,1,2}.go-store-headless.go-store.svc`. Raft peers use this.
- ClusterIP Service `go-store` for clients; round-robin is acceptable
  once reads are at least eventually consistent.
- Liveness probe = `/healthz` (process up).
- Readiness probe (future) = "this pod has joined the Raft cluster and
  caught up to within X entries of the leader." Until then, kube-proxy
  shouldn't send it traffic.
- Graceful shutdown: stop accepting new HTTP, transfer Raft leadership
  if we're the leader, flush state, exit.

---

## 8. Open questions / decisions to revisit

- **Read consistency default.** Stale local reads forever, or upgrade
  to ReadIndex once stable? Probably the former for the learning goal.
- **Transport.** HTTP is simple and debuggable; gRPC is realistic.
  Starting with HTTP.
- **Raft log storage.** Reuse `wal/`, write a new one, or use
  `go.etcd.io/etcd/server/v3/storage/wal`? Likely roll our own first
  for the lesson.
- **Snapshot format.** Just JSON-dump the map for now; revisit when it
  matters.
