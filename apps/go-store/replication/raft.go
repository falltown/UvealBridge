package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// proposalIDSize is the prefix every Propose adds to a marshaled
// Command before handing it to raft. It's the local correlation token
// that lets the Ready loop wake up the right caller after an entry
// commits. It is meaningless to other peers; they just discard the
// prefix and apply the trailing Command bytes.
const proposalIDSize = 8

// applyResult is what the Ready loop sends back to a waiting Propose
// caller once the entry it proposed has been applied locally.
type applyResult struct {
	result any
	err    error
}

// RaftReplicator drives a single raft.Node, persists its log to the
// provided storage, applies committed entries to the local Applier,
// and lets HTTP handlers Propose new commands. Single-node only at
// this step — no transport, no peers — but the API shape is exactly
// what we'll need when peers come in Step 3.
//
// Lifecycle:
//
//	r := NewRaftReplicator(...)
//	defer r.Close()
//	r.Propose(ctx, cmd)   // blocks until committed+applied
type RaftReplicator struct {
	id        uint64
	node      raft.Node
	storage   *raft.MemoryStorage
	applier   Applier
	transport Transport
	ticker    *time.Ticker
	stopCh    chan struct{}
	doneCh    chan struct{}
	logger    *log.Logger

	// pending maps an envelope reqID to the channel the proposing
	// goroutine is waiting on. Guarded by mu.
	mu      sync.Mutex
	pending map[uint64]chan applyResult
	nextID  atomic.Uint64
}

// RaftConfig collects the tunables for NewRaftReplicator. Defaults
// (zero values) are reasonable for tests and single-node.
type RaftConfig struct {
	// NodeID is this node's raft identity. Must be non-zero and
	// stable across restarts. In Kubernetes, derive it from the
	// pod ordinal (go-store-0 -> 1, etc.).
	NodeID uint64

	// Peers is the initial peer set (NodeIDs) used to bootstrap a
	// fresh cluster. For single-node testing pass nil or []uint64{NodeID}.
	Peers []uint64

	// Applier is the FSM committed entries are applied to.
	Applier Applier

	// TickInterval drives raft's logical clock. HeartbeatTick and
	// ElectionTick are measured in these. Default 100ms.
	TickInterval time.Duration

	// HeartbeatTick is how many TickIntervals between leader
	// heartbeats. Default 1.
	HeartbeatTick int

	// ElectionTick is how many TickIntervals a follower waits
	// without a heartbeat before starting an election. Must be
	// > HeartbeatTick. Default 10.
	ElectionTick int

	// Logger is used for raft-level logs. Defaults to a discard
	// logger so tests stay quiet.
	Logger *log.Logger

	// Transport ships outbound raft messages to peers. If nil, a
	// no-op transport is used (correct for single-node clusters and
	// most tests). For real multi-node operation, supply an
	// HTTPTransport and call Transport.Bind(r) after construction
	// so inbound traffic can be Stepped into this node.
	Transport Transport
}

// NewRaftReplicator starts a raft.Node and the Ready loop. Call Close
// to shut down cleanly.
func NewRaftReplicator(cfg RaftConfig) (*RaftReplicator, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("raft: NodeID must be non-zero")
	}
	if cfg.Applier == nil {
		return nil, errors.New("raft: Applier required")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.ElectionTick <= cfg.HeartbeatTick {
		return nil, fmt.Errorf("raft: ElectionTick (%d) must be > HeartbeatTick (%d)",
			cfg.ElectionTick, cfg.HeartbeatTick)
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(discardWriter{}, "", 0)
	}
	if cfg.Transport == nil {
		cfg.Transport = noopTransport{}
	}
	if len(cfg.Peers) == 0 {
		cfg.Peers = []uint64{cfg.NodeID}
	}

	storage := raft.NewMemoryStorage()
	rcfg := &raft.Config{
		ID:              cfg.NodeID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   4 << 20, // 4 MiB
		MaxInflightMsgs: 256,
	}

	peers := make([]raft.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, raft.Peer{ID: p})
	}

	r := &RaftReplicator{
		id:        cfg.NodeID,
		node:      raft.StartNode(rcfg, peers),
		storage:   storage,
		applier:   cfg.Applier,
		transport: cfg.Transport,
		ticker:    time.NewTicker(cfg.TickInterval),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		logger:    cfg.Logger,
		pending:   make(map[uint64]chan applyResult),
	}
	go r.run()
	return r, nil
}

// Propose marshals cmd, prepends a request ID, hands it to raft, and
// blocks until the entry has been committed and applied locally (or
// ctx is canceled). The returned (result, err) come straight from
// Applier.Apply.
func (r *RaftReplicator) Propose(ctx context.Context, cmd Command) (any, error) {
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return nil, fmt.Errorf("raft propose: marshal: %w", err)
	}

	reqID := r.nextID.Add(1)
	envelope := make([]byte, proposalIDSize+len(cmdBytes))
	binary.BigEndian.PutUint64(envelope[:proposalIDSize], reqID)
	copy(envelope[proposalIDSize:], cmdBytes)

	// Register the wait channel BEFORE calling node.Propose so we
	// can't miss a fast apply that races us.
	ch := make(chan applyResult, 1)
	r.mu.Lock()
	r.pending[reqID] = ch
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.pending, reqID)
		r.mu.Unlock()
	}()

	if err := r.node.Propose(ctx, envelope); err != nil {
		return nil, fmt.Errorf("raft propose: %w", err)
	}

	select {
	case res := <-ch:
		return res.result, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, errors.New("raft: replicator stopped")
	}
}

// Close stops the Ready loop and the raft node. Safe to call once;
// further Propose calls return an error.
func (r *RaftReplicator) Close() error {
	select {
	case <-r.stopCh:
		return nil // already closed
	default:
	}
	close(r.stopCh)
	<-r.doneCh
	r.ticker.Stop()
	r.node.Stop()
	return nil
}

// run is the Ready loop: the single goroutine that owns interactions
// with raft.Node. Everything raft-related happens here.
func (r *RaftReplicator) run() {
	defer close(r.doneCh)
	for {
		select {
		case <-r.stopCh:
			return

		case <-r.ticker.C:
			r.node.Tick()

		case rd := <-r.node.Ready():
			// 1. Persist log entries and hard state. With MemoryStorage
			//    this is in-RAM; with a disk-backed storage we'd fsync
			//    here before doing anything else.
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := r.storage.SetHardState(rd.HardState); err != nil {
					r.logger.Printf("raft: SetHardState: %v", err)
				}
			}
			if len(rd.Entries) > 0 {
				if err := r.storage.Append(rd.Entries); err != nil {
					r.logger.Printf("raft: Append: %v", err)
				}
			}

			// 2. Send messages to peers. The transport is responsible
			//    for non-blocking, best-effort delivery; raft retries on
			//    loss so we never need to wait here.
			if len(rd.Messages) > 0 {
				r.transport.Send(rd.Messages)
			}

			// 3. Apply committed entries to the FSM and wake any
			//    proposing goroutine waiting on the result.
			for _, ent := range rd.CommittedEntries {
				r.applyEntry(ent)
			}

			// 4. Apply snapshot if present (Step 8). Skip for now.

			// 5. Tell raft we're done with this Ready batch.
			r.node.Advance()
		}
	}
}

// applyEntry decodes one committed raft entry, applies it to the FSM
// (if it's a normal entry with data), and signals the waiter if any.
func (r *RaftReplicator) applyEntry(ent raftpb.Entry) {
	switch ent.Type {
	case raftpb.EntryNormal:
		// Leader emits an empty EntryNormal after winning an election;
		// it carries no command, just commits the term.
		if len(ent.Data) == 0 {
			return
		}
		if len(ent.Data) < proposalIDSize {
			r.logger.Printf("raft: dropping malformed entry (len=%d) at index %d", len(ent.Data), ent.Index)
			return
		}
		reqID := binary.BigEndian.Uint64(ent.Data[:proposalIDSize])
		cmd, err := UnmarshalCommand(ent.Data[proposalIDSize:])
		if err != nil {
			r.logger.Printf("raft: decode command at index %d: %v", ent.Index, err)
			r.deliver(reqID, applyResult{err: err})
			return
		}
		res, err := r.applier.Apply(cmd)
		r.deliver(reqID, applyResult{result: res, err: err})

	case raftpb.EntryConfChange:
		// Membership changes land here. Step 10. For now, acknowledge
		// the change with raft so it doesn't get stuck, but otherwise
		// ignore.
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err == nil {
			r.node.ApplyConfChange(cc)
		}
	}
}

// deliver hands result to the waiter for reqID, if one is registered.
// Non-blocking: the channel is buffered with capacity 1 and the slot
// is unique per reqID, so the send always succeeds.
func (r *RaftReplicator) deliver(reqID uint64, res applyResult) {
	r.mu.Lock()
	ch, ok := r.pending[reqID]
	r.mu.Unlock()
	if !ok {
		return // no waiter (followers won't have one; nor will a
		//        proposer whose ctx already expired)
	}
	ch <- res
}

// discardWriter is io.Writer that throws everything away. Saves us
// pulling in io/ioutil's Discard just for the default logger.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// Step feeds an inbound raft message into the local raft.Node. It's
// the inbound counterpart to Transport.Send: an HTTPTransport calls
// this from its HTTP handler after decoding a message. Returns an
// error if the replicator has been closed or the node refuses the
// message (raft is shutting down).
func (r *RaftReplicator) Step(ctx context.Context, m raftpb.Message) error {
	select {
	case <-r.stopCh:
		return errReplicatorStopped
	default:
	}
	return r.node.Step(ctx, m)
}

// LeaderID returns the raft node ID that this replica currently
// believes to be leader, or 0 if no leader is known (e.g. mid-election).
// Cheap; safe to call from any goroutine.
func (r *RaftReplicator) LeaderID() uint64 {
	return r.node.Status().Lead
}

// IsLeader reports whether this node currently believes itself to be
// leader. Note: this is a hint, not a guarantee — a stale leader can
// still answer true briefly. Use only for routing decisions where
// correctness comes from raft below.
func (r *RaftReplicator) IsLeader() bool {
	return r.node.Status().Lead == r.id
}

// Compile-time check.
var _ Replicator = (*RaftReplicator)(nil)
var _ MessageReceiver = (*RaftReplicator)(nil)
