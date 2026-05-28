package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recordingApplier captures every Command it sees, in order, so a
// test can verify the FSM-side of the raft pipeline.
type recordingApplier struct {
	mu   sync.Mutex
	cmds []Command
}

func (r *recordingApplier) Apply(c Command) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, c)
	// Set returns nil; Delete returns whether the key "existed".
	// For a recording-only applier, return true for Delete so tests
	// can distinguish nil from a bool result.
	if c.Kind == CmdDelete {
		return true, nil
	}
	return nil, nil
}

func (r *recordingApplier) snapshot() []Command {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Command, len(r.cmds))
	copy(out, r.cmds)
	return out
}

func newSingleNodeReplicator(t *testing.T, a Applier) *RaftReplicator {
	t.Helper()
	// Short tick so a single-node cluster elects itself essentially
	// instantly; otherwise tests would wait the full election timeout.
	r, err := NewRaftReplicator(RaftConfig{
		NodeID:        1,
		Applier:       a,
		TickInterval:  10 * time.Millisecond,
		HeartbeatTick: 1,
		ElectionTick:  3,
	})
	if err != nil {
		t.Fatalf("NewRaftReplicator: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// proposeWithRetry retries Propose until the node has become leader
// (single-node leadership is usually instant but the very first
// Propose can race the initial empty entry). Bounded by ctx.
func proposeWithRetry(t *testing.T, r *RaftReplicator, cmd Command) (any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		res, err := r.Propose(ctx, cmd)
		if err == nil {
			return res, nil
		}
		// raft.ErrProposalDropped happens when the node hasn't elected
		// a leader yet. Retry briefly.
		if ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRaftReplicatorAppliesProposedCommands(t *testing.T) {
	app := &recordingApplier{}
	r := newSingleNodeReplicator(t, app)

	want := []Command{
		{Kind: CmdSet, Key: "a", Value: "1"},
		{Kind: CmdSet, Key: "b", Value: "2"},
		{Kind: CmdDelete, Key: "a"},
	}

	for _, c := range want {
		res, err := proposeWithRetry(t, r, c)
		if err != nil {
			t.Fatalf("Propose %+v: %v", c, err)
		}
		// CmdDelete in our recording applier returns true.
		if c.Kind == CmdDelete && res != true {
			t.Fatalf("Delete result=%v, want true", res)
		}
	}

	got := app.snapshot()
	if len(got) != len(want) {
		t.Fatalf("applied %d commands, want %d: %+v", len(got), len(want), got)
	}
	for i, c := range want {
		if got[i] != c {
			t.Fatalf("apply order mismatch at %d: got=%+v want=%+v", i, got[i], c)
		}
	}
}

func TestRaftReplicatorRejectsBadCommand(t *testing.T) {
	app := &recordingApplier{}
	r := newSingleNodeReplicator(t, app)

	// Wait for election to settle before testing the marshal-side error,
	// so we don't conflate "no leader yet" with "bad command".
	if _, err := proposeWithRetry(t, r, Command{Kind: CmdSet, Key: "warmup", Value: ""}); err != nil {
		t.Fatalf("warmup propose: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := r.Propose(ctx, Command{Kind: CmdInvalid, Key: "k"})
	if err == nil {
		t.Fatal("expected marshal error for invalid kind")
	}
}

func TestRaftReplicatorContextCancel(t *testing.T) {
	app := &recordingApplier{}
	r := newSingleNodeReplicator(t, app)

	// Drain any startup before we measure cancellation behavior.
	if _, err := proposeWithRetry(t, r, Command{Kind: CmdSet, Key: "warmup", Value: ""}); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.Propose(ctx, Command{Kind: CmdSet, Key: "k", Value: "v"})
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestRaftReplicatorCloseUnblocksProposer(t *testing.T) {
	app := &recordingApplier{}
	r := newSingleNodeReplicator(t, app)

	if _, err := proposeWithRetry(t, r, Command{Kind: CmdSet, Key: "warmup", Value: ""}); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// Close in the background; concurrent Propose should not deadlock.
	done := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = r.Close()
		close(done)
	}()

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = r.Propose(ctx, Command{Kind: CmdSet, Key: fmt.Sprintf("k%d", i), Value: "v"})
		cancel()
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not complete; proposer likely deadlocked")
	}
}
