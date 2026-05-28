package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// inMemoryTransport is an in-process Transport used by multi-node
// tests. It maps peer IDs to *RaftReplicator and delivers each message
// by calling Step directly on the destination, on its own goroutine
// (mirroring HTTPTransport's non-blocking semantics).
//
// Peers are registered after the replicators exist via Register, so
// the construction order matches the production wiring:
//
//	transport -> replicator -> transport.Register(peerID, peerReplicator)
type inMemoryTransport struct {
	self uint64

	mu    sync.RWMutex
	peers map[uint64]MessageReceiver
}

func newInMemoryTransport(self uint64) *inMemoryTransport {
	return &inMemoryTransport{self: self, peers: make(map[uint64]MessageReceiver)}
}

func (t *inMemoryTransport) Register(id uint64, r MessageReceiver) {
	t.mu.Lock()
	t.peers[id] = r
	t.mu.Unlock()
}

func (t *inMemoryTransport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 || m.To == t.self {
			continue
		}
		t.mu.RLock()
		dst, ok := t.peers[m.To]
		t.mu.RUnlock()
		if !ok {
			continue
		}
		m := m // capture
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = dst.Step(ctx, m)
		}()
	}
}

// buildCluster constructs an N-node raft cluster wired with
// in-memory transports. Returns the replicators (1-indexed) and
// their per-node appliers (same indexing).
func buildCluster(t *testing.T, n int) ([]*RaftReplicator, []*recordingApplier) {
	t.Helper()

	peerIDs := make([]uint64, n)
	for i := 0; i < n; i++ {
		peerIDs[i] = uint64(i + 1)
	}

	transports := make([]*inMemoryTransport, n)
	for i := 0; i < n; i++ {
		transports[i] = newInMemoryTransport(uint64(i + 1))
	}

	apps := make([]*recordingApplier, n)
	reps := make([]*RaftReplicator, n)
	for i := 0; i < n; i++ {
		apps[i] = &recordingApplier{}
		r, err := NewRaftReplicator(RaftConfig{
			NodeID:        uint64(i + 1),
			Peers:         peerIDs,
			Applier:       apps[i],
			Transport:     transports[i],
			TickInterval:  10 * time.Millisecond,
			HeartbeatTick: 1,
			ElectionTick:  5,
		})
		if err != nil {
			t.Fatalf("NewRaftReplicator(%d): %v", i+1, err)
		}
		reps[i] = r
	}

	// Now that all replicators exist, wire each transport's peer table.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			transports[i].Register(uint64(j+1), reps[j])
		}
	}

	t.Cleanup(func() {
		for _, r := range reps {
			_ = r.Close()
		}
	})
	return reps, apps
}

// waitForLeader polls until any replica reports a non-zero LeaderID
// and returns the leader's RaftReplicator. Fails the test if no
// leader emerges within timeout.
func waitForLeader(t *testing.T, reps []*RaftReplicator, timeout time.Duration) *RaftReplicator {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range reps {
			if id := r.LeaderID(); id != 0 {
				for _, x := range reps {
					if x.id == id {
						return x
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestRaftClusterElectsLeaderAndReplicates(t *testing.T) {
	reps, apps := buildCluster(t, 3)

	leader := waitForLeader(t, reps, 5*time.Second)

	want := []Command{
		{Kind: CmdSet, Key: "a", Value: "1"},
		{Kind: CmdSet, Key: "b", Value: "2"},
		{Kind: CmdDelete, Key: "a"},
	}
	for _, c := range want {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := leader.Propose(ctx, c); err != nil {
			cancel()
			t.Fatalf("leader Propose %+v: %v", c, err)
		}
		cancel()
	}

	// Every follower should converge to the same applied sequence.
	deadline := time.Now().Add(3 * time.Second)
	for i, app := range apps {
		for {
			got := app.snapshot()
			if len(got) == len(want) {
				for k, c := range want {
					if got[k] != c {
						t.Fatalf("node %d apply mismatch at %d: got=%+v want=%+v", i+1, k, got[k], c)
					}
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d only applied %d/%d commands: %+v", i+1, len(got), len(want), got)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestRaftClusterFollowerProposeForwards(t *testing.T) {
	// raft.Node.Propose on a follower transparently forwards to the
	// current leader via MsgProp. Verifies the transport carries
	// that message both ways.
	reps, apps := buildCluster(t, 3)
	leader := waitForLeader(t, reps, 5*time.Second)

	// Pick any non-leader.
	var follower *RaftReplicator
	for _, r := range reps {
		if r != leader {
			follower = r
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := follower.Propose(ctx, Command{Kind: CmdSet, Key: "from-follower", Value: "ok"}); err != nil {
		t.Fatalf("follower Propose: %v", err)
	}

	// All three nodes must apply it.
	deadline := time.Now().Add(3 * time.Second)
	for i, app := range apps {
		for {
			got := app.snapshot()
			if len(got) >= 1 && got[len(got)-1].Key == "from-follower" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never applied follower-proposed command: %+v", i+1, got)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
