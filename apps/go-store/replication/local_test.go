package replication

import (
	"context"
	"errors"
	"testing"
)

// fakeApplier records the last Command and returns canned results so
// we can verify LocalReplicator faithfully forwards without doing any
// transformation of its own.
type fakeApplier struct {
	last   Command
	result any
	err    error
	calls  int
}

func (f *fakeApplier) Apply(cmd Command) (any, error) {
	f.calls++
	f.last = cmd
	return f.result, f.err
}

func TestLocalReplicatorForwards(t *testing.T) {
	f := &fakeApplier{result: "ok"}
	r := NewLocalReplicator(f)

	cmd := Command{Kind: CmdSet, Key: "k", Value: "v"}
	got, err := r.Propose(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got != "ok" {
		t.Fatalf("result=%v, want \"ok\"", got)
	}
	if f.calls != 1 || f.last != cmd {
		t.Fatalf("Apply called=%d last=%+v, want calls=1 last=%+v", f.calls, f.last, cmd)
	}
}

func TestLocalReplicatorPropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	f := &fakeApplier{err: sentinel}
	r := NewLocalReplicator(f)

	_, err := r.Propose(context.Background(), Command{Kind: CmdSet, Key: "k", Value: "v"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v, want %v", err, sentinel)
	}
}

func TestLocalReplicatorRespectsCanceledContext(t *testing.T) {
	f := &fakeApplier{}
	r := NewLocalReplicator(f)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.Propose(ctx, Command{Kind: CmdSet, Key: "k", Value: "v"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if f.calls != 0 {
		t.Fatalf("Apply should not have been called on canceled ctx, got calls=%d", f.calls)
	}
}
