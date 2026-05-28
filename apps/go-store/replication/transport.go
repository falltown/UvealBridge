package replication

// Transport is the boundary between the raft Ready loop ("here are
// messages I want delivered to peers") and the network ("get the
// bytes there"). Implementations are responsible for resolving peer
// IDs to addresses, marshaling, and best-effort delivery. raft itself
// tolerates dropped, reordered, and duplicated messages — it will
// retry — so a Transport is allowed to fail silently.
//
// Two implementations live in this package:
//
//   - HTTPTransport: real wire protocol used between pods.
//   - inMemoryTransport (test-only, transport_test.go): in-process
//     fanout used by multi-node unit tests.
//
// Send is called from the Ready goroutine and MUST NOT block on a
// slow peer. Use background goroutines / per-peer queues internally.
//
// Receiving is the inverse: the transport accepts inbound bytes
// (HTTP body, in-process call), unmarshals them into a raftpb.Message,
// and hands the message to the bound MessageReceiver (the local
// RaftReplicator). The transport is constructed first; the receiver
// is wired in via Bind after the RaftReplicator exists. That order
// avoids a chicken-and-egg between the two.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// Transport is what RaftReplicator uses to ship outbound messages.
// Implementations must not block the caller — fan out asynchronously.
type Transport interface {
	Send(msgs []raftpb.Message)
}

// MessageReceiver is what a Transport calls when an inbound raft
// message has been decoded off the wire. RaftReplicator implements
// this via Step.
type MessageReceiver interface {
	Step(ctx context.Context, m raftpb.Message) error
}

// noopTransport is the default when a RaftReplicator is constructed
// without one (e.g. single-node tests). It discards outbound traffic
// and never delivers anything inbound. Lets the Ready loop stay
// branchless.
type noopTransport struct{}

func (noopTransport) Send([]raftpb.Message) {}

// RaftMessagePath is the HTTP endpoint HTTPTransport listens on for
// inbound raft RPCs. Exported so callers can mount Handler() at the
// matching path (or read it for logs).
const RaftMessagePath = "/raft/message"

// HTTPTransport ships raft messages between pods over plain HTTP.
// Each outbound message becomes one POST to the destination peer's
// RaftMessagePath; the body is the raftpb.Message marshaled with its
// own proto encoder. Inbound traffic arrives on Handler() and is
// stepped into the bound receiver.
//
// Properties:
//   - Send returns immediately. Each message is dispatched on its own
//     goroutine with a per-request timeout so one slow peer can't
//     stall the Ready loop or pile up forever.
//   - Failures are logged and dropped. raft will retry.
//   - The receiver is bound after construction via Bind, which lets
//     RaftReplicator hold the Transport from the start without a
//     circular dependency.
type HTTPTransport struct {
	self    uint64
	peers   map[uint64]string // nodeID -> base URL, e.g. "http://go-store-1.go-store-headless:8080"
	client  *http.Client
	logger  *log.Logger
	timeout time.Duration

	mu       sync.RWMutex
	receiver MessageReceiver
}

// NewHTTPTransport builds a transport for node `self` that knows how
// to reach every other peer via the given map (nodeID -> base URL).
// The self entry is allowed and ignored — messages addressed to self
// are dropped (raft never sends those, but we're defensive).
//
// `peers` is copied; later mutations by the caller don't affect us.
// A nil logger uses a discarding logger.
func NewHTTPTransport(self uint64, peers map[uint64]string, logger *log.Logger) *HTTPTransport {
	if logger == nil {
		logger = log.New(discardWriter{}, "", 0)
	}
	cp := make(map[uint64]string, len(peers))
	for k, v := range peers {
		cp[k] = v
	}
	return &HTTPTransport{
		self:    self,
		peers:   cp,
		client:  &http.Client{Timeout: 2 * time.Second},
		logger:  logger,
		timeout: 2 * time.Second,
	}
}

// Bind attaches the receiver inbound messages are delivered to. Must
// be called before Handler starts serving traffic; otherwise inbound
// messages are dropped with a log line.
func (t *HTTPTransport) Bind(r MessageReceiver) {
	t.mu.Lock()
	t.receiver = r
	t.mu.Unlock()
}

// Send dispatches each message on its own goroutine. Best-effort.
func (t *HTTPTransport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 || m.To == t.self {
			continue
		}
		url, ok := t.peers[m.To]
		if !ok {
			t.logger.Printf("raft transport: no address for peer %d, dropping %s", m.To, m.Type)
			continue
		}
		go t.sendOne(url, m)
	}
}

func (t *HTTPTransport) sendOne(baseURL string, m raftpb.Message) {
	data, err := m.Marshal()
	if err != nil {
		t.logger.Printf("raft transport: marshal %s to %d: %v", m.Type, m.To, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+RaftMessagePath, bytes.NewReader(data))
	if err != nil {
		t.logger.Printf("raft transport: new request to %d: %v", m.To, err)
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := t.client.Do(req)
	if err != nil {
		// Routine in a cluster where a peer is restarting; debug-only.
		t.logger.Printf("raft transport: send %s to %d: %v", m.Type, m.To, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.logger.Printf("raft transport: peer %d returned %s", m.To, resp.Status)
	}
}

// Handler returns an http.Handler that accepts inbound raft messages.
// Mount it at RaftMessagePath on whatever mux the pod is using:
//
//	mux.Handle(replication.RaftMessagePath, transport.Handler())
func (t *HTTPTransport) Handler() http.Handler {
	return http.HandlerFunc(t.serve)
}

func (t *HTTPTransport) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Cap the body. Raft messages are bounded by MaxSizePerMsg (4 MiB
	// in our config); allow a little headroom for envelope overhead.
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var m raftpb.Message
	if err := m.Unmarshal(body); err != nil {
		http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}

	t.mu.RLock()
	rcv := t.receiver
	t.mu.RUnlock()
	if rcv == nil {
		t.logger.Printf("raft transport: receiver not bound, dropping %s from %d", m.Type, m.From)
		http.Error(w, "receiver not ready", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), t.timeout)
	defer cancel()
	if err := rcv.Step(ctx, m); err != nil {
		// raft.ErrStopped is normal during shutdown — caller will retry.
		if !errors.Is(err, errReplicatorStopped) {
			t.logger.Printf("raft transport: step %s from %d: %v", m.Type, m.From, err)
		}
		http.Error(w, fmt.Sprintf("step: %v", err), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// errReplicatorStopped is returned by RaftReplicator.Step after Close.
// Defined here so transport.go can compare against it without import
// cycles inside the package.
var errReplicatorStopped = errors.New("raft: replicator stopped")

// Compile-time checks.
var (
	_ Transport = (*HTTPTransport)(nil)
	_ Transport = noopTransport{}
)
