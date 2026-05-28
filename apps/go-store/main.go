package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/falltown/UvealBridge/apps/go-store/httpapi"
	"github.com/falltown/UvealBridge/apps/go-store/replication"
	"github.com/falltown/UvealBridge/apps/go-store/store"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// State store: always the in-memory SafeStore. With raft enabled,
	// the cluster's raft log (Step 7) becomes the source of truth on
	// disk; a per-pod WAL would be a divergent second log that re-
	// applies on restart and drifts away from the cluster. DATA_DIR
	// will be reused for raft's own persistent log/snapshot storage
	// once we implement Step 7.
	log.Printf("using in-memory SafeStore")
	safe := store.NewSafe()
	var s store.Store = safe
	var applier replication.Applier = safe

	// Build the Replicator and (optionally) a raft transport HTTP handler.
	// In single-node / non-cluster mode this returns a LocalReplicator and
	// a nil raftHandler. In cluster mode it returns a RaftReplicator
	// driving an HTTPTransport, and an http.Handler we must mount at
	// replication.RaftMessagePath so peers can reach us.
	r, raftHandler, closeReplicator, err := buildReplicator(applier)
	if err != nil {
		log.Fatalf("build replicator: %v", err)
	}
	defer closeReplicator()

	// Public mux. httpapi handles "/kv*", "/healthz", etc.; mount it
	// under "/" as the catch-all. The raft endpoint (if enabled) gets a
	// more-specific pattern and wins via Go 1.22 ServeMux precedence.
	mux := http.NewServeMux()
	if raftHandler != nil {
		mux.Handle(replication.RaftMessagePath, raftHandler)
		log.Printf("raft transport listening at %s", replication.RaftMessagePath)
	}
	mux.Handle("/", httpapi.NewHandler(s, r))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run server in a goroutine so we can handle signals for graceful shutdown.
	errCh := make(chan error, 1)
	go func() {
		log.Printf("go-store listening on http://%s", addr)
		errCh <- srv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	case sig := <-stop:
		log.Printf("received %s, shutting down...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("graceful shutdown failed: %v", err)
		}
	}
}

// buildReplicator decides between LocalReplicator (single-node) and
// RaftReplicator (cluster mode) based on env vars.
//
// Cluster mode requires:
//
//	POD_NAME       - StatefulSet pod hostname (e.g. "go-store-1")
//	CLUSTER_SIZE   - integer >= 1, total raft members in the cluster
//
// Optional, with sensible defaults:
//
//	RAFT_BASE_NAME    - default "go-store"; StatefulSet name
//	RAFT_HEADLESS_SVC - default "go-store-headless"; service giving each
//	                    pod a stable DNS name
//	RAFT_PEER_PORT    - default 8080; port peers listen on
//
// Returns the Replicator, an inbound http.Handler that must be mounted
// at RaftMessagePath (nil in single-node mode), and a Close func the
// caller must defer.
func buildReplicator(applier replication.Applier) (replication.Replicator, http.Handler, func(), error) {
	sizeStr := os.Getenv("CLUSTER_SIZE")
	if sizeStr == "" {
		log.Printf("using LocalReplicator (CLUSTER_SIZE not set)")
		return replication.NewLocalReplicator(applier), nil, func() {}, nil
	}

	size, err := strconv.Atoi(sizeStr)
	if err != nil || size < 1 {
		return nil, nil, nil, fmt.Errorf("CLUSTER_SIZE=%q must be a positive integer", sizeStr)
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		if n, hostErr := os.Hostname(); hostErr == nil {
			podName = n
		}
	}
	if podName == "" {
		return nil, nil, nil, errors.New("POD_NAME not set and os.Hostname() empty")
	}

	base := envOr("RAFT_BASE_NAME", "go-store")
	headless := envOr("RAFT_HEADLESS_SVC", "go-store-headless")
	port, err := strconv.Atoi(envOr("RAFT_PEER_PORT", "8080"))
	if err != nil || port <= 0 {
		return nil, nil, nil, fmt.Errorf("RAFT_PEER_PORT invalid: %v", err)
	}

	selfID, peers, err := replication.BuildStatefulSetPeers(podName, base, headless, port, size)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("derive peers: %w", err)
	}

	peerIDs := make([]uint64, 0, len(peers))
	for id := range peers {
		peerIDs = append(peerIDs, id)
	}

	raftLog := log.New(os.Stderr, "raft ", log.LstdFlags|log.Lmicroseconds)

	// Election timing. Defaults: 250ms tick, 1 heartbeat tick, 12
	// election ticks => leader sends a heartbeat every 250ms and a
	// follower waits 3s without hearing one before starting an
	// election. This is deliberately slower than the etcd default;
	// in-cluster DNS + kubelet startup races eat the first second or
	// two and we don't want that to translate into a flurry of
	// reelections. Override with RAFT_TICK_MS / RAFT_ELECTION_TICKS
	// if you ever need to tighten it.
	tickMS, err := strconv.Atoi(envOr("RAFT_TICK_MS", "250"))
	if err != nil || tickMS <= 0 {
		return nil, nil, nil, fmt.Errorf("RAFT_TICK_MS invalid: %v", err)
	}
	electionTicks, err := strconv.Atoi(envOr("RAFT_ELECTION_TICKS", "12"))
	if err != nil || electionTicks <= 1 {
		return nil, nil, nil, fmt.Errorf("RAFT_ELECTION_TICKS invalid: %v", err)
	}

	// Construction order: transport -> replicator -> Bind. The
	// replicator captures the transport for outbound Send; Bind wires
	// the inbound HTTP handler back into the replicator's Step.
	transport := replication.NewHTTPTransport(selfID, peers, raftLog)

	r, err := replication.NewRaftReplicator(replication.RaftConfig{
		NodeID:        selfID,
		Peers:         peerIDs,
		Applier:       applier,
		Transport:     transport,
		Logger:        raftLog,
		TickInterval:  time.Duration(tickMS) * time.Millisecond,
		HeartbeatTick: 1,
		ElectionTick:  electionTicks,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("start raft: %w", err)
	}
	transport.Bind(r)

	log.Printf("raft enabled: nodeID=%d size=%d peers=%v", selfID, size, peers)
	return r, transport.Handler(), func() { _ = r.Close() }, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
