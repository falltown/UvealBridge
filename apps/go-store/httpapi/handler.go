// Package httpapi exposes the store.Store over a small REST API.
//
//	GET    /kv          -> 200, JSON {"keys":[...], "count":N}
//	GET    /kv/{key}    -> 200 text body | 404
//	PUT    /kv/{key}    -> 204 (body becomes the value)
//	DELETE /kv/{key}    -> 204 | 404
//	GET    /healthz     -> 200 "ok"
//
// Reads (GET) talk to the store directly. Writes (PUT, DELETE) go
// through a replication.Replicator so the same handler works whether
// the binary is running standalone (LocalReplicator) or as a Raft peer
// (RaftReplicator). The handler stays oblivious to which.
//
// Every response also carries an X-Pod header identifying which replica
// served the request (sourced from POD_NAME or os.Hostname()).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/falltown/UvealBridge/apps/go-store/replication"
	"github.com/falltown/UvealBridge/apps/go-store/store"
)

// NewHandler returns an http.Handler that serves the KV API. Reads
// hit s; writes are proposed through r.
func NewHandler(s store.Store, r replication.Replicator) http.Handler {
	mux := http.NewServeMux()
	pod := podName()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})

	mux.HandleFunc("GET /kv", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys":  s.Keys(),
			"count": s.Len(),
		})
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, req *http.Request) {
		key := req.PathValue("key")
		v, ok := s.Get(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, v)
	})

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, req *http.Request) {
		key := req.PathValue("key")
		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20)) // 1 MiB cap
		if err != nil {
			http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := r.Propose(req.Context(), replication.Command{
			Kind:  replication.CmdSet,
			Key:   key,
			Value: string(body),
		}); err != nil {
			writeProposeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, req *http.Request) {
		key := req.PathValue("key")
		res, err := r.Propose(req.Context(), replication.Command{
			Kind: replication.CmdDelete,
			Key:  key,
		})
		if err != nil {
			writeProposeError(w, err)
			return
		}
		// Applier returns bool: true if the key existed and was deleted.
		// Treat a missing-or-wrong-type result as "didn't exist" so we
		// stay safe if a future Applier returns nil for some reason.
		existed, _ := res.(bool)
		if !existed {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return withPodHeader(pod, mux)
}

// writeProposeError maps Replicator.Propose errors to HTTP status
// codes. Kept centralized so PUT and DELETE stay in sync.
func writeProposeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Client gave up or our deadline fired; either way the write
		// might still commit. 503 tells the client to retry.
		http.Error(w, "request canceled: "+err.Error(), http.StatusServiceUnavailable)
	default:
		// Includes Raft "not leader", transport errors, etc. once
		// RaftReplicator lands. For now LocalReplicator only ever
		// errors on bad commands, which would be a server-side bug.
		http.Error(w, "propose failed: "+err.Error(), http.StatusServiceUnavailable)
	}
}
