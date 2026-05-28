package replication

import (
	"fmt"
	"strconv"
	"strings"
)

// ParsePodOrdinal extracts the trailing integer from a StatefulSet pod
// hostname. StatefulSet guarantees pods are named "<base>-<ordinal>",
// starting at 0, e.g. "go-store-0", "go-store-1", "go-store-2".
//
// Returns an error if the name has no trailing "-N" suffix or N is
// negative — both indicate the binary is running outside a
// StatefulSet, which is a configuration bug we want loud.
func ParsePodOrdinal(podName string) (int, error) {
	i := strings.LastIndex(podName, "-")
	if i < 0 || i == len(podName)-1 {
		return 0, fmt.Errorf("pod name %q has no ordinal suffix", podName)
	}
	n, err := strconv.Atoi(podName[i+1:])
	if err != nil {
		return 0, fmt.Errorf("pod name %q: parse ordinal: %w", podName, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("pod name %q: negative ordinal %d", podName, n)
	}
	return n, nil
}

// BuildStatefulSetPeers turns a (podName, base, headless, port, size)
// tuple into:
//
//   - selfID:  this node's raft ID (= ordinal + 1, because raft
//     rejects 0 as a node ID).
//   - peers:   nodeID -> base URL for every member of the cluster,
//     including self. RaftReplicator's transport ignores
//     messages addressed to self, but having it in the map
//     keeps callers from special-casing.
//
// Example: BuildStatefulSetPeers("go-store-1", "go-store",
// "go-store-headless", 8080, 3) returns selfID=2 and
//
//	{1: "http://go-store-0.go-store-headless:8080",
//	 2: "http://go-store-1.go-store-headless:8080",
//	 3: "http://go-store-2.go-store-headless:8080"}.
//
// The peer URLs deliberately use bare DNS without a namespace
// suffix: inside the cluster, Kubernetes resolves
// "go-store-0.go-store-headless" within the pod's own namespace.
// Cross-namespace clusters would need to pass a fully-qualified
// headless name; we don't support that yet.
func BuildStatefulSetPeers(podName, base, headless string, port, size int) (selfID uint64, peers map[uint64]string, err error) {
	if size < 1 {
		return 0, nil, fmt.Errorf("cluster size must be >= 1, got %d", size)
	}
	if base == "" {
		return 0, nil, fmt.Errorf("base name required")
	}
	if headless == "" {
		return 0, nil, fmt.Errorf("headless service name required")
	}
	if port <= 0 {
		return 0, nil, fmt.Errorf("port must be > 0, got %d", port)
	}
	ord, err := ParsePodOrdinal(podName)
	if err != nil {
		return 0, nil, err
	}
	if ord >= size {
		return 0, nil, fmt.Errorf("pod ordinal %d outside cluster of size %d", ord, size)
	}

	peers = make(map[uint64]string, size)
	for i := 0; i < size; i++ {
		peers[uint64(i+1)] = fmt.Sprintf("http://%s-%d.%s:%d", base, i, headless, port)
	}
	return uint64(ord + 1), peers, nil
}
