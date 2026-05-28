package httpapi

import (
	"net/http"
	"os"
)

// podName identifies the process serving the request. In Kubernetes the
// downward API (or the default HOSTNAME env var, which equals the pod name)
// makes this trivially available. Falls back to "unknown" outside k8s.
func podName() string {
	if n := os.Getenv("POD_NAME"); n != "" {
		return n
	}
	if n, err := os.Hostname(); err == nil && n != "" {
		return n
	}
	return "unknown"
}

// withPodHeader returns an http.Handler that stamps every response with
// X-Pod before delegating to next. Wrapping the mux once (rather than each
// route) keeps handler bodies clean and also covers mux-generated responses
// like 404 / 405.
func withPodHeader(pod string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Pod", pod)
		next.ServeHTTP(w, r)
	})
}
