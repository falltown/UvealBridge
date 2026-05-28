package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/falltown/UvealBridge/apps/go-store/replication"
	"github.com/falltown/UvealBridge/apps/go-store/store"
)

func newTestServer() *httptest.Server {
	s := store.NewSafe()
	r := replication.NewLocalReplicator(s)
	return httptest.NewServer(NewHandler(s, r))
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status=%d, want 204", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "bar" {
		t.Fatalf("GET status=%d body=%q, want 200 \"bar\"", resp.StatusCode, body)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/kv/foo", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status=%d, want 204", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET-after-delete status=%d, want 404", resp.StatusCode)
	}
}

func TestGetMissing(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("got %d %q, want 200 \"ok\"", resp.StatusCode, body)
	}
}
