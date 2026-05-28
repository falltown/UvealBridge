package replication

import "testing"

func TestParsePodOrdinal(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"go-store-0", 0, false},
		{"go-store-1", 1, false},
		{"go-store-12", 12, false},
		{"weird-name-with-dashes-3", 3, false},
		{"no-suffix-", 0, true},
		{"nodashes", 0, true},
		{"go-store-abc", 0, true},
	}
	for _, tc := range cases {
		got, err := ParsePodOrdinal(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParsePodOrdinal(%q) want error, got %d", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePodOrdinal(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParsePodOrdinal(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestBuildStatefulSetPeers(t *testing.T) {
	self, peers, err := BuildStatefulSetPeers("go-store-1", "go-store", "go-store-headless", 8080, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if self != 2 {
		t.Fatalf("selfID = %d, want 2", self)
	}
	want := map[uint64]string{
		1: "http://go-store-0.go-store-headless:8080",
		2: "http://go-store-1.go-store-headless:8080",
		3: "http://go-store-2.go-store-headless:8080",
	}
	if len(peers) != len(want) {
		t.Fatalf("peers len = %d, want %d", len(peers), len(want))
	}
	for id, url := range want {
		if peers[id] != url {
			t.Errorf("peers[%d] = %q, want %q", id, peers[id], url)
		}
	}
}

func TestBuildStatefulSetPeersRejectsBadInput(t *testing.T) {
	cases := []struct {
		name                string
		pod, base, headless string
		port, size          int
	}{
		{"ordinal-out-of-range", "go-store-5", "go-store", "go-store-headless", 8080, 3},
		{"zero-size", "go-store-0", "go-store", "go-store-headless", 8080, 0},
		{"empty-base", "go-store-0", "", "go-store-headless", 8080, 3},
		{"empty-headless", "go-store-0", "go-store", "", 8080, 3},
		{"zero-port", "go-store-0", "go-store", "go-store-headless", 0, 3},
		{"bad-name", "weird", "go-store", "go-store-headless", 8080, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := BuildStatefulSetPeers(tc.pod, tc.base, tc.headless, tc.port, tc.size); err == nil {
				t.Errorf("expected error for %+v", tc)
			}
		})
	}
}
