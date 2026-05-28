package store

import (
	"sync"
	"testing"
)

// runStoreTests exercises the Store interface and is reused across
// implementations so both basic and thread-safe stores share the same
// behavioral contract.
func runStoreTests(t *testing.T, factory func() Store) {
	t.Helper()

	t.Run("SetGet", func(t *testing.T) {
		s := factory()
		s.Set("foo", "bar")
		v, ok := s.Get("foo")
		if !ok || v != "bar" {
			t.Fatalf("got (%q,%v), want (\"bar\",true)", v, ok)
		}
	})

	t.Run("GetMissing", func(t *testing.T) {
		s := factory()
		if _, ok := s.Get("missing"); ok {
			t.Fatal("expected missing key to return ok=false")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		s := factory()
		s.Set("k", "v")
		if !s.Delete("k") {
			t.Fatal("expected Delete to return true for existing key")
		}
		if s.Delete("k") {
			t.Fatal("expected Delete to return false for missing key")
		}
		if _, ok := s.Get("k"); ok {
			t.Fatal("expected key to be gone after Delete")
		}
	})

	t.Run("LenAndKeys", func(t *testing.T) {
		s := factory()
		s.Set("a", "1")
		s.Set("b", "2")
		if s.Len() != 2 {
			t.Fatalf("Len=%d, want 2", s.Len())
		}
		if len(s.Keys()) != 2 {
			t.Fatalf("Keys len=%d, want 2", len(s.Keys()))
		}
	})
}

func TestBasicStore(t *testing.T) {
	runStoreTests(t, func() Store { return NewBasic() })
}

func TestSafeStore(t *testing.T) {
	runStoreTests(t, func() Store { return NewSafe() })
}

func TestSafeStoreConcurrentAccess(t *testing.T) {
	s := NewSafe()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Set("k", "v")
			_, _ = s.Get("k")
		}(i)
	}
	wg.Wait()
}
