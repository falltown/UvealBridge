package store

import (
	"testing"

	"github.com/falltown/UvealBridge/apps/go-store/replication"
)

// runApplierTests is the behavioral contract every Applier must obey.
// Shared so Safe and WAL stores can't drift in subtle ways.
func runApplierTests(t *testing.T, factory func() replication.Applier) {
	t.Helper()

	t.Run("ApplySet", func(t *testing.T) {
		a := factory()
		res, err := a.Apply(replication.Command{Kind: replication.CmdSet, Key: "k", Value: "v"})
		if err != nil {
			t.Fatalf("Apply Set: %v", err)
		}
		if res != nil {
			t.Fatalf("Set result=%v, want nil", res)
		}

		got, ok := a.(Store).Get("k")
		if !ok || got != "v" {
			t.Fatalf("Get after Apply Set: (%q,%v), want (\"v\",true)", got, ok)
		}
	})

	t.Run("ApplyDeleteExisting", func(t *testing.T) {
		a := factory()
		a.(Store).Set("k", "v")
		res, err := a.Apply(replication.Command{Kind: replication.CmdDelete, Key: "k"})
		if err != nil {
			t.Fatalf("Apply Delete: %v", err)
		}
		existed, ok := res.(bool)
		if !ok || !existed {
			t.Fatalf("Delete existing result=%v, want true", res)
		}
		if _, ok := a.(Store).Get("k"); ok {
			t.Fatal("key still present after Apply Delete")
		}
	})

	t.Run("ApplyDeleteMissing", func(t *testing.T) {
		a := factory()
		res, err := a.Apply(replication.Command{Kind: replication.CmdDelete, Key: "nope"})
		if err != nil {
			t.Fatalf("Apply Delete missing: %v", err)
		}
		existed, ok := res.(bool)
		if !ok || existed {
			t.Fatalf("Delete missing result=%v, want false", res)
		}
	})

	t.Run("ApplyUnknownKind", func(t *testing.T) {
		a := factory()
		_, err := a.Apply(replication.Command{Kind: replication.CmdInvalid, Key: "k"})
		if err == nil {
			t.Fatal("expected error for unknown kind, got nil")
		}
	})
}

func TestSafeStoreApplier(t *testing.T) {
	runApplierTests(t, func() replication.Applier { return NewSafe() })
}

func TestWALStoreApplier(t *testing.T) {
	runApplierTests(t, func() replication.Applier {
		dir := t.TempDir()
		s, err := NewWAL(dir)
		if err != nil {
			t.Fatalf("NewWAL: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
