package replication

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Kind: CmdSet, Key: "a", Value: "b"},
		{Kind: CmdSet, Key: "k", Value: ""},                           // empty value is legal for Set
		{Kind: CmdSet, Key: "unicode-键", Value: "値\x00with\x00nulls"}, // arbitrary bytes
		{Kind: CmdSet, Key: "big", Value: strings.Repeat("x", 1<<16)}, // exercise multi-byte uvarint
		{Kind: CmdDelete, Key: "gone"},
		{Kind: CmdDelete, Key: "gone", Value: "ignored-on-delete"}, // value must be dropped
	}
	for _, in := range cases {
		t.Run(in.Key, func(t *testing.T) {
			b, err := in.Marshal()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got, err := UnmarshalCommand(b)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			want := in
			if want.Kind == CmdDelete {
				want.Value = "" // Marshal drops it; round-trip should match
			}
			if got != want {
				t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, want)
			}
		})
	}
}

func TestCommandMarshalRejects(t *testing.T) {
	cases := map[string]Command{
		"unknown kind": {Kind: CmdInvalid, Key: "k"},
		"empty key":    {Kind: CmdSet, Key: "", Value: "v"},
		"huge key":     {Kind: CmdSet, Key: strings.Repeat("x", maxKeyLen+1), Value: "v"},
		"huge value":   {Kind: CmdSet, Key: "k", Value: strings.Repeat("x", maxValueLen+1)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Marshal(); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestUnmarshalRejects(t *testing.T) {
	good, err := (Command{Kind: CmdSet, Key: "k", Value: "v"}).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"empty":           nil,
		"one byte":        {1},
		"bad version":     {99, byte(CmdSet), 1, 'k', 1, 'v'},
		"unknown kind":    {cmdVersion, 99, 1, 'k', 0},
		"zero key len":    {cmdVersion, byte(CmdSet), 0, 0},
		"truncated key":   {cmdVersion, byte(CmdSet), 5, 'a', 'b'},
		"truncated value": {cmdVersion, byte(CmdSet), 1, 'k', 5, 'v'},
		"trailing bytes":  append(bytes.Clone(good), 0xff),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalCommand(b); err == nil {
				t.Fatalf("expected error, got nil (input=%x)", b)
			}
		})
	}
}

// TestCommandDeterministic guards the property Raft relies on: the same
// logical command always serializes to the exact same bytes. If this
// ever fails (e.g. someone adds a map field), replicas will diverge.
func TestCommandDeterministic(t *testing.T) {
	c := Command{Kind: CmdSet, Key: "k", Value: "v"}
	first, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		again, err := c.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("non-deterministic encoding at iter %d", i)
		}
	}
}
