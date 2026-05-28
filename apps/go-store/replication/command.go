package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// CmdKind identifies the mutation a Command represents. Stable on the
// wire — values are written to the replication log and must never be
// reassigned. Add new kinds by appending.
type CmdKind uint8

const (
	CmdInvalid CmdKind = 0
	CmdSet     CmdKind = 1
	CmdDelete  CmdKind = 2
)

// cmdVersion is the byte we stamp at the start of every encoded
// Command. Bumping it is how we evolve the format without silently
// mis-decoding old log entries — a decoder that sees an unknown
// version returns an error instead of guessing.
const cmdVersion uint8 = 1

// Maximum sizes to keep a malicious / corrupt log entry from making us
// allocate gigabytes. 1 MiB matches the request body cap in the HTTP
// layer; keys are bounded much tighter.
const (
	maxKeyLen   = 1 << 12 // 4 KiB
	maxValueLen = 1 << 20 // 1 MiB
)

// Command is a single mutation to the store. It is the unit of
// replication: every Set/Delete becomes one Command, gets serialized,
// committed through consensus, and then applied identically on every
// replica. Reads are never Commands.
//
// Determinism rule: nothing in Apply may depend on data outside the
// Command and the current store state. No clocks, no randomness, no
// map-iteration order leaking into mutations.
type Command struct {
	Kind  CmdKind
	Key   string
	Value string // ignored for CmdDelete
}

// Marshal encodes c using the wire format documented at the top of
// the file. Returns an error for unknown kinds or oversize fields so
// we fail fast at the producer rather than at every replica.
func (c Command) Marshal() ([]byte, error) {
	switch c.Kind {
	case CmdSet, CmdDelete:
	default:
		return nil, fmt.Errorf("marshal command: unknown kind %d", c.Kind)
	}
	if len(c.Key) == 0 {
		return nil, errors.New("marshal command: empty key")
	}
	if len(c.Key) > maxKeyLen {
		return nil, fmt.Errorf("marshal command: key too large (%d > %d)", len(c.Key), maxKeyLen)
	}
	if len(c.Value) > maxValueLen {
		return nil, fmt.Errorf("marshal command: value too large (%d > %d)", len(c.Value), maxValueLen)
	}

	// Worst-case size: 2 fixed bytes + 2 uvarints (max 10 each) + key + value.
	buf := make([]byte, 0, 2+binary.MaxVarintLen64*2+len(c.Key)+len(c.Value))
	buf = append(buf, cmdVersion, byte(c.Kind))
	buf = binary.AppendUvarint(buf, uint64(len(c.Key)))
	buf = append(buf, c.Key...)
	// Always write a value length, even for Delete (0). Keeps the
	// parser branch-free and the format self-describing.
	val := c.Value
	if c.Kind == CmdDelete {
		val = ""
	}
	buf = binary.AppendUvarint(buf, uint64(len(val)))
	buf = append(buf, val...)
	return buf, nil
}

// UnmarshalCommand is the inverse of Command.Marshal. It validates
// version, kind, and length bounds; any structural problem returns
// an error and zero Command. Trailing bytes are an error (catches
// truncation-with-padding bugs early).
func UnmarshalCommand(b []byte) (Command, error) {
	if len(b) < 2 {
		return Command{}, errors.New("unmarshal command: short header")
	}
	if b[0] != cmdVersion {
		return Command{}, fmt.Errorf("unmarshal command: unsupported version %d", b[0])
	}
	kind := CmdKind(b[1])
	switch kind {
	case CmdSet, CmdDelete:
	default:
		return Command{}, fmt.Errorf("unmarshal command: unknown kind %d", kind)
	}
	rest := b[2:]

	keyLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return Command{}, errors.New("unmarshal command: bad key length")
	}
	if keyLen == 0 || keyLen > maxKeyLen {
		return Command{}, fmt.Errorf("unmarshal command: key length out of range (%d)", keyLen)
	}
	rest = rest[n:]
	if uint64(len(rest)) < keyLen {
		return Command{}, errors.New("unmarshal command: truncated key")
	}
	key := string(rest[:keyLen])
	rest = rest[keyLen:]

	valLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return Command{}, errors.New("unmarshal command: bad value length")
	}
	if valLen > maxValueLen {
		return Command{}, fmt.Errorf("unmarshal command: value length out of range (%d)", valLen)
	}
	rest = rest[n:]
	if uint64(len(rest)) < valLen {
		return Command{}, errors.New("unmarshal command: truncated value")
	}
	val := string(rest[:valLen])
	rest = rest[valLen:]

	if len(rest) != 0 {
		return Command{}, fmt.Errorf("unmarshal command: %d trailing bytes", len(rest))
	}
	return Command{Kind: kind, Key: key, Value: val}, nil
}
