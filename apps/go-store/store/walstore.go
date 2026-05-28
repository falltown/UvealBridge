package store

import (
	"fmt"
	"sync"

	"github.com/falltown/UvealBridge/apps/go-store/replication"
	"github.com/falltown/UvealBridge/apps/go-store/wal"
)

// WALStore is a thread-safe key-value store backed by an append-only
// write-ahead log on disk. Every mutation is durably fsynced to the log
// before being applied to the in-memory map, so an ack'd write survives
// process crash. Reads are served entirely from memory.
//
// The log grows without bound (no compaction yet) and every write does
// its own fsync (no batching yet). Both are deliberate omissions for
// the first cut.
type WALStore struct {
	mu   sync.RWMutex
	data map[string]string
	log  *wal.Log
}

// NewWAL opens (or creates) the WAL inside dir and replays it to rebuild
// in-memory state. dir must already exist.
func NewWAL(dir string) (*WALStore, error) {
	l, err := wal.Open(dir)
	if err != nil {
		return nil, err
	}
	s := &WALStore{
		data: make(map[string]string),
		log:  l,
	}
	if err := l.Replay(func(b []byte) error {
		rec, err := decodeRecord(b)
		if err != nil {
			return fmt.Errorf("decode record: %w", err)
		}
		s.apply(rec)
		return nil
	}); err != nil {
		l.Close()
		return nil, err
	}
	return s, nil
}

// apply mutates the in-memory map according to rec. Caller holds mu.
func (s *WALStore) apply(rec walRecord) {
	switch rec.Op {
	case opSet:
		s.data[rec.Key] = rec.Value
	case opDel:
		delete(s.data, rec.Key)
	}
}

// appendAndApply durably logs rec and then applies it to the in-memory
// map. Order matters: log first, memory second, so a crash between the
// two leaves a record that replay will re-apply (we never ack a write
// that isn't durable). Caller holds mu.
func (s *WALStore) appendAndApply(rec walRecord) {
	b, err := encodeRecord(rec)
	if err != nil {
		panic(fmt.Errorf("encode record: %w", err))
	}
	if err := s.log.Append(b); err != nil {
		// Store's interface returns no error from Set/Delete; a disk
		// failure here is unrecoverable. Panicking is safer than
		// silently losing the write — the pod restarts, replays what
		// was durable, and the client retries.
		panic(fmt.Errorf("wal append: %w", err))
	}
	s.apply(rec)
}

func (s *WALStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendAndApply(walRecord{Op: opSet, Key: key, Value: value})
}

func (s *WALStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *WALStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	s.appendAndApply(walRecord{Op: opDel, Key: key})
	return true
}

func (s *WALStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *WALStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Close flushes pending writes and closes the WAL. Safe to call
// multiple times. Should be invoked during graceful shutdown.
func (s *WALStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil
	}
	err := s.log.Close()
	s.log = nil
	return err
}

// Apply makes WALStore satisfy replication.Applier. Same contract as
// SafeStore.Apply: deterministic, one call per committed Command.
// Reuses Set/Delete so the WAL append + fsync discipline stays in one
// place.
func (s *WALStore) Apply(cmd replication.Command) (any, error) {
	switch cmd.Kind {
	case replication.CmdSet:
		s.Set(cmd.Key, cmd.Value)
		return nil, nil
	case replication.CmdDelete:
		return s.Delete(cmd.Key), nil
	default:
		return nil, fmt.Errorf("wal store: unknown command kind %d", cmd.Kind)
	}
}

// Compile-time check that *WALStore satisfies the Store interface.
var _ Store = (*WALStore)(nil)

// Compile-time check that *WALStore satisfies replication.Applier.
var _ replication.Applier = (*WALStore)(nil)
