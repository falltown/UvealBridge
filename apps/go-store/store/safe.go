package store

import (
	"fmt"
	"sync"

	"github.com/falltown/UvealBridge/apps/go-store/replication"
)

// SafeStore is a thread-safe in-memory key-value store backed by an
// RWMutex, suitable for concurrent use by multiple goroutines.
type SafeStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewSafe() *SafeStore {
	return &SafeStore{data: make(map[string]string)}
}

func (s *SafeStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *SafeStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *SafeStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	delete(s.data, key)
	return true
}

func (s *SafeStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *SafeStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Apply makes SafeStore satisfy replication.Applier. It is the single
// choke point through which replicated mutations enter the store.
// Direct Set/Delete calls remain valid for tests and the no-op
// replicator; once Raft is wired in, the HTTP layer will only ever
// reach the store via this method.
func (s *SafeStore) Apply(cmd replication.Command) (any, error) {
	switch cmd.Kind {
	case replication.CmdSet:
		s.Set(cmd.Key, cmd.Value)
		return nil, nil
	case replication.CmdDelete:
		return s.Delete(cmd.Key), nil
	default:
		return nil, fmt.Errorf("safe store: unknown command kind %d", cmd.Kind)
	}
}

// Compile-time check that *SafeStore satisfies replication.Applier.
var _ replication.Applier = (*SafeStore)(nil)
