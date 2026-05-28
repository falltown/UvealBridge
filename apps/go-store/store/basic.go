package store

// BasicStore is a simple, non-thread-safe in-memory key-value store.
// Use it from a single goroutine only.
type BasicStore struct {
	data map[string]string
}

func NewBasic() *BasicStore {
	return &BasicStore{data: make(map[string]string)}
}

func (s *BasicStore) Set(key, value string) {
	s.data[key] = value
}

func (s *BasicStore) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s *BasicStore) Delete(key string) bool {
	if _, ok := s.data[key]; !ok {
		return false
	}
	delete(s.data, key)
	return true
}

func (s *BasicStore) Len() int {
	return len(s.data)
}

func (s *BasicStore) Keys() []string {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
