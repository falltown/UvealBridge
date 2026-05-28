package store

// Store is the common interface for all key-value store implementations.
type Store interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Delete(key string) bool
	Len() int
	Keys() []string
}
