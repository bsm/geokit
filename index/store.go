package index

import "sync"

// StoreReader is an abstract KV store reader
type StoreReader interface {
	// Get must return a value for a given key or nil of not found
	Get(key []byte) (value []byte, err error)
	// Close implements the io.Closer interface
	Close() error
}

// StoreWriter is an abstract KV store writer
type StoreWriter interface {
	// Put adds a key/value pair to the store
	Put(key, value []byte) error
	// Close implements the io.Closer interface
	Close() error
}

// InMemStore implements StoreReader + StoreWriter, only use for tests
type InMemStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewInMemStore inits an InMemStore
func NewInMemStore() *InMemStore {
	return &InMemStore{data: make(map[string][]byte)}
}

// Len returns the number of stored keys
func (m *InMemStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.data)
}

// Get implements StoreReader
func (m *InMemStore) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.data[string(key)], nil
}

// Put implements StoreWriter
func (m *InMemStore) Put(key, value []byte) error {
	sk := string(key)
	m.mu.Lock()
	val := m.data[sk]
	m.data[sk] = append(val[:0], value...)
	m.mu.Unlock()
	return nil
}

// Close implements StoreReader + StoreWriter
func (*InMemStore) Close() error { return nil }
