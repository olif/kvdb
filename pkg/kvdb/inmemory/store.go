package inmemory

import (
	"fmt"
	"log"
	"sync"

	"github.com/olif/kvdb/pkg/kvdb"
)

// Store is a simple key value store which keeps the state in-memory. It
// has the performance characteristics of go:s map structure
type Store struct {
	maxRecordSize int
	logger        *log.Logger

	sync.RWMutex
	table map[string][]byte
}

// Config contains the configuration properties for the inmemory store
type Config struct {
	MaxRecordSize int
	Logger        *log.Logger
}

// NewStore returns a new memory store
func NewStore(config Config) *Store {
	return &Store{
		maxRecordSize: config.MaxRecordSize,
		logger:        config.Logger,

		table: map[string][]byte{},
	}
}

// Get returns the value associated with the key or a kvdb.NotFoundError if the
// key was not found, or any other error encountered
func (s *Store) Get(key string) ([]byte, error) {
	s.RLock()
	v, ok := s.table[key]
	s.RUnlock()
	if !ok {
		return nil, kvdb.NewNotFoundError(key)
	}

	return v, nil
}

// Put saves the value to the database and returns any error encountered
func (s *Store) Put(key string, value []byte) error {
	size := len([]byte(key)) + len(value)
	if size > s.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", s.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}
	s.Lock()
	s.table[key] = value
	s.Unlock()
	return nil
}

// Delete removes the value from the store
func (s *Store) Delete(key string) error {
	s.Lock()
	delete(s.table, key)
	s.Unlock()
	return nil
}

// Close closes the store
func (s *Store) Close() error {
	s.logger.Print("Closing database")
	return nil
}

// IsNotFoundError returns true if the error signales that a non-existing key
// was requested
func (s *Store) IsNotFoundError(err error) bool {
	return kvdb.IsNotFoundError(err)
}

// IsBadRequestError returns true if the error, or any of the wrapped errors
// is of type BadRequestError
func (s *Store) IsBadRequestError(err error) bool {
	return kvdb.IsBadRequestError(err)
}
