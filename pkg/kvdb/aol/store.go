package aol

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/record"
)

const (
	logFile              = "store.db"
	defaultMaxRecordSize = 1024 * 1024 //1Mb
	defaultAsync         = false
)

var voidLogger = log.New(ioutil.Discard, "", log.LstdFlags)

// Store implements the kvdb store interface providing a simple key-value
// database engine based on an append-log.
type Store struct {
	storagePath   string
	maxRecordSize int
	logger        *log.Logger
	async         bool

	writeMutex sync.Mutex
}

// Config contains the configuration properties for the simplelog store
type Config struct {
	// Storage path
	BasePath string
	// Sets a limit on the size of the records
	MaxRecordSize *int
	// If true, fsync will be called on every write
	Async *bool

	Logger *log.Logger
}

// NewStore returns a new SimpleLogStore
func NewStore(config Config) (*Store, error) {
	var (
		maxRecordSize = defaultMaxRecordSize
		storagePath   = path.Join(config.BasePath, logFile)
		async         = defaultAsync
		logger        = voidLogger
	)

	if _, err := os.OpenFile(storagePath, os.O_CREATE, 0600); err != nil {
		return nil, err
	}

	if config.MaxRecordSize != nil {
		maxRecordSize = *config.MaxRecordSize
	}

	if config.Async != nil {
		async = *config.Async
	}

	if config.Logger != nil {
		logger = config.Logger
	}

	return &Store{
		storagePath:   storagePath,
		maxRecordSize: maxRecordSize,
		async:         async,
		logger:        logger,
	}, nil
}

// Get returns the value associated with the key or a kvdb.NotFoundError if the
// key was not found, or any other error encountered
func (s *Store) Get(key string) ([]byte, error) {
	file, err := os.Open(s.storagePath)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s, %w", s.storagePath, err)
	}

	scanner, err := record.NewScanner(file, s.maxRecordSize)
	if err != nil {
		return nil, fmt.Errorf("could not create scanner for file: %s, %w", s.storagePath, err)
	}

	var found *record.Record
	for scanner.Scan() {
		record := scanner.Record()
		if record.Key() == key {
			found = record
		}
	}

	if scanner.Err() != nil {
		s.logger.Printf("error encountered: %s", scanner.Err())
		return nil, scanner.Err()
	}

	if found == nil || found.IsTombstone() {
		return nil, kvdb.NewNotFoundError(key)
	}

	return found.Value(), nil
}

// Put saves the value to the database and returns any error encountered
func (s *Store) Put(key string, value []byte) error {
	record := record.NewValue(key, value)
	return s.append(record)
}

// Delete removes the value from the store and returns any error encountered
func (s *Store) Delete(key string) error {
	record := record.NewTombstone(key)
	return s.append(record)
}

func (s *Store) append(record *record.Record) error {
	if record.Size() > s.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", s.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}

	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	file, err := os.OpenFile(s.storagePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("could not open file: %s for write, %w", s.storagePath, err)
	}

	_, err = record.Write(file)
	if err != nil {
		return fmt.Errorf("could not write record to file: %s, %w", s.storagePath, err)
	}

	if !s.async {
		return file.Sync()
	}

	return file.Close()
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
