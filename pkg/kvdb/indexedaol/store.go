package indexedaol

import (
	"fmt"
	"io"
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

	index      *index
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

type index struct {
	mutex  sync.RWMutex
	table  map[string]int64
	cursor int64
}

func (i *index) get(key string) (int64, bool) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	val, ok := i.table[key]
	return val, ok
}

func (i *index) put(key string, written int64) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.table[key] = i.cursor
	i.cursor += written
}

// NewStore returns a new SimpleLogStore
func NewStore(config Config) (*Store, error) {
	var (
		maxRecordSize = defaultMaxRecordSize
		storagePath   = path.Join(config.BasePath, logFile)
		async         = defaultAsync
		logger        = voidLogger
	)

	f, err := os.OpenFile(storagePath, os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()

	if config.MaxRecordSize != nil {
		maxRecordSize = *config.MaxRecordSize
	}

	if config.Async != nil {
		async = *config.Async
	}

	if config.Logger != nil {
		logger = config.Logger
	}

	idx, err := buildIndex(storagePath, maxRecordSize)
	if err != nil {
		return nil, err
	}

	logger.Printf("Index rebuilt with %d records", len(idx.table))

	return &Store{
		storagePath:   storagePath,
		maxRecordSize: maxRecordSize,
		async:         async,
		logger:        logger,
		index:         idx,
	}, nil
}

func buildIndex(filePath string, maxRecordSize int) (*index, error) {
	idx := index{
		cursor: 0,
		table:  map[string]int64{},
	}

	f, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0600)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	scanner, err := record.NewScanner(f, maxRecordSize)
	if err != nil {
		return nil, err
	}

	for scanner.Scan() {
		record := scanner.Record()
		idx.put(record.Key(), int64(record.Size()))
	}

	if scanner.Err() != nil {
		return nil, fmt.Errorf("could not scan entry, %w", err)
	}

	return &idx, nil
}

// Get returns the value for the given key, a kvdb.NotFoundError if the
// key was not found or other errors encountered
func (store *Store) Get(key string) ([]byte, error) {
	offset, ok := store.index.get(key)
	if !ok {
		return nil, kvdb.NewNotFoundError(key)
	}

	f, err := os.OpenFile(store.storagePath, os.O_CREATE|os.O_RDONLY, 0600)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	scanner, err := record.NewScanner(f, store.maxRecordSize)
	if err != nil {
		return nil, err
	}

	if scanner.Scan() {
		record := scanner.Record()

		if record.IsTombstone() {
			return nil, kvdb.NewNotFoundError(key)
		}

		return record.Value(), nil
	}

	return nil, kvdb.NewNotFoundError(key)
}

// Put stores the value for the given key
func (store *Store) Put(key string, value []byte) error {
	record := record.NewValue(key, value)
	return store.append(record)
}

// Delete deletes the data for the given key
func (store *Store) Delete(key string) error {
	record := record.NewTombstone(key)
	return store.append(record)
}

func (store *Store) append(record *record.Record) error {
	if record.Size() > store.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", store.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}

	store.writeMutex.Lock()
	defer store.writeMutex.Unlock()

	file, err := os.OpenFile(store.storagePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("could not open file: %s for write, %w", store.storagePath, err)
	}

	n, err := record.Write(file)
	if err != nil {
		return fmt.Errorf("could not write record to file: %s, %w", store.storagePath, err)
	}

	if !store.async {
		if err := file.Sync(); err != nil {
			return err
		}
	}

	if err := file.Close(); err != nil {
		return err
	}

	store.index.put(record.Key(), int64(n))
	return nil
}

// Close closes the store
func (store *Store) Close() error {
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
