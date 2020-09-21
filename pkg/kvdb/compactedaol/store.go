package compactedaol

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/record"
)

const (
	defaultAsync          = false
	defaultMaxRecordSize  = 1024 * 1024 //1Mb
	defaultSegmentMaxSize = 4096 * 1024 //4Mb
	closedSegmentSuffix   = ".cseg"
	openSegmentSuffix     = ".oseg"
)

var voidLogger = log.New(ioutil.Discard, "", log.LstdFlags)

// Store implements the kvdb store interface providing a simple key-value
// database engine based on an append-log.
type Store struct {
	storagePath    string
	maxRecordSize  int
	maxSegmentSize int64
	logger         *log.Logger
	async          bool

	writeMutex  *sync.Mutex
	openSegment *segment

	// closedSegmentStack contains all immutable segments. The newest segment
	// has position 0 and the oldest at the last position
	closedSegments *segmentStack
}

// Config contains the configuration properties for the compacted aol store
type Config struct {
	// Storage path
	BasePath string
	// Sets a limit on the size of the records
	MaxRecordSize *int
	// If true, fsync will be called on every write
	Async *bool
	// MaxSegmentSize defines the maximum size of the segments, must be >=
	// MaxRecordSize
	MaxSegmentSize *int

	Logger *log.Logger
}

// NewStore returns a new SimpleLogStore
func NewStore(config Config) (*Store, error) {
	var (
		maxRecordSize  = defaultMaxRecordSize
		maxSegmentSize = defaultSegmentMaxSize
		storagePath    = config.BasePath
		async          = defaultAsync
		logger         = voidLogger
	)

	if _, err := os.OpenFile(storagePath, os.O_CREATE, 0600); err != nil {
		return nil, err
	}

	if config.MaxRecordSize != nil {
		maxRecordSize = *config.MaxRecordSize
	}

	if config.MaxSegmentSize != nil {
		maxSegmentSize = *config.MaxSegmentSize
	}

	if config.Async != nil {
		async = *config.Async
	}

	if config.Logger != nil {
		logger = config.Logger
	}

	openSegment, err := loadOpenSegment(storagePath, maxRecordSize, async, logger)
	if err != nil {
		return nil, err
	}

	closedSegments, err := loadClosedSegments(storagePath, maxRecordSize, async, logger)
	if err != nil {
		return nil, err
	}

	store := &Store{
		storagePath:    storagePath,
		maxRecordSize:  maxRecordSize,
		maxSegmentSize: int64(maxSegmentSize),
		async:          async,
		logger:         logger,
		writeMutex:     &sync.Mutex{},
		openSegment:    openSegment,
		closedSegments: closedSegments,
	}

	return store, nil
}

func loadOpenSegment(storagePath string, maxRecordSize int, async bool, log *log.Logger) (*segment, error) {
	fi, err := listFilesWithSuffix(storagePath, openSegmentSuffix, true)
	if err != nil {
		return nil, fmt.Errorf("could not load open segment: %w", err)
	}

	switch len(fi) {
	case 0:
		return newSegment(storagePath, maxRecordSize, async, log), nil
	case 1:
		return fromFile(fi[0].filepath, maxRecordSize, async, log)
	default:
		return nil, fmt.Errorf("more than one open segment found")
	}
}

func loadClosedSegments(storagePath string, maxRecordSize int, async bool, log *log.Logger) (*segmentStack, error) {
	fis, err := listFilesWithSuffix(storagePath, closedSegmentSuffix, true)
	if err != nil {
		return nil, fmt.Errorf("could not list closed segment files: %w", err)
	}

	closedSegments := newSegmentStack()
	for _, fi := range fis {
		s, err := fromFile(fi.filepath, maxRecordSize, async, log)
		if err != nil {
			return nil, fmt.Errorf("could not load closed segment: %s, %w", fi.filepath, err)
		}
		closedSegments.push(s)
	}

	return closedSegments, nil
}

// Get returns the value associated with the key or a kvdb.NotFoundError if the
// key was not found, or any other error encountered
func (s *Store) Get(key string) ([]byte, error) {
	record, err := s.openSegment.get(key)
	if err == nil {
		return resolveRecord(record)
	} else if !kvdb.IsNotFoundError(err) {
		return nil, err
	}

	for iter := s.closedSegments.iter(); iter.hasNext(); {
		record, err := iter.next().get(key)
		if err == nil {
			return resolveRecord(record)
		} else if !kvdb.IsNotFoundError(err) {
			return nil, err
		}
	}

	return nil, kvdb.NewNotFoundError(key)
}

func resolveRecord(record *record.Record) ([]byte, error) {
	if record.IsTombstone() {
		return nil, kvdb.NewNotFoundError(record.Key())
	}

	return record.Value(), nil
}

// Put saves the value to the database and returns any error encountered
func (s *Store) Put(key string, value []byte) error {
	record := record.NewValue(key, value)

	if record.Size() > s.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", s.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}

	return s.append(record)
}

// Delete removes the value from the store and returns any error encountered
func (s *Store) Delete(key string) error {
	record := record.NewTombstone(key)
	return s.append(record)
}

func (s *Store) append(record *record.Record) error {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	if s.openSegment.size()+int64(record.Size()) > s.maxSegmentSize {
		if err := s.rotateOpenSegment(); err != nil {
			return fmt.Errorf("could not write value due to segment rotation failure: %w", err)
		}
	}

	return s.openSegment.append(record)
}

func (s *Store) rotateOpenSegment() error {
	newOpenSegment := newSegment(s.storagePath, s.maxRecordSize, s.async, s.logger)
	if err := s.openSegment.changeSuffix(openSegmentSuffix, closedSegmentSuffix); err != nil {
		return fmt.Errorf("could not change open segment suffix: %w", err)
	}

	s.closedSegments.push(s.openSegment)
	s.openSegment = newOpenSegment
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
