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
	defaultMaxRecordSize  = 1024 * 1024 //1Mb
	defaultAsync          = false
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

	writeMutex     *sync.Mutex
	segmentMutex   *sync.RWMutex
	openSegment    segment
	closedSegments []segment
}

// Config contains the configuration properties for the simplelog store
type Config struct {
	// Storage path
	BasePath string
	// Sets a limit on the size of the records
	MaxRecordSize *int
	// If true, fsync will be called on every write
	Async *bool
	// MaxSegmentSize defines the maximum size of the segments, must be >= MaxRecordSize
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

	return &Store{
		storagePath:    storagePath,
		maxRecordSize:  maxRecordSize,
		maxSegmentSize: int64(maxSegmentSize),
		async:          async,
		logger:         logger,
		writeMutex:     &sync.Mutex{},
		segmentMutex:   &sync.RWMutex{},
		openSegment:    openSegment,
	}, nil
}

func loadOpenSegment(storagePath string, maxRecordSize int, async bool, log *log.Logger) (segment, error) {
	fi, err := listFilesWithSuffix(storagePath, openSegmentSuffix, true)
	if err != nil {
		return segment{}, fmt.Errorf("could not load open segment: %w", err)
	}

	switch len(fi) {
	case 0:
		return newSegment(storagePath, maxRecordSize, async, log), nil
	case 1:
		return fromFile(fi[0].filepath, maxRecordSize, async, log)
	default:
		return segment{}, fmt.Errorf("more than one open segment found")
	}
}

// Get returns the value associated with the key or a kvdb.NotFoundError if the
// key was not found, or any other error encountered
func (s *Store) Get(key string) ([]byte, error) {
	return s.openSegment.get(key)
}

// Put saves the value to the database and returns any error encountered
func (s *Store) Put(key string, value []byte) error {
	record := record.NewValue(key, value)

	if record.Size() > s.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", s.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}

	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	if s.openSegment.size()+int64(record.Size()) > s.maxSegmentSize {
		s.rotateOpenSegment()
	}
	return s.openSegment.append(record)
}

// Delete removes the value from the store and returns any error encountered
func (s *Store) Delete(key string) error {
	record := record.NewTombstone(key)
	return s.openSegment.append(record)
}

func (s *Store) rotateOpenSegment() {
	newOpenSegment := newSegment(s.storagePath, s.maxRecordSize, s.async, s.logger)
	s.segmentMutex.Lock()
	defer s.segmentMutex.Unlock()
	closedSegment, err := s.openSegment.changeSuffix(openSegmentSuffix, closedSegmentSuffix)
	if err != nil {
		panic(err)
	}
	s.closedSegments = append([]segment{closedSegment}, s.closedSegments...)
	s.openSegment = newOpenSegment
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
