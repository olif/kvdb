package compactedaol

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/record"
)

const (
	defaultAsync               = false
	defaultMaxRecordSize       = 1024 * 1024                //1Mb
	defaultSegmentMaxSize      = 4096 * 1024                //4Mb
	defaultCompactionThreshold = defaultSegmentMaxSize * 10 // 40 Mb
	defaultCompactionInterval  = 10 * time.Second
	closedSegmentSuffix        = ".cseg"
	openSegmentSuffix          = ".oseg"
	compactionSuffix           = ".comp"
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

	openSegment *segment

	// closedSegmentStack contains all immutable segments. The newest segment
	// has position 0 and the oldest at the last position
	closedSegments *segmentStack

	openSegmentMutex sync.RWMutex
	writeMutex       sync.Mutex

	compacter *compacter
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
	// CompactionThreshold defines the upper bound for how large a file can be
	// and still be subject to compaction
	CompactionThreshold *int
	// CompactionInterval defines the how often compaction should be run. Only
	// one compaction at a time will be run.
	CompactionInterval *time.Duration

	Logger *log.Logger
}

// NewStore returns a new SimpleLogStore
func NewStore(config Config) (*Store, error) {
	var (
		maxRecordSize       = defaultMaxRecordSize
		maxSegmentSize      = defaultSegmentMaxSize
		compactionThreshold = defaultCompactionThreshold
		compactionInterval  = defaultCompactionInterval
		storagePath         = config.BasePath
		async               = defaultAsync
		logger              = voidLogger
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

	if config.CompactionThreshold != nil {
		compactionThreshold = *config.CompactionThreshold
	}

	if config.CompactionInterval != nil {
		compactionInterval = *config.CompactionInterval
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
		openSegment:    openSegment,
		closedSegments: closedSegments,
	}

	store.compacter = newCompacter(store.onCompactionDone, compactionInterval, compactionThreshold, storagePath, maxRecordSize, logger)
	store.compacter.run()

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
	s.openSegmentMutex.RLock()
	record, err := s.openSegment.get(key)
	s.openSegmentMutex.RUnlock()

	if err == nil {
		return resolveRecord(record)
	} else if !kvdb.IsNotFoundError(err) {
		return nil, err
	}

	iter := s.closedSegments.iter()

	for iter.hasNext() {
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
		s.rotateOpenSegment()
	}

	return s.openSegment.append(record)
}

func (s *Store) rotateOpenSegment() {
	s.openSegmentMutex.Lock()
	defer s.openSegmentMutex.Unlock()

	newOpenSegment := newSegment(s.storagePath, s.maxRecordSize, s.async, s.logger)
	err := s.openSegment.changeSuffix(openSegmentSuffix, closedSegmentSuffix)
	if err != nil {
		panic(err)
	}

	s.closedSegments.push(s.openSegment)
	s.openSegment = newOpenSegment
}

// Close closes the store
func (s *Store) Close() error {
	s.logger.Println("Closing database")
	s.compacter.stop()
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

func (s *Store) onCompactionDone(targetFile string, compactedFiles []string) error {
	newSegment, err := fromFile(targetFile, s.maxRecordSize, s.async, s.logger)
	if err != nil {
		return fmt.Errorf("could not create segment of compaction target: %w", err)
	}

	err = s.closedSegments.replace(func(segment *segment) bool {
		return segment.storagePath == compactedFiles[0]
	}, newSegment)

	if err != nil {
		return fmt.Errorf("could not replace with compacted segment: %w", err)
	}

	filesToRemove := compactedFiles[1:]
	for i := range filesToRemove {
		err = s.closedSegments.remove(func(segment *segment) bool {
			return filesToRemove[i] == segment.storagePath
		})

		if err != nil {
			return fmt.Errorf("could not remove compacted segment: %w", err)
		}
	}

	if err = newSegment.changeSuffix(compactionSuffix, closedSegmentSuffix); err != nil {
		return fmt.Errorf("could not rename compacted segment: %w", err)
	}

	return nil
}
