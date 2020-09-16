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
	compactionSuffix      = ".comp"
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

	writeMutex   *sync.Mutex
	segmentMutex *sync.RWMutex
	openSegment  *segment

	// closedSegmentStack contains all immutable segments. The newest segment
	// has position 0 and the oldest is at len(closedSegmentStack) - 1
	closedSegmentStack []*segment

	compacter *NrOfFilesCompacter
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

	closedSegments, err := loadClosedSegments(storagePath, maxRecordSize, async, logger)
	if err != nil {
		return nil, err
	}

	store := &Store{
		storagePath:        storagePath,
		maxRecordSize:      maxRecordSize,
		maxSegmentSize:     int64(maxSegmentSize),
		async:              async,
		logger:             logger,
		writeMutex:         &sync.Mutex{},
		segmentMutex:       &sync.RWMutex{},
		openSegment:        openSegment,
		closedSegmentStack: closedSegments,
	}

	compacter := NewNrOfFilesCompacter(storagePath, closedSegmentSuffix, logger, 3, maxRecordSize, store.onCompactionDone)
	store.compacter = compacter
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

func loadClosedSegments(storagePath string, maxRecordSize int, async bool, log *log.Logger) ([]*segment, error) {
	fis, err := listFilesWithSuffix(storagePath, closedSegmentSuffix, true)
	if err != nil {
		return nil, fmt.Errorf("could not list closed segment files: %w", err)
	}

	closedSegments := []*segment{}
	for _, fi := range fis {
		s, err := fromFile(fi.filepath, maxRecordSize, async, log)
		if err != nil {
			return nil, fmt.Errorf("could not load closed segment: %s, %w", fi.filepath, err)
		}
		closedSegments = append(closedSegments, s)
	}

	return closedSegments, nil
}

// Get returns the value associated with the key or a kvdb.NotFoundError if the
// key was not found, or any other error encountered
func (s *Store) Get(key string) ([]byte, error) {
	val, err := s.openSegment.get(key)
	if err == nil {
		return val, nil
	} else if !kvdb.IsNotFoundError(err) {
		return nil, err
	}

	for i := range s.closedSegmentStack {
		val, err := s.closedSegmentStack[i].get(key)
		if err == nil {
			return val, nil
		} else if !kvdb.IsNotFoundError(err) {
			return nil, err
		}
	}

	return nil, kvdb.NewNotFoundError(key)
}

// Put saves the value to the database and returns any error encountered
func (s *Store) Put(key string, value []byte) error {
	record := record.NewValue(key, value)

	if record.Size() > s.maxRecordSize {
		msg := fmt.Sprintf("key-value too big, max size: %d", s.maxRecordSize)
		return kvdb.NewBadRequestError(msg)
	}

	// if len(s.closedSegmentStack) > 3 {
	// 	s.logger.Println("performing compaction")
	// 	s.doCompaction(s.closedSegmentStack[len(s.closedSegmentStack)-2:])
	// }

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
	err := s.openSegment.changeSuffix(openSegmentSuffix, closedSegmentSuffix)
	if err != nil {
		panic(err)
	}

	s.segmentMutex.Lock()
	s.closedSegmentStack = append([]*segment{s.openSegment}, s.closedSegmentStack...)
	s.openSegment = newOpenSegment
	s.segmentMutex.Unlock()
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

func (s *Store) onCompactionDone(targetFilePath string, compactedFilePaths ...string) error {
	newSegment, err := fromFile(targetFilePath, s.maxRecordSize, s.async, s.logger)
	if err != nil {
		return fmt.Errorf("could not create segment of compaction target: %w", err)
	}

	resultingSegments, err := pruneSegments(s.closedSegmentStack, newSegment, compactedFilePaths...)
	if err != nil {
		return fmt.Errorf("could not prune closed segments: %w", err)
	}

	err = newSegment.changeSuffix(".tmp", closedSegmentSuffix)
	if err != nil {
		return fmt.Errorf("could not rename compacted segment: %w", err)
	}

	s.segmentMutex.Lock()
	s.closedSegmentStack = resultingSegments
	s.segmentMutex.Unlock()

	for _, s := range compactedFilePaths[1:] {
		os.Remove(s)
	}

	return nil
}

// // doCompaction takes a set of consecutive segments from the closedSegmentStack
// // and performs a compaction.
// func (s *Store) doCompaction(segments []*segment) error {
// 	s.logger.Printf("compacting: %d segments", len(segments))
// 	var sources = []io.Reader{}

// 	for _, segment := range segments {
// 		file, err := os.Open(segment.storagePath)

// 		if err != nil {
// 			return fmt.Errorf("could not open file for compaction: %w", err)
// 		}

// 		defer func() {
// 			file.Close()
// 		}()

// 		sources = append(sources, file)
// 	}

// 	inflightPath := strings.ReplaceAll(segments[0].storagePath, closedSegmentSuffix, compactionSuffix)
// 	target, err := os.OpenFile(inflightPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
// 	if err != nil {
// 		return fmt.Errorf("could not create compaction target: %w", err)
// 	}
// 	defer target.Close()

// 	err = record.Merge(target, s.maxRecordSize, sources...)
// 	if err != nil {
// 		return err
// 	}

// 	compactedSegment, err := fromFile(inflightPath, s.maxRecordSize, s.async, s.logger)
// 	if err != nil {
// 		return fmt.Errorf("could not create segment of compaction target: %w", err)
// 	}

// 	resultingSegments, err := pruneSegments(s.closedSegmentStack, segments, compactedSegment)
// 	if err != nil {
// 		return fmt.Errorf("could not prune closed segments: %w", err)
// 	}

// 	err = compactedSegment.changeSuffix(".comp", closedSegmentSuffix)
// 	if err != nil {
// 		return fmt.Errorf("could not rename compacted segment: %w", err)
// 	}

// 	s.segmentMutex.Lock()
// 	s.closedSegmentStack = resultingSegments
// 	s.segmentMutex.Unlock()

// 	for _, s := range segments[1:] {
// 		s.clearFile()
// 	}

// 	return nil
// }

// pruneSegments replaces the elements in segmentSet that also exists in
// toRemove with toAdd. It requires the elements in toRemove to be consecutive
// in segmentSet.
//
// Ex: segmentsSet = {4, 3, 2, 1} toRemove = {3, 2} toAdd = 5
// result = pruneSegments(segmentSet, toRemove, toAdd)
// fmt.Println(result) = {4, 5, 1}
func pruneSegments(segmentStack []*segment, newSegment *segment, compactedFiles ...string) ([]*segment, error) {
	resultingSegments := []*segment{}

	if compactedFiles == nil || len(compactedFiles) == 0 {
		return nil, fmt.Errorf("no elements given in toRemove")
	}

	inRemoveStack := false
	hasAdded := false
	for i := range segmentStack {
		for j := range compactedFiles {
			if segmentStack[i].storagePath == compactedFiles[j] {
				inRemoveStack = true
				break
			}
			inRemoveStack = false
		}

		if !inRemoveStack {
			resultingSegments = append(resultingSegments, segmentStack[i])
		} else if !hasAdded {
			resultingSegments = append(resultingSegments, newSegment)
			hasAdded = true
		}
	}

	return resultingSegments, nil
}
