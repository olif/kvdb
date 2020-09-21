package compactedaol

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/record"
)

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

type segment struct {
	storagePath   string
	maxRecordSize int
	logger        *log.Logger
	async         bool
	suffix        string

	index *index
	mutex sync.RWMutex
}

func newSegment(baseDir string, maxRecordSize int, async bool, logger *log.Logger) *segment {
	filename := genFileName(openSegmentSuffix)
	filePath := path.Join(baseDir, filename)

	return &segment{
		storagePath: filePath,
		logger:      logger,
		async:       async,
		index: &index{
			cursor: 0,
			table:  map[string]int64{},
			mutex:  sync.RWMutex{},
		},
		mutex: sync.RWMutex{},
	}
}

func fromFile(filePath string, maxRecordSize int, async bool, logger *log.Logger) (*segment, error) {
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

	return &segment{
		storagePath: filePath,
		index:       &idx,
		mutex:       sync.RWMutex{},
	}, nil
}

func (s *segment) get(key string) (*record.Record, error) {
	s.mutex.RLock()
	offset, ok := s.index.get(key)
	s.mutex.RUnlock()
	if !ok {
		return nil, kvdb.NewNotFoundError(key)
	}

	f, err := s.getFile(os.O_RDONLY)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	scanner, err := record.NewScanner(f, s.maxRecordSize)
	if err != nil {
		return nil, err
	}

	if scanner.Scan() {
		return scanner.Record(), nil
	}

	return nil, kvdb.NewNotFoundError(key)
}

func (s *segment) append(record *record.Record) error {
	file, err := s.getFile(os.O_CREATE | os.O_WRONLY | os.O_APPEND)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("could not open file: %s for write, %w", s.storagePath, err)
	}

	n, err := record.Write(file)
	if err != nil {
		return fmt.Errorf("could not write record to file: %s, %w", s.storagePath, err)
	}

	if !s.async {
		if err := file.Sync(); err != nil {
			return err
		}
	}

	if err := file.Close(); err != nil {
		return err
	}

	s.index.put(record.Key(), int64(n))
	return nil
}

func (s *segment) getFile(mode int) (*os.File, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return os.OpenFile(s.storagePath, mode, 0600)
}

func (s *segment) changeSuffix(oldSuffix, newSuffix string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	newFilePath := strings.Replace(s.storagePath, oldSuffix, newSuffix, 1)

	if err := os.Rename(s.storagePath, newFilePath); err != nil {
		return err
	}

	s.storagePath = newFilePath
	return nil
}

func (s *segment) size() int64 {
	s.index.mutex.RLock()
	defer s.index.mutex.RUnlock()

	return s.index.cursor
}

func (s *segment) clearFile() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.index.table = map[string]int64{}
	s.index.cursor = 0
	return os.Remove(s.storagePath)
}

type segmentStack struct {
	segments []*segment
	mutex    sync.RWMutex
}

func newSegmentStack() *segmentStack {
	segments := make([]*segment, 0)
	return &segmentStack{
		segments: segments,
		mutex:    sync.RWMutex{},
	}
}

func (s *segmentStack) iter() *segmentStackIter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return &segmentStackIter{
		segments: s.segments,
		pos:      -1,
	}
}

func (s *segmentStack) push(seg *segment) {
	s.mutex.Lock()
	s.segments = append([]*segment{seg}, s.segments...)
	s.mutex.Unlock()
}

func (s *segmentStack) remove(predicate func(segment *segment) bool) error {
	rem := []*segment{}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range s.segments {
		if predicate(s.segments[i]) {
			if err := s.segments[i].clearFile(); err != nil {
				return fmt.Errorf("could not remove segment file: %s, due to: %w",
					s.segments[i].storagePath, err)
			}
		} else {
			rem = append(rem, s.segments[i])
		}
	}
	s.segments = rem

	return nil
}

func (s *segmentStack) replace(predicate func(segment *segment) bool, seg *segment) error {
	rem := []*segment{}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := range s.segments {
		if predicate(s.segments[i]) {
			if err := s.segments[i].clearFile(); err != nil {
				return fmt.Errorf("could not remove segment file: %s, due to: %w",
					s.segments[i].storagePath, err)
			}
			rem = append(rem, seg)
		} else {
			rem = append(rem, s.segments[i])
		}
	}
	s.segments = rem

	return nil
}

type segmentStackIter struct {
	segments []*segment
	pos      int
}

func (i *segmentStackIter) hasNext() bool {
	return i.pos < len(i.segments)-1
}

func (i *segmentStackIter) next() *segment {
	i.pos = i.pos + 1
	return i.segments[i.pos]
}

func genFileName(suffix string) string {
	return fmt.Sprintf("%d%s", time.Now().UTC().UnixNano(), suffix)
}
