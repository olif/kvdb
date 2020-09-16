package compactedaol

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/olif/kvdb/pkg/kvdb/record"
	"golang.org/x/sync/semaphore"
)

type onCompactionDoneFunc func(targetFilePath string, compactedFilePaths ...string) error

type NrOfFilesCompacter struct {
	basepath          string
	inFlightSemaphore *semaphore.Weighted
	logger            *log.Logger

	fileSuffix       string
	maxSegmentsFiles int
	maxRecordSize    int

	// onCompactionDone is called when compaction process is finished. It is called
	// with the name of the new sstable together with the files that was compacted.
	// The elements in compactedFilePaths are sorted ascending in the priority order
	// used during the merge process.
	onCompactionDone onCompactionDoneFunc
}

func NewNrOfFilesCompacter(
	basePath string,
	fileSuffix string,
	logger *log.Logger,
	maxSegments int,
	maxRecordSize int,
	onCompactionDone onCompactionDoneFunc) *NrOfFilesCompacter {

	c := NrOfFilesCompacter{
		basepath:          basePath,
		inFlightSemaphore: semaphore.NewWeighted(1),
		logger:            logger,
		fileSuffix:        fileSuffix,
		maxSegmentsFiles:  maxSegments,
		maxRecordSize:     maxRecordSize,
		onCompactionDone:  onCompactionDone,
	}

	go (func() {
		for range time.Tick(1 * time.Second) {
			files, err := listFilesWithSuffix(basePath, fileSuffix, false)
			if err != nil {
				logger.Fatalf("could not list files: %s", err)
			} else if len(files) > maxSegments {
				if !c.inFlightSemaphore.TryAcquire(1) {
					c.doCompaction()
					defer c.inFlightSemaphore.Release(1)
				}
			}
		}
	})()

	return &c
}

func (c *NrOfFilesCompacter) doCompaction() {
	c.logger.Println("running compaction...")
	files, err := c.findFilesToCompact()
	if err != nil {
		c.logger.Fatalf("could not find files to compact: %s", err)
	}

	if len(files) < 2 {
		return
	}

	file1, file2 := files[0], files[1]
	targetPath := changeSuffix(file2, c.fileSuffix, ".tmp")

	if err = mergeRecords(c.maxRecordSize, targetPath, file2, file1); err != nil {
		c.logger.Fatal("could not execute compaction, %w", err)
	}

	if err = c.onCompactionDone(targetPath, file2, file1); err != nil {
		c.logger.Fatal(err)
	}
}

// doCompaction takes a set of consecutive segments from the closedSegmentStack
// and performs a compaction.
func mergeRecords(maxRecordSize int, targetFile string, sourceFiles ...string) error {
	// s.logger.Printf("compacting: %d segments", len(sourceFiles))
	var sources = []io.Reader{}

	for _, source := range sourceFiles {
		file, err := os.Open(source)

		if err != nil {
			return fmt.Errorf("could not open file for compaction: %w", err)
		}

		defer func() {
			file.Close()
		}()

		sources = append(sources, file)
	}

	target, err := os.OpenFile(targetFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("could not create compaction target: %w", err)
	}
	defer target.Close()

	return record.Merge(target, maxRecordSize, sources...)

	// compactedSegment, err := fromFile(targetFile, s.maxRecordSize, s.async, s.logger)
	// if err != nil {
	// 	return fmt.Errorf("could not create segment of compaction target: %w", err)
	// }

	// resultingSegments, err := pruneSegments(s.closedSegmentStack, segments, compactedSegment)
	// if err != nil {
	// 	return fmt.Errorf("could not prune closed segments: %w", err)
	// }

	// err = compactedSegment.changeSuffix(".comp", closedSegmentSuffix)
	// if err != nil {
	// 	return fmt.Errorf("could not rename compacted segment: %w", err)
	// }

	// s.segmentMutex.Lock()
	// s.closedSegmentStack = resultingSegments
	// s.segmentMutex.Unlock()

	// for _, s := range segments[1:] {
	// 	s.clearFile()
	// }

	// return nil
}

func (c *NrOfFilesCompacter) findFilesToCompact() ([]string, error) {
	filepathsToCompact := []string{}
	files, err := listFilesWithSuffix(c.basepath, c.fileSuffix, false)
	if err != nil {
		return nil, fmt.Errorf("could not list sstables, %w", err)
	}

	if len(files) >= 2 {
		filepathsToCompact = append(filepathsToCompact, files[0].filepath)
		filepathsToCompact = append(filepathsToCompact, files[1].filepath)
	}

	return filepathsToCompact, nil
}
