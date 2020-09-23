package compactedaol

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/olif/kvdb/pkg/kvdb/record"
	"golang.org/x/sync/semaphore"
)

type onCompactionDoneFunc func(targetFile string, compactedFiles []string) error

type compacter struct {
	onCompactionDone    onCompactionDoneFunc
	compactionThreshold int
	storagePath         string
	maxRecordSize       int
	semaphore           *semaphore.Weighted
	logger              *log.Logger
	ticker              *time.Ticker
}

func newCompacter(
	onCompactionDone onCompactionDoneFunc,
	compactionInterval time.Duration,
	compactionThreshold int,
	storagePath string,
	maxRecordSize int,
	logger *log.Logger) *compacter {

	return &compacter{
		onCompactionDone:    onCompactionDone,
		compactionThreshold: compactionThreshold,
		storagePath:         storagePath,
		maxRecordSize:       maxRecordSize,
		semaphore:           semaphore.NewWeighted(1),
		logger:              logger,
		ticker:              time.NewTicker(compactionInterval),
	}
}

func (c *compacter) start() {
	go (func() {
		for range c.ticker.C {
			go func() {
				c.execute()
			}()
		}
	})()
}

func (c *compacter) execute() {
	if c.semaphore.TryAcquire(1) {
		defer c.semaphore.Release(1)
		filesToCompact := c.selectFilesForCompaction()

		if filesToCompact == nil || len(filesToCompact) == 0 {
			c.logger.Println("compaction: no files to compact")
			return
		}

		targetFile := strings.ReplaceAll(filesToCompact[0], closedSegmentSuffix, compactionSuffix)
		if err := c.doCompaction(targetFile, filesToCompact); err != nil {
			c.logger.Printf("compaction: could not run compaction: %s", err)
			return
		}

		if err := c.onCompactionDone(targetFile, filesToCompact); err != nil {
			c.logger.Printf("compaction: could not run onCompactionDone: %s", err)
		}
	}
}

func (c *compacter) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	c.semaphore.Acquire(ctx, 1)
	c.ticker.Stop()
	c.semaphore.Release(1)
}

// selectFilesForCompaction tries to select as many consecutive files as
// possible without exceeding the CompactionThreshold.
func (c *compacter) selectFilesForCompaction() []string {
	files, _ := listFilesWithSuffix(c.storagePath, closedSegmentSuffix, false)

	fileGroup := []string{}
	var groupSize int64 = 0
	for i := range files {
		fs := files[i].info
		if groupSize+fs.Size() < int64(c.compactionThreshold) {
			groupSize += fs.Size()
			fileGroup = append(fileGroup, files[i].filepath)
		} else {
			if len(fileGroup) > 1 {
				break
			} else {
				fileGroup = []string{files[i].filepath}
				groupSize = fs.Size()
			}
		}
	}

	if len(fileGroup) >= 2 {
		sort.Sort(sort.Reverse(sort.StringSlice(fileGroup)))
		return fileGroup
	}

	return nil
}

func (c *compacter) doCompaction(target string, sources []string) error {
	var sourceScanners = []*record.Scanner{}

	for _, source := range sources {
		file, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("could not open file for compaction: %w", err)
		}
		defer file.Close()

		scanner, err := record.NewScanner(file, c.maxRecordSize)
		if err != nil {
			return err
		}

		sourceScanners = append([]*record.Scanner{scanner}, sourceScanners...)
	}

	data := map[string]*record.Record{}
	for _, source := range sourceScanners {
		for source.Scan() {
			if source.Err() != nil {
				c.logger.Fatalf("compacter: skipping corrupted record for key: %s\n", source.Record().Key())
			} else {
				record := source.Record()
				data[record.Key()] = record
			}
		}
	}

	targetFile, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("could not create compaction target: %w", err)
	}
	defer targetFile.Close()

	for _, record := range data {
		_, err := record.Write(targetFile)
		if err != nil {
			return fmt.Errorf("compacter: could not write to target: %w", err)
		}
	}

	return nil
}
