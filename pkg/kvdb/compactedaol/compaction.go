package compactedaol

import (
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
}

func newCompacter(
	onCompactionDone onCompactionDoneFunc,
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
	}
}

func (c *compacter) run() {
	go (func() {
		for range time.Tick(10 * time.Second) {
			go func() {
				if c.semaphore.TryAcquire(1) {
					defer c.semaphore.Release(1)

					c.logger.Println("compaction: running compaction")

					filesToCompact := c.selectFilesForCompaction()
					if filesToCompact == nil || len(filesToCompact) == 0 {
						c.logger.Println("compaction: no files to compact")
						return
					}

					targetFile := strings.ReplaceAll(filesToCompact[0], closedSegmentSuffix, compactionSuffix)
					err := c.doCompaction(targetFile, filesToCompact)
					if err != nil {
						c.logger.Printf("could not run compaction: %s", err)
						return
					}

					c.onCompactionDone(targetFile, filesToCompact)
				}
			}()
		}
	})()
}

// finds n consecutive segments where filesize is lower than the
// closedSegmentMaxSize
func (c *compacter) selectFilesForCompaction() []string {
	filesToCompact := []string{}
	files, _ := listFilesWithSuffix(c.storagePath, closedSegmentSuffix, false)

	counter := 0
	for i := range files {
		if files[i].info.Size() < int64(c.compactionThreshold) {
			counter++
			filesToCompact = append(filesToCompact, files[i].filepath)
		} else {
			counter = 0
			filesToCompact = []string{}
		}
	}

	if counter >= 2 {
		sort.Sort(sort.Reverse(sort.StringSlice(filesToCompact)))
		return filesToCompact
	}

	return nil
}

func (c *compacter) doCompaction(targetFile string, filesToCompact []string) error {
	var sources = []*record.Scanner{}

	// create scanners for the files to compact
	for _, source := range filesToCompact {
		file, err := os.Open(source)

		if err != nil {
			return fmt.Errorf("could not open file for compaction: %w", err)
		}

		defer func() {
			file.Close()
		}()

		scanner, err := record.NewScanner(file, c.maxRecordSize)
		if err != nil {
			return err
		}

		sources = append(sources, scanner)
	}

	data := map[string][]byte{}
	for _, source := range sources {
		for source.Scan() {
			record := source.Record()
			data[record.Key()] = record.Value()
		}
	}

	c.logger.Printf("compacter: creating new segment: %s\n", targetFile)
	target, err := os.OpenFile(targetFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("could not create compaction target: %w", err)
	}
	defer target.Close()

	for key, val := range data {
		r := record.NewValue(key, val)
		_, err := r.Write(target)
		if err != nil {
			return fmt.Errorf("compacter: could not write to target: %w", err)
		}
	}

	return nil
}
