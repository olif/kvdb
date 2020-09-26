package test

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/compactedaol"
)

const (
	readers     = 20
	writers     = 40
	removers    = 2
	nWrites     = 1000
	nReads      = 1000
	nRemoves    = 1000
	cardinality = 1000
)

var data string = `{
  "extends": [
    "tslint:latest",
    "tslint-config-standard",
    "tslint-react"
  ],
  "rules": {
    "semicolon": [true, "never"],
    "object-literal-sort-keys": false,
    "trailing-comma": [true, {"multiline": "always", "singleline": "never"}],
    "jsx-no-lambda": false,
    "jsx-no-multiline-js": false,
    "quotemark": [true, "single", "jsx-double"],
    "no-implicit-dependencies": [true, "dev"],
    "no-console": [false],
    "max-line-length": [true, 200]
  }
}`

func TestLoad(t *testing.T) {
	dbPath, err := ioutil.TempDir("./", dbPathPattern)
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dbPath)

	logger := log.New(os.Stdout, "", log.LstdFlags)
	maxRecordSize := 5 * 1024
	maxSegmentSize := 20 * 1024
	async := true
	compactionThreshold := 4 * maxSegmentSize
	compactionInterval := 200 * time.Millisecond

	store, err := compactedaol.NewStore(compactedaol.Config{
		Async:               &async,
		Logger:              logger,
		BasePath:            dbPath,
		MaxRecordSize:       &maxRecordSize,
		MaxSegmentSize:      &maxSegmentSize,
		CompactionThreshold: &compactionThreshold,
		CompactionInterval:  &compactionInterval,
	})

	if err != nil {
		t.Fatal(err)
	}

	run(t, store)
}

func run(t *testing.T, store kvdb.Store) {
	wg := sync.WaitGroup{}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(tst *testing.T, wi int, gr *sync.WaitGroup) {
			writer(tst, store)
			gr.Done()
		}(t, i, &wg)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(tst *testing.T, ri int, gr *sync.WaitGroup) {
			reader(tst, store)
			gr.Done()
		}(t, i, &wg)
	}

	for i := 0; i < removers; i++ {
		wg.Add(1)
		go func(tst *testing.T, ri int, gr *sync.WaitGroup) {
			remover(tst, store)
			gr.Done()
		}(t, i, &wg)
	}

	wg.Wait()
	store.Close()
}

func reader(t *testing.T, store kvdb.Store) {
	for i := 0; i < nReads; i++ {
		rnd := rand.Intn(cardinality)
		_, err := store.Get(fmt.Sprintf("key-%d", rnd))
		if err != nil && !kvdb.IsNotFoundError(err) {
			t.Fatal(err)
		}
	}
}

func writer(t *testing.T, store kvdb.Store) {
	for i := 0; i < nWrites; i++ {
		rnd := rand.Intn(cardinality)
		err := store.Put(fmt.Sprintf("key-%d", rnd), []byte(data))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func remover(t *testing.T, store kvdb.Store) {
	for i := 0; i < nRemoves; i++ {
		rnd := rand.Intn(cardinality)
		err := store.Delete(fmt.Sprintf("key-%d", rnd))
		if err != nil {
			t.Fatal(err)
		}
	}
}
