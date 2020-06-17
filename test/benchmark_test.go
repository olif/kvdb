package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/aol"
	"github.com/olif/kvdb/pkg/kvdb/indexedaol"
	"github.com/olif/kvdb/pkg/kvdb/inmemory"
	"github.com/olif/kvdb/pkg/kvdb/record"
)

const dbPathPattern = "./test"

var maxRecordSize int = 100000

// Aol
func BenchmarkAolReadFrom10Db(b *testing.B) {
	Read(b, getAolStore(b, true), 10, 100, 1)
}
func BenchmarkAolReadFrom100Db(b *testing.B) {
	Read(b, getAolStore(b, true), 100, 100, 1)
}
func BenchmarkAolReadFrom1000Db(b *testing.B) {
	Read(b, getAolStore(b, true), 1000, 100, 1)
}
func BenchmarkAolReadFrom10000Db(b *testing.B) {
	Read(b, getAolStore(b, true), 10000, 100, 1)
}
func BenchmarkAolRead10000Db100(b *testing.B) {
	Read(b, getAolStore(b, true), 10000, 100, 1)
}
func BenchmarkAolWrite100Db100(b *testing.B) {
	Write(b, getAolStore(b, true), 0, 100, 1)
}
func BenchmarkAolWrite1000Db100(b *testing.B) {
	Write(b, getAolStore(b, true), 0, 1000, 1)
}

// Indexed aol
func BenchmarkIndexedAolReadFrom10Db(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 10, 100, 1)
}
func BenchmarkIndexedAolReadFrom100Db(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 100, 100, 1)
}
func BenchmarkIndexedAolReadFrom1000Db(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 1000, 100, 1)
}
func BenchmarkIndexedAolReadFrom10000Db(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 10000, 100, 1)
}
func BenchmarkIndexedAolRead10000Db100(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 10000, 100, 1)
}
func BenchmarkIndexedAolWrite100Db100(b *testing.B) {
	Write(b, getIndexedAolStore(b, true), 0, 100, 1)
}
func BenchmarkIndexedAolWrite1000Db100(b *testing.B) {
	Write(b, getIndexedAolStore(b, true), 0, 1000, 1)
}

func getInMemoryStore(b *testing.B) testCtx {
	dbPath, err := ioutil.TempDir("./", dbPathPattern)
	if err != nil {
		b.Fatal(err)
	}

	store := inmemory.NewStore(inmemory.Config{
		MaxRecordSize: maxRecordSize,
	})

	return testCtx{dbPath, store}

}

func getAolStore(b *testing.B, async bool) testCtx {
	dbPath, err := ioutil.TempDir("./", dbPathPattern)
	if err != nil {
		b.Fatal(err)
	}

	store, err := aol.NewStore(aol.Config{
		BasePath:      dbPath,
		MaxRecordSize: &maxRecordSize,
		Async:         &async,
	})

	if err != nil {
		b.Fatal(err)
	}

	return testCtx{dbPath, store}
}

func getIndexedAolStore(b *testing.B, async bool) testCtx {
	dbPath, err := ioutil.TempDir("./", dbPathPattern)
	if err != nil {
		b.Fatal(err)
	}

	store, err := indexedaol.NewStore(indexedaol.Config{
		BasePath:      dbPath,
		MaxRecordSize: &maxRecordSize,
		Async:         &async,
	})

	if err != nil {
		b.Fatal(err)
	}

	return testCtx{dbPath, store}
}

type testCtx struct {
	dbPath string
	store  kvdb.Store
}

func Teardown(ctx testCtx) {
	os.RemoveAll(ctx.dbPath)
}

type keyVal struct {
	key   string
	value []byte
}

func genKeyValues(size, n int) []keyVal {
	keyValues := []keyVal{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := []byte(strings.Repeat("a", size))
		keyValues = append(keyValues, keyVal{key: key, value: val})
	}

	return keyValues
}

func Write(b *testing.B, testCtx testCtx, dbSize, valSize, n int) {
	defer Teardown(testCtx)

	initData := genKeyValues(valSize, dbSize)
	write(b, testCtx.store, initData)

	testData := genKeyValues(valSize, n)
	record := record.NewValue(testData[0].key, testData[0].value)
	s := record.Size()
	b.SetBytes(int64(n) * int64(s))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			write(b, testCtx.store, testData)
		}
	})
}

func write(b *testing.B, store kvdb.Store, testData []keyVal) {
	for i := range testData {
		err := store.Put(testData[i].key, testData[i].value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Read(b *testing.B, ctx testCtx, dbSize, valSize, n int) {
	defer Teardown(ctx)

	testData := genKeyValues(valSize, dbSize)
	write(b, ctx.store, testData)
	file, err := os.Open(ctx.dbPath)
	if err != nil {
		b.Fatal(err)
	}
	err = file.Sync()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			read(b, ctx.store, testData, n)
		}
	})
	b.StopTimer()
}

func read(b *testing.B, store kvdb.Store, testData []keyVal, n int) {
	for i := 0; i < n; i++ {
		expected := testData[rand.Intn(len(testData))]
		// expected := testData[i%len(testData)]
		v, err := store.Get(expected.key)
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(expected.value, v) {
			b.Fatal("actual value is different from expected")
		}

	}
}
