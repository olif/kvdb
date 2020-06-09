package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

func BenchmarkInMemoryRead10000Db100(b *testing.B) { Read(b, getInMemoryStore(b), 10000, 100, 100) }
func BenchmarkAolRead10000Db100(b *testing.B)      { Read(b, getAolStore(b, true), 10000, 100, 100) }
func BenchmarkIndexedAolRead10000Db100(b *testing.B) {
	Read(b, getIndexedAolStore(b, true), 10000, 100, 100)
}

func BenchmarkInMemoryWrite10000Db100(b *testing.B) {
	Write(b, getInMemoryStore(b), 0, 100, 10000)
}
func BenchmarkAolWrite10000Db100(b *testing.B) {
	Write(b, getAolStore(b, false), 0, 100, 10000)
}
func BenchmarkIndexedAolWrite10000Db100(b *testing.B) {
	Write(b, getIndexedAolStore(b, false), 0, 100, 10000)
}

// func BenchmarkRead100Db100(b *testing.B)    { Read(b, 100, 100, 100) }
// func BenchmarkRead1000Db100(b *testing.B)   { Read(b, 1000, 100, 100) }
// func BenchmarkRead10000Db100(b *testing.B)  { Read(b, 10000, 100, 100) }
// func BenchmarkRead100000Db100(b *testing.B) { Read(b, 100000, 100, 100) }

// func BenchmarkWrite100Db100(b *testing.B)    { Write(b, 100, 100, 100, true) }
// func BenchmarkWrite1000Db100(b *testing.B)   { Write(b, 1000, 100, 100, true) }
// func BenchmarkWrite10000Db100(b *testing.B)  { Write(b, 10000, 100, 100, true) }
// func BenchmarkWrite100000Db100(b *testing.B) { Write(b, 100000, 100, 100, true) }

// func BenchmarkWrite1000Async(b *testing.B) { Write(b, 0, 100, 1000, true) }
// func BenchmarkWrite1000Sync(b *testing.B)  { Write(b, 0, 100, 1000, false) }

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
		expected := testData[i%len(testData)]
		v, err := store.Get(expected.key)
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(expected.value, v) {
			b.Fatal("actual value is different from expected")
		}

	}
}

func BenchmarkWriteAsync(b *testing.B) { write1Kb(b, false) }
func BenchmarkWriteSync(b *testing.B)  { write1Kb(b, true) }

func write1Kb(b *testing.B, sync bool) {
	data := []byte(strings.Repeat("a", 1024))
	dbPath, err := ioutil.TempDir("./", "test")
	defer os.RemoveAll(dbPath)
	if err != nil {
		b.Fatal(err)
	}

	filePath := fmt.Sprintf("%s/%s", dbPath, "test.db")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	defer file.Close()
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := file.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		if sync {
			file.Sync()
		}
	}
}
