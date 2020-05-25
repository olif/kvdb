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
	"github.com/olif/kvdb/pkg/kvdb/record"
)

func BenchmarkRead100Db100(b *testing.B)    { Read(b, 100, 100, 100) }
func BenchmarkRead1000Db100(b *testing.B)   { Read(b, 1000, 100, 100) }
func BenchmarkRead10000Db100(b *testing.B)  { Read(b, 10000, 100, 100) }
func BenchmarkRead100000Db100(b *testing.B) { Read(b, 100000, 100, 100) }

func BenchmarkWrite100Db100(b *testing.B)    { Write(b, 100, 100, 100, true) }
func BenchmarkWrite1000Db100(b *testing.B)   { Write(b, 1000, 100, 100, true) }
func BenchmarkWrite10000Db100(b *testing.B)  { Write(b, 10000, 100, 100, true) }
func BenchmarkWrite100000Db100(b *testing.B) { Write(b, 100000, 100, 100, true) }

func BenchmarkWrite1000Async(b *testing.B) { Write(b, 0, 100, 1000, true) }
func BenchmarkWrite1000Sync(b *testing.B)  { Write(b, 0, 100, 1000, false) }

type TestCtx struct {
	dbPath string
	store  kvdb.Store
}

func Setup(b *testing.B, async bool) *TestCtx {
	dbPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		b.Fatal(err)
	}

	maxRecordSize := 100000
	config := aol.Config{
		BasePath:      dbPath,
		MaxRecordSize: &maxRecordSize,
		Async:         &async,
	}
	store, err := aol.NewStore(config)
	if err != nil {
		panic(err)
	}

	return &TestCtx{
		dbPath: dbPath,
		store:  store,
	}
}

func Teardown(ctx *TestCtx) {
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

func Write(b *testing.B, dbSize, valSize, n int, async bool) {
	testCtx := Setup(b, async)
	defer Teardown(testCtx)

	initData := genKeyValues(valSize, dbSize)
	write(b, testCtx.store, initData)

	testData := genKeyValues(valSize, n)
	record := record.NewValue(testData[0].key, testData[0].value)
	s := record.Size()
	b.SetBytes(int64(s))
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

func Read(b *testing.B, dbSize, valSize, n int) {
	ctx := Setup(b, true)
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
