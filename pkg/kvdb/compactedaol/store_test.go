package compactedaol

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/stretchr/testify/require"
)

const testPath = "./testdata/"

func TestSimpleSetGetDelete(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	const (
		testKey   = "testkey"
		testValue = "testValue"
	)

	store, err := NewStore(Config{
		BasePath: testPath,
	})
	require.NoError(t, err)

	err = store.Put(testKey, []byte(testValue))
	require.NoError(t, err)

	data, err := store.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, []byte(testValue), data)

	err = store.Delete(testKey)
	require.NoError(t, err)

	_, err = store.Get(testKey)
	require.Error(t, err)
	require.True(t, kvdb.IsNotFoundError(err))
}

func TestCanRotateSegments(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	const (
		testKey   = "testkey"
		testValue = "testValue"
	)

	store, err := NewStore(Config{
		BasePath: testPath,
	})
	require.NoError(t, err)

	store.Put("key1", []byte("value1"))
	store.rotateOpenSegment()
	store.Put("key2", []byte("value2"))
	store.rotateOpenSegment()
	store.Put("key3", []byte("value3"))

	val1, err := store.Get("key1")
	require.NoError(t, err)
	val2, err := store.Get("key2")
	require.NoError(t, err)
	val3, err := store.Get("key3")
	require.NoError(t, err)

	require.Equal(t, "value1", string(val1))
	require.Equal(t, "value2", string(val2))
	require.Equal(t, "value3", string(val3))
}

func TestCanRebuildIndex(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	store, err := NewStore(Config{
		BasePath: testPath,
	})
	require.NoError(t, err)

	store.Put("key1", []byte("value1"))
	store.rotateOpenSegment()
	store.Put("key2", []byte("value2"))
	store.rotateOpenSegment()
	store.Put("key3", []byte("value3"))

	store.Close()

	newStore, err := NewStore(Config{
		BasePath: testPath,
	})

	require.NoError(t, err)

	val1, err := newStore.Get("key1")
	require.NoError(t, err)
	val2, err := newStore.Get("key2")
	require.NoError(t, err)
	val3, err := newStore.Get("key3")
	require.NoError(t, err)

	require.Equal(t, "value1", string(val1))
	require.Equal(t, "value2", string(val2))
	require.Equal(t, "value3", string(val3))
}

// func TestCanPruneSegments(t *testing.T) {
// 	s1 := segment{storagePath: "s1.cseg"}
// 	s2 := segment{storagePath: "s2.cseg"}
// 	s3 := segment{storagePath: "s3.cseg"}
// 	s4 := segment{storagePath: "s4.cseg"}
// 	s5 := segment{storagePath: "new.cseg"}

// 	allSegments := []*segment{&s4, &s3, &s2, &s1}

// 	// Remove all
// 	toRemove := []string{"s4.cseg", "s3.cseg", "s2.cseg", "s1.cseg"}
// 	result, err := pruneSegments(allSegments, &s5, toRemove...)
// 	require.NoError(t, err)
// 	require.Len(t, result, 1)

// 	// Remove one
// 	toRemove = []string{"s3.cseg"}
// 	result, err = pruneSegments(allSegments, &s5, toRemove...)
// 	require.Len(t, result, 4)
// 	require.Equal(t, "new.cseg", result[1].storagePath)

// 	// Remove two
// 	toRemove = []string{s3.storagePath, s2.storagePath}
// 	result, err = pruneSegments(allSegments, &s5, toRemove...)
// 	require.Len(t, result, 3)
// 	require.Equal(t, "s4.cseg", result[0].storagePath)
// 	require.Equal(t, "new.cseg", result[1].storagePath)
// 	require.Equal(t, "s1.cseg", result[2].storagePath)

// 	// Remove none
// 	toRemove = []string{}
// 	result, err = pruneSegments(allSegments, &s5, toRemove...)
// 	require.Error(t, err)
// }
