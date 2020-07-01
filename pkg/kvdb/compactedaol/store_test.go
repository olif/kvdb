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

func TestCanRebuildIndex(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		// os.RemoveAll(testPath)
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
