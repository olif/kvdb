package aol

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/stretchr/testify/require"
)

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
