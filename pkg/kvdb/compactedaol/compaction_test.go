package compactedaol

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDoCompaction(t *testing.T) {
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
	store.Put("key1", []byte("key1-3"))
	store.rotateOpenSegment()
	store.Put("key4", []byte("value4"))

	var compactedFiles []string
	var targetFile string

	compacter := newCompacter(func(tf string, cf []string) error {
		compactedFiles = cf
		targetFile = tf
		return nil
	}, 1*time.Second, 50, testPath, 40, voidLogger)

	compacter.execute()

	require.Len(t, compactedFiles, 2)
	require.True(t, strings.Contains(targetFile, compactionSuffix))
}
