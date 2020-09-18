package compactedaol

// func TestDoCompaction(t *testing.T) {
// 	testPath, err := ioutil.TempDir("./", "test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	defer (func() {
// 		// os.RemoveAll(testPath)
// 	})()

// 	const (
// 		testKey   = "testkey"
// 		testValue = "testValue"
// 	)

// 	store, err := NewStore(Config{
// 		BasePath: testPath,
// 	})
// 	require.NoError(t, err)

// 	store.Put("key1", []byte("value1"))
// 	store.rotateOpenSegment()
// 	store.Put("key2", []byte("value2"))
// 	store.rotateOpenSegment()
// 	store.Put("key3", []byte("value3"))
// 	store.Put("key1", []byte("key1-3"))
// 	store.rotateOpenSegment()
// 	store.Put("key4", []byte("value4"))

// 	compacter := NewCompacter(testPath, closedSegmentSuffix,
// 		voidLogger, 3, 100000, func(targetFile string, compactedFiles ...string) error {
// 			return nil
// 		})

// 	compacter.doCompaction()

// }
