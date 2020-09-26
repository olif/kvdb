package compactedaol

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/olif/kvdb/pkg/kvdb"
	"github.com/olif/kvdb/pkg/kvdb/record"
	"github.com/stretchr/testify/require"
)

func createTestSegment(testPath string) *segment {
	seg := newSegment(testPath, 1000*1024, true, voidLogger)
	seg.append(record.NewValue("test", []byte("test")))
	return seg
}

func TestCreateWriteAndLoadSegment(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	seg := createTestSegment(testPath)
	seg.append(record.NewValue("key1", []byte("value1")))
	seg.append(record.NewValue("key2", []byte("value2")))
	seg.append(record.NewValue("key3", []byte("value3")))
	seg.append(record.NewTombstone("key2"))

	loadedSeg, err := fromFile(seg.storagePath, 1000*1024, true, voidLogger)
	if err != nil {
		t.Fatal(err)
	}

	val1, err := loadedSeg.get("key1")
	require.NoError(t, err)
	require.Equal(t, "value1", string(val1.Value()))

	val2, err := loadedSeg.get("key2")
	require.NoError(t, err)
	require.True(t, val2.IsTombstone())

	val3, err := loadedSeg.get("key3")
	require.NoError(t, err)
	require.Equal(t, "value3", string(val3.Value()))
}

func TestIteratorCanHoldReferenceToRemovedSegments(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	stack := newSegmentStack()

	seg1 := createTestSegment(testPath)
	seg2 := createTestSegment(testPath)
	seg2.append(record.NewValue("key", []byte("val")))
	seg3 := createTestSegment(testPath)

	stack.push(seg1)
	stack.push(seg2)
	stack.push(seg3)

	iterator := stack.iter()

	stack.remove(func(segment *segment) bool {
		return segment == seg2
	})

	counter := 0
	for iterator.hasNext() {
		iterator.next()
		counter++
	}

	require.Equal(t, 3, counter)
	require.Len(t, stack.segments, 2)

	_, err = seg2.get("key")
	require.True(t, kvdb.IsNotFoundError(err))
}

func TestSegmentReplace(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	stack := newSegmentStack()
	seg1 := createTestSegment(testPath)
	seg2 := createTestSegment(testPath)
	seg3 := createTestSegment(testPath)
	rep := createTestSegment(testPath)

	stack.push(seg1)
	stack.push(seg2)
	stack.push(seg3)

	err = stack.replace(func(segment *segment) bool {
		return segment == seg2
	}, rep)

	require.NoError(t, err)
	require.Equal(t, rep.storagePath, stack.segments[1].storagePath)
}

func TestSegmentRemove(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	stack := newSegmentStack()
	seg1 := createTestSegment(testPath)
	seg2 := createTestSegment(testPath)
	seg3 := createTestSegment(testPath)

	stack.push(seg1)
	stack.push(seg2)
	stack.push(seg3)

	stack.remove(func(segment *segment) bool {
		return segment == seg3
	})

	require.Len(t, stack.segments, 2)
	require.Equal(t, seg2, stack.segments[0])
	require.Equal(t, seg1, stack.segments[1])
}
