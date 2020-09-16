package record

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	f1 := bytes.NewBuffer([]byte{})
	f2 := bytes.NewBuffer([]byte{})
	f3 := bytes.NewBuffer([]byte{})
	target := bytes.NewBuffer([]byte{})

	record1 := NewValue("key2", []byte("record1"))
	record12 := NewValue("key12", []byte("record1"))
	record13 := NewValue("key13", []byte("record1"))
	record1.Write(f1)
	record12.Write(f1)
	record13.Write(f1)

	record2 := NewValue("key2", []byte("record2"))
	record2.Write(f2)

	record3 := NewValue("key2", []byte("record3"))
	record3.Write(f3)

	Merge(target, 4096, f1, f2, f3)

	targetScanner, _ := NewScanner(target, 4096)

	mergedRecords := []Record{}
	for targetScanner.Scan() {
		record := targetScanner.Record()
		mergedRecords = append(mergedRecords, *record)
	}
	record1.Write(f1)

	require.Len(t, mergedRecords, 3)
	require.Equal(t, mergedRecords[0].Value(), []byte("record3"))
	require.Equal(t, mergedRecords[1].Value(), []byte("record1"))
}
