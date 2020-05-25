package record

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	key   = "key1"
	value = "value1"
)

func TestToFromValueKind(t *testing.T) {
	record := NewValue(key, []byte(value))

	data := record.ToBytes()
	r, err := FromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, r.Key())
	require.Equal(t, value, string(r.Value()))
}

func TestToFromTombstoneKind(t *testing.T) {
	key := "key1"
	record := NewTombstone(key)

	data := record.ToBytes()
	r, err := FromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, r.Key())
	require.True(t, r.IsTombstone())
}

func TestFromBytesReturnsErrInsufficientDataIfNotEnoughDataIsProvided(t *testing.T) {
	_, err := FromBytes([]byte{0x00, 0x1})

	require.True(t, errors.Is(err, ErrInsufficientData))
}

func TestFromBytesReturnsErrCorruptDataIfChecksumMismatches(t *testing.T) {
	r := NewValue("test", []byte("record"))
	data := r.ToBytes()

	data[len(data)-1] = data[len(data)-1] << 1

	_, err := FromBytes(data)

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCorruptData))
}

func TestWriteScan(t *testing.T) {
	buff := new(bytes.Buffer)
	r1 := NewValue("key1", []byte("value1"))
	r1.Write(buff)

	r2 := NewValue("key2", []byte("value2"))
	r2.Write(buff)

	reader := bytes.NewReader(buff.Bytes())
	scanner, err := NewScanner(reader, 1024)
	scanner.Scan()
	data := scanner.Record()

	require.NoError(t, err)
	require.Equal(t, r1.key, data.key)
	require.Equal(t, r1.value, data.value)

	scanner.Scan()
	data = scanner.Record()

	require.NoError(t, err)
	require.Equal(t, r2.key, data.key)
	require.Equal(t, r2.value, data.value)

	hasNext := scanner.Scan()
	require.False(t, hasNext)
	scanner.Record()
}
