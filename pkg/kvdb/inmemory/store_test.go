package inmemory

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	var (
		key = "key"
		val = []byte("value")
	)

	store := NewStore(Config{
		MaxRecordSize: 100,
		Logger:        &log.Logger{},
	})

	err := store.Put(key, val)
	require.NoError(t, err)

	getVal, err := store.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, getVal)

	err = store.Delete(key)
	require.NoError(t, err)

	_, err = store.Get(key)
	require.True(t, store.IsNotFoundError(err))
}

func TestReturnsBadRequestIfValueTooBig(t *testing.T) {
	store := NewStore(Config{
		MaxRecordSize: 1,
		Logger:        &log.Logger{},
	})

	err := store.Put("too", []byte("big"))
	require.True(t, store.IsBadRequestError(err))
}
