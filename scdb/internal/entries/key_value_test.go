package entries

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var KvDataArray = []byte{
	/* size: 22u32*/ 0, 0, 0, 23,
	/* key size: 3u32*/ 0, 0, 0, 3,
	/* key */ 102, 111, 111,
	/* is_deleted */ 0,
	/* expiry 0u64 */ 0, 0, 0, 0, 0, 0, 0, 0,
	/* value */ 98, 97, 114,
}

func TestExtractKeyValueEntryFromByteArray(t *testing.T) {
	kv := NewKeyValueEntry([]byte("foo"), []byte("bar"), 0)

	t.Run("ExtractKeyValueEntryFromByteArrayWorksAsExpected", func(t *testing.T) {
		got, err := ExtractKeyValueEntryFromByteArray(KvDataArray, 0)
		if err != nil {
			t.Fatalf("error extracting key value from byte array: %s", err)
		}
		assert.Equal(t, kv, got)
	})

	t.Run("ExtractKeyValueEntryFromByteArrayWithOffsetWorksAsExpected", func(t *testing.T) {
		dataArray := internal.ConcatByteArrays([]byte{89, 78}, KvDataArray)
		got, err := ExtractKeyValueEntryFromByteArray(dataArray, 2)
		if err != nil {
			t.Fatalf("error extracting key value from byte array: %s", err)
		}
		assert.Equal(t, kv, got)
	})

	t.Run("ExtractKeyValueEntryFromByteArrayWithOutOfBoundsOffsetReturnsErrOutOfBounds", func(t *testing.T) {
		dataArray := internal.ConcatByteArrays([]byte{89, 78}, KvDataArray)
		_, err := ExtractKeyValueEntryFromByteArray(dataArray, 4)
		expectedError := errors.NewErrOutOfBounds(fmt.Sprintf("slice %d - %d out of bounds for maxLength %d for data %v", 12, 222843, len(dataArray), dataArray))
		assert.Equal(t, expectedError, err)
	})
}

func TestKeyValueEntry_AsBytes(t *testing.T) {
	kv := NewKeyValueEntry([]byte("foo"), []byte("bar"), 0)
	assert.Equal(t, KvDataArray, kv.AsBytes())
}

func TestKeyValueEntry_IsExpired(t *testing.T) {
	neverExpires := NewKeyValueEntry([]byte("never_expires"), []byte("bar"), 0)
	// 1666023836 is some past timestamp in October 2022
	expired := NewKeyValueEntry([]byte("expires"), []byte("bar"), 1666023836)
	notExpired := NewKeyValueEntry([]byte("not_expired"), []byte("bar"), uint64(time.Now().Unix())*2)

	assert.False(t, neverExpires.IsExpired())
	assert.False(t, notExpired.IsExpired())
	assert.True(t, expired.IsExpired())
}
