package values

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var valuesByteArray = []byte{
	/* size: 47u32*/ 0, 0, 0, 47,
	/* index key size: 2u32*/ 0, 0, 0, 2,
	/* key: fo */ 102, 111,
	/* key: foo */ 102, 111, 111,
	/* is_deleted */ 0,
	/* is_root */ 0,
	/* expiry 0u64 */ 0, 0, 0, 0, 0, 0, 0, 0,
	/* next offset 900u64 */ 0, 0, 0, 0, 0, 0, 3, 132,
	/* previous offset 90u64 */ 0, 0, 0, 0, 0, 0, 0, 90,
	/* kv_address: 100u64 */ 0, 0, 0, 0, 0, 0, 0, 100,
}

func TestExtractInvertedIndexEntryFromByteArray(t *testing.T) {
	entry := NewInvertedIndexEntry([]byte("fo"), []byte("foo"), 0, false, 100, 900, 90)

	t.Run("ExtractInvertedIndexEntryFromByteArrayWorksAsExpected", func(t *testing.T) {
		got, err := ExtractInvertedIndexEntryFromByteArray(valuesByteArray, 0)
		if err != nil {
			t.Fatalf("error extracting key value from byte array: %s", err)
		}
		assert.Equal(t, entry, got)
	})

	t.Run("ExtractInvertedIndexEntryFromByteArrayWithOffsetWorksAsExpected", func(t *testing.T) {
		dataArray := internal.ConcatByteArrays([]byte{89, 78}, valuesByteArray)
		got, err := ExtractInvertedIndexEntryFromByteArray(dataArray, 2)
		if err != nil {
			t.Fatalf("error extracting key value from byte array: %s", err)
		}
		assert.Equal(t, entry, got)
	})

	t.Run("ExtractInvertedIndexEntryFromByteArrayWithOutOfBoundsOffsetReturnsErrOutOfBounds", func(t *testing.T) {
		dataArray := internal.ConcatByteArrays([]byte{89, 78}, valuesByteArray)
		_, err := ExtractInvertedIndexEntryFromByteArray(dataArray, 4)
		expectedError := errors.NewErrOutOfBounds(fmt.Sprintf("slice %d - %d out of bounds for maxLength %d for data %v", 12, 157307, len(dataArray), dataArray))
		assert.Equal(t, expectedError, err)
	})
}

func TestInvertedIndexEntry_AsBytes(t *testing.T) {
	entry := NewInvertedIndexEntry([]byte("fo"), []byte("foo"), 0, false, 100, 900, 90)
	assert.Equal(t, valuesByteArray, entry.AsBytes())
}

func TestInvertedIndexEntry_IsExpired(t *testing.T) {
	neverExpires := NewInvertedIndexEntry([]byte("ne"), []byte("never_expires"), 0, false, 100, 900, 90)
	// 1666023836 is some past timestamp in October 2022
	expired := NewInvertedIndexEntry([]byte("exp"), []byte("expires"), 1666023836, false, 100, 900, 90)
	notExpired := NewInvertedIndexEntry([]byte("no"), []byte("not_expired"), uint64(time.Now().Unix())*2, false, 100, 900, 90)

	assert.False(t, IsExpired(neverExpires))
	assert.False(t, IsExpired(notExpired))
	assert.True(t, IsExpired(expired))
}
