package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetHash(t *testing.T) {
	var blockSize uint64 = 1289

	t.Run("GetHashGeneratesUniqueValues", func(t *testing.T) {
		keys := [][]byte{[]byte("fooo"), []byte("food"), []byte("bar"), []byte("Bargain"), []byte("Balance"), []byte("Z")}
		hashedMap := map[uint64][]byte{}

		for _, key := range keys {
			hash := GetHash(key, blockSize)
			assert.LessOrEqual(t, hash, blockSize)
			hashedMap[hash] = key
		}

		// if the hashes are truly unique,
		// the length of the map will be the same as that of the vector
		assert.Equal(t, len(hashedMap), len(keys))
	})

	t.Run("GetHashAlwaysGeneratesSameHashForSameKey", func(t *testing.T) {
		key := []byte("fooo")
		expectedHash := GetHash(key, blockSize)

		for i := 0; i < 3; i++ {
			assert.Equal(t, expectedHash, GetHash(key, blockSize))
		}
	})
}
