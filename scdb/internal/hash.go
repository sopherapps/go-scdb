package internal

import "github.com/cespare/xxhash/v2"

// GetHash generates the hash of the key for the given block length
// to get the position in the block that the given key corresponds to
func GetHash(key []byte, blockLength uint64) uint64 {
	hash := xxhash.Sum64(key)
	return hash % blockLength
}
