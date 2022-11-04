package buffers

import "os"

const DEFAULT_POOL_CAPACITY uint64 = 5

// BufferPool is a pool of key-value and index Buffer's.
//
// It is possible to have more than one buffer with the same address in a kind of overlap
// In order to avoid corruption, we always update the last kv buffer that has a given address
// since buffers are in FIFO queue. When retrieving a value, we also use the last buffer
// that has a given address
type BufferPool struct {
	kvCapacity          uint64
	indexCapacity       uint64
	bufferSize          uint64
	keyValuesStartPoint uint64
	maxKeys             uint64
	redundantBlocks     uint16
	kvBuffers           []Buffer // this will act as a FIFO
	indexBuffers        []Buffer // this will act as a min-slice where the buffer with the biggest left-offset is replaced
	File                *os.File
	FilePath            string
	FileSize            uint64
}

// NewBufferPool creates a new BufferPool with the given `capacity` number of Buffers and
// for the file at the given path (creating it if necessary)
func NewBufferPool(capacity *uint64, filePath string, maxKeys *uint64, redundantBlocks *uint16, bufferSize *uint32) (*BufferPool, error) {
	panic("implement me")
}

// Append appends a given data array to the file attached to this buffer pool
// It returns the address where the data was appended
func (bp *BufferPool) Append(data []byte) (uint64, error) {
	panic("implement me")
}

// UpdateIndex updates the index at the given address with the new data.
//
// - This will fail if the data could spill into the key-value entry section or in the header section e.g.
// if the address is less than entries.HEADER_SIZE_IN_BYTES
// or (addr + data length) is greater than or equal BufferPool.keyValuesStartPoint
func (bp *BufferPool) UpdateIndex(addr uint64, data []byte) error {
	panic("implement me")
}

// ClearFile clears all data on disk and memory making it like a new store
func (bp *BufferPool) ClearFile() error {
	panic("implement me")
}

// CompactFile removes any deleted or expired entries from the file. It must first lock the buffer and the file.
// In order to be more efficient, it creates a new file, copying only that data which is not deleted or expired
func (bp *BufferPool) CompactFile() error {
	panic("implement me")
}

// GetValue returns the *Value at the given address if the key there corresponds to the given key
// Otherwise, it returns nil. This is to handle hash collisions.
func (bp *BufferPool) GetValue(kvAddress uint64, key []byte) (*Value, error) {
	panic("implement me")
}

// TryDeleteKvEntry attempts to delete the key-value entry for the given kv_address as long as the key it holds
// is the same as the key provided
func (bp *BufferPool) TryDeleteKvEntry(kvAddress uint64, key []byte) (bool, error) {
	panic("implement me")
}

// AddrBelongsToKey checks to see if the given kv address is for the given key.
// Note that this returns true for expired keys as long as compaction has not yet been done.
// This avoids duplicate entries for the same key being tracked in separate index entries
//
// It also returns false if the address goes beyond the size of the file
func (bp *BufferPool) AddrBelongsToKey(kvAddress uint64, key []byte) (bool, error) {
	panic("implement me")
}

// ReadIndex reads the index at the given address and returns it
//
// If the address is less than [HEADER_SIZE_IN_BYTES] or [BufferPool.key_values_start_point],
// an ErrOutOfBounds error is returned
func (bp *BufferPool) ReadIndex(addr uint64) ([]byte, error) {
	panic("implement me")
}

// Eq checks that other is equal to bp
func (bp *BufferPool) Eq(other *BufferPool) bool {
	panic("implement me")
}
