package buffers

import (
	"bytes"
	"errors"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"github.com/sopherapps/go-scbd/scdb/internal/entries"
	"io"
	"math"
	"os"
)

const DefaultPoolCapacity uint64 = 5

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
	var bufSize uint32
	if bufferSize != nil {
		bufSize = *bufferSize
	} else {
		bufSize = uint32(os.Getpagesize())
	}

	var poolCap uint64
	if capacity != nil {
		poolCap = *capacity
	} else {
		poolCap = DefaultPoolCapacity
	}

	dbFileExists, err := pathExists(filePath)
	if err != nil {
		return nil, err
	}

	fileOpenFlag := os.O_RDWR
	if !dbFileExists {
		fileOpenFlag = fileOpenFlag | os.O_CREATE
	}

	file, err := os.OpenFile(filePath, fileOpenFlag, 0666)
	if err != nil {
		return nil, err
	}

	var header *entries.DbFileHeader
	if !dbFileExists {
		header = entries.NewDbFileHeader(maxKeys, redundantBlocks, &bufSize)
		_, err = initializeDbFile(file, header)
		if err != nil {
			return nil, err
		}
	} else {
		header, err = entries.ExtractDbFileHeaderFromFile(file)
		if err != nil {
			return nil, err
		}
	}

	fileSize, err := getFileSize(file)
	if err != nil {
		return nil, err
	}

	indexCap := getIndexCapacity(header.NumberOfIndexBlocks, poolCap)
	kvCap := poolCap - indexCap

	pool := &BufferPool{
		kvCapacity:          kvCap,
		indexCapacity:       indexCap,
		bufferSize:          uint64(bufSize),
		keyValuesStartPoint: header.KeyValuesStartPoint,
		maxKeys:             header.MaxKeys,
		redundantBlocks:     header.RedundantBlocks,
		kvBuffers:           make([]Buffer, 0, kvCap),
		indexBuffers:        make([]Buffer, 0, indexCap),
		File:                file,
		FilePath:            filePath,
		FileSize:            fileSize,
	}
	return pool, nil
}

// Append appends a given data array to the file attached to this buffer pool
// It returns the address where the data was appended
func (bp *BufferPool) Append(data []byte) (uint64, error) {
	// loop in reverse, starting at the back
	// since the latest kv_buffers are the ones updated when new changes occur
	start := len(bp.kvBuffers) - 1
	for i := start; i >= 0; i-- {
		// make sure you get the pointer
		buf := &bp.kvBuffers[i]
		if buf.CanAppend(bp.FileSize) {
			// write the data to buffer
			addr := buf.Append(data)
			// update the FileSize of this pool
			bp.FileSize = buf.RightOffset
			// write the data to file
			_, err := bp.File.WriteAt(data, int64(addr))
			if err != nil {
				return 0, err
			}

			return addr, err
		}
	}

	addr := bp.FileSize
	_, err := bp.File.WriteAt(data, int64(addr))
	if err != nil {
		return 0, err
	}

	bp.FileSize += uint64(len(data))

	return addr, nil
}

// UpdateIndex updates the index at the given address with the new data.
//
// - This will fail if the data could spill into the key-value entry section or in the header section e.g.
// if the address is less than entries.HEADER_SIZE_IN_BYTES
// or (addr + data length) is greater than or equal BufferPool.keyValuesStartPoint
func (bp *BufferPool) UpdateIndex(addr uint64, data []byte) error {
	dataLength := uint64(len(data))
	err := internal.ValidateBounds(addr, addr+dataLength, entries.HeaderSizeInBytes, bp.keyValuesStartPoint, "data is outside the index bounds")
	if err != nil {
		return err
	}

	for i := 0; i < len(bp.indexBuffers); i++ {
		buf := &bp.indexBuffers[i]
		if buf.Contains(addr) {
			err = buf.Replace(addr, data)
			if err != nil {
				return err
			}
		}
	}

	_, err = bp.File.WriteAt(data, int64(addr))
	return err
}

// ClearFile clears all data on disk and memory making it like a new store
func (bp *BufferPool) ClearFile() error {
	bufSize := uint32(bp.bufferSize)
	header := entries.NewDbFileHeader(&bp.maxKeys, &bp.redundantBlocks, &bufSize)
	fileSize, err := initializeDbFile(bp.File, header)
	if err != nil {
		return err
	}
	bp.FileSize = uint64(fileSize)
	bp.indexBuffers = make([]Buffer, 0, bp.indexCapacity)
	bp.kvBuffers = make([]Buffer, 0, bp.kvCapacity)
	return nil
}

// CompactFile removes any deleted or expired entries from the file. It must first lock the buffer and the file.
// In order to be more efficient, it creates a new file, copying only that data which is not deleted or expired
func (bp *BufferPool) CompactFile() error {
	panic("implement me")
}

// GetValue returns the *Value at the given address if the key there corresponds to the given key
// Otherwise, it returns nil. This is to handle hash collisions.
func (bp *BufferPool) GetValue(kvAddress uint64, key []byte) (*Value, error) {
	if kvAddress == 0 {
		return nil, nil
	}

	// loop in reverse, starting at the back
	// since the latest kv_buffers are the ones updated when new changes occur
	kvBufLen := len(bp.kvBuffers)
	for i := kvBufLen - 1; i >= 0; i-- {
		buf := &bp.kvBuffers[i]
		if buf.Contains(kvAddress) {
			return buf.GetValue(kvAddress, key)
		}
	}

	if uint64(kvBufLen) >= bp.kvCapacity {
		// Pop front (the oldest entry)
		bp.kvBuffers = bp.kvBuffers[1:]
	}

	buf := make([]byte, bp.bufferSize)
	bytesRead, err := bp.File.ReadAt(buf, int64(kvAddress))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	bp.kvBuffers = append(bp.kvBuffers, *NewBuffer(kvAddress, buf[:bytesRead], bp.bufferSize))
	entry, err := entries.ExtractKeyValueEntryFromByteArray(buf, 0)
	if err != nil {
		return nil, err
	}

	if bytes.Equal(entry.Key, key) && !entry.IsExpired() {
		return ExtractValueFromKeyValueEntry(entry), nil
	}

	return nil, nil
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
	isMetaDataEqual := bp.kvCapacity == other.kvCapacity &&
		bp.indexCapacity == other.indexCapacity &&
		bp.keyValuesStartPoint == other.keyValuesStartPoint &&
		bp.bufferSize == other.bufferSize &&
		bp.maxKeys == other.maxKeys &&
		bp.redundantBlocks == other.redundantBlocks &&
		bp.FilePath == other.FilePath &&
		len(bp.kvBuffers) == len(other.kvBuffers) &&
		len(bp.indexBuffers) == len(other.indexBuffers)
	if !isMetaDataEqual {
		return false
	}

	for i, buf := range bp.kvBuffers {
		if !buf.Eq(&other.kvBuffers[i]) {
			return false
		}
	}

	for i, buf := range bp.indexBuffers {
		if !buf.Eq(&other.indexBuffers[i]) {
			return false
		}
	}

	return true
}

// pathExists checks to see if a given path exists
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}

// getFileSize computes the file size of the given file
func getFileSize(file *os.File) (uint64, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(fileStat.Size()), nil
}

// getIndexCapacity computes the capacity (i.e. number of buffers) of the buffers to be set aside for index buffers
// It can't be less than 1 and it can't be more than the number of index blocks available
func getIndexCapacity(numOfIndexBlocks uint64, totalCapacity uint64) uint64 {
	idxCap := math.Floor(2.0 * float64(totalCapacity) / 3.0)
	return uint64(math.Max(1, math.Min(idxCap, float64(numOfIndexBlocks))))
}

// initializeDbFile initializes the database file, giving it the header and the index place holders
// and truncating it. It returns the new file size
func initializeDbFile(file *os.File, header *entries.DbFileHeader) (int64, error) {
	headerBytes := header.AsBytes()
	headerLength := int64(len(headerBytes))
	finalSize := headerLength + int64(header.NumberOfIndexBlocks*header.NetBlockSize)

	err := file.Truncate(finalSize)
	if err != nil {
		return 0, err
	}

	_, err = file.WriteAt(headerBytes, 0)
	if err != nil {
		return 0, err
	}

	return finalSize, nil
}
