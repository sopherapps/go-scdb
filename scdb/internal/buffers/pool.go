package buffers

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/headers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/values"
	"github.com/sopherapps/go-scdb/scdb/internal/inverted_index"
	"io"
	"math"
	"os"
	"path/filepath"
)

const DefaultPoolCapacity uint64 = 5

// KeyValuePair is a pair of key and value
//
// It is especially useful when searching
type KeyValuePair struct {
	K []byte
	V []byte
}

func (kv KeyValuePair) String() string {
	return fmt.Sprintf("%s: %s", kv.K, kv.V)
}

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
	kvBuffers           []*Buffer // this will act as a FIFO
	indexBuffers        map[uint64]*Buffer
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

	var header *headers.DbFileHeader
	if !dbFileExists {
		header = headers.NewDbFileHeader(maxKeys, redundantBlocks, &bufSize)
		_, err = headers.InitializeFile(file, header)
		if err != nil {
			return nil, err
		}
	} else {
		header, err = headers.ExtractDbFileHeaderFromFile(file)
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
		kvBuffers:           make([]*Buffer, 0, kvCap),
		indexBuffers:        make(map[uint64]*Buffer, indexCap),
		File:                file,
		FilePath:            filePath,
		FileSize:            fileSize,
	}
	return pool, nil
}

// Close closes the buffer pool, freeing up any resources
func (bp *BufferPool) Close() error {
	bp.indexBuffers = nil
	bp.kvBuffers = nil
	return bp.File.Close()
}

// Append appends a given data array to the file attached to this buffer pool
// It returns the address where the data was appended
func (bp *BufferPool) Append(data []byte) (uint64, error) {
	// loop in reverse, starting at the back
	// since the latest kv_buffers are the ones updated when new changes occur
	start := len(bp.kvBuffers) - 1
	for i := start; i >= 0; i-- {
		// make sure you get the pointer
		buf := bp.kvBuffers[i]
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
	err := internal.ValidateBounds(addr, addr+dataLength, headers.HeaderSizeInBytes, bp.keyValuesStartPoint, "data is outside the index bounds")
	if err != nil {
		return err
	}

	blockLeftOffset := bp.getBlockLeftOffset(addr, headers.HeaderSizeInBytes)
	buf, ok := bp.indexBuffers[blockLeftOffset]
	if ok {
		err = buf.Replace(addr, data)
		if err != nil {
			return err
		}
	}

	_, err = bp.File.WriteAt(data, int64(addr))
	return err
}

// ClearFile clears all data on disk and memory making it like a new store
func (bp *BufferPool) ClearFile() error {
	bufSize := uint32(bp.bufferSize)
	header := headers.NewDbFileHeader(&bp.maxKeys, &bp.redundantBlocks, &bufSize)
	fileSize, err := headers.InitializeFile(bp.File, header)
	if err != nil {
		return err
	}
	bp.FileSize = uint64(fileSize)
	bp.indexBuffers = make(map[uint64]*Buffer, bp.indexCapacity)
	bp.kvBuffers = bp.kvBuffers[:0]
	return nil
}

// CompactFile removes any deleted or expired entries from the file. It must first lock the buffer and the file.
// In order to be more efficient, it creates a new file, copying only that data which is not deleted or expired
func (bp *BufferPool) CompactFile(searchIndex *inverted_index.InvertedIndex) error {
	folder := filepath.Dir(bp.FilePath)
	newFilePath := filepath.Join(folder, "tmp__compact.scdb")
	newFile, err := os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	header, err := headers.ExtractDbFileHeaderFromFile(bp.File)
	if err != nil {
		return err
	}

	// Add headers to new file
	_, err = newFile.WriteAt(header.AsBytes(), 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	idxEntrySize := headers.IndexEntrySizeInBytes
	idxEntrySizeAsInt64 := int64(idxEntrySize)
	zero := make([]byte, idxEntrySize)
	zeroStr := string(zero)
	idxOffset := int64(headers.HeaderSizeInBytes)
	newFileOffset := int64(header.KeyValuesStartPoint)

	numOfBlocks := int64(header.NumberOfIndexBlocks)
	blockSize := int64(header.NetBlockSize)

	for i := int64(0); i < numOfBlocks; i++ {
		indexBlock, err := bp.readIndexBlock(i, blockSize)
		if err != nil {
			return err
		}

		// write index block into new file
		_, err = newFile.WriteAt(indexBlock, idxOffset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		idxBlockLength := uint64(len(indexBlock))
		for lwr := uint64(0); lwr < idxBlockLength; lwr += idxEntrySize {
			upr := lwr + idxEntrySize
			idxBytes := indexBlock[lwr:upr]

			if string(idxBytes) != zeroStr {
				kvByteArray, e := getKvByteArray(bp.File, idxBytes)
				if e != nil {
					return e
				}

				kv, e := values.ExtractKeyValueEntryFromByteArray(kvByteArray, 0)
				if e != nil {
					return e
				}

				if !values.IsExpired(kv) && !kv.IsDeleted {
					kvSize := int64(len(kvByteArray))
					// insert key value at the bottom of the new file
					_, er := newFile.WriteAt(kvByteArray, newFileOffset)
					if er != nil && !errors.Is(er, io.EOF) {
						return er
					}

					// update index to have the index of the newly added key-value entry
					newKvAddr := uint64(newFileOffset)
					_, er = newFile.WriteAt(internal.Uint64ToByteArray(newKvAddr), idxOffset)
					if er != nil && !errors.Is(er, io.EOF) {
						return er
					}

					// update search index
					err = searchIndex.Add(kv.Key, newKvAddr, kv.Expiry)
					if err != nil {
						return err
					}

					// increment the new file offset
					newFileOffset += kvSize
				} else {
					// if expired or deleted, update index to zero
					_, er := newFile.WriteAt(zero, idxOffset)
					if er != nil && !errors.Is(er, io.EOF) {
						return er
					}
				}
			}

			// increment the index offset
			idxOffset += idxEntrySizeAsInt64
		}

	}

	// clean up the buffers and update metadata
	bp.kvBuffers = bp.kvBuffers[:0]
	bp.indexBuffers = make(map[uint64]*Buffer, bp.indexCapacity)
	bp.File = newFile
	bp.FileSize = uint64(newFileOffset)

	// Replace old file with new file
	err = os.Remove(bp.FilePath)
	if err != nil {
		return err
	}

	err = os.Rename(newFilePath, bp.FilePath)
	return err
}

// GetValue returns the *entries.KeyValueEntry at the given address if the key there corresponds to the given key
// Otherwise, it returns nil. This is to handle hash collisions.
func (bp *BufferPool) GetValue(kvAddress uint64, key []byte) (*values.KeyValueEntry, error) {
	if kvAddress == 0 {
		return nil, nil
	}

	// loop in reverse, starting at the back
	// since the latest kv_buffers are the ones updated when new changes occur
	kvBufLen := len(bp.kvBuffers)
	for i := kvBufLen - 1; i >= 0; i-- {
		buf := bp.kvBuffers[i]
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

	// update kv_buffers only upto actual data read (cater for partially filled buffer)
	bp.kvBuffers = append(bp.kvBuffers, NewBuffer(kvAddress, buf[:bytesRead], bp.bufferSize))
	entry, err := values.ExtractKeyValueEntryFromByteArray(buf, 0)
	if err != nil {
		return nil, err
	}

	if bytes.Equal(entry.Key, key) && !values.IsExpired(entry) && !entry.IsDeleted {
		return entry, nil
	}

	return nil, nil
}

// TryDeleteKvEntry attempts to delete the key-value entry for the given kv_address as long as the key it holds
// is the same as the key provided. It returns true if successful
func (bp *BufferPool) TryDeleteKvEntry(kvAddress uint64, key []byte) (bool, error) {
	keySize := int64(len(key))
	addrForIsDeleted := int64(kvAddress+values.OffsetForKeyInKVArray) + keySize
	// loop in reverse, starting at the back
	// since the latest kv_buffers are the ones updated when new changes occur
	kvBufLen := len(bp.kvBuffers)
	for i := kvBufLen - 1; i >= 0; i-- {
		buf := bp.kvBuffers[i]
		if buf.Contains(kvAddress) {
			success, err := buf.TryDeleteKvEntry(kvAddress, key)
			if err != nil {
				return false, err
			}

			if success {
				// set isDeleted to true i.e. 1
				_, err = bp.File.WriteAt([]byte{1}, addrForIsDeleted)
				if err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}

	keyInData, err := extractKeyAsByteArrayFromFile(bp.File, kvAddress, keySize)
	if err != nil {
		return false, err
	}

	if bytes.Equal(keyInData, key) {
		// set isDeleted to true i.e. 1
		_, err = bp.File.WriteAt([]byte{1}, addrForIsDeleted)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

// AddrBelongsToKey checks to see if the given kv address is for the given key.
// Note that this returns true for expired keys as long as compaction has not yet been done.
// This avoids duplicate entries for the same key being tracked in separate index entries
//
// It also returns false if the address goes beyond the size of the file
func (bp *BufferPool) AddrBelongsToKey(kvAddress uint64, key []byte) (bool, error) {
	if kvAddress >= bp.FileSize {
		return false, nil
	}

	// loop in reverse, starting at the back
	// since the latest kvBuffers are the ones updated when new changes occur
	kvBufLen := len(bp.kvBuffers)
	for i := kvBufLen - 1; i >= 0; i-- {
		buf := bp.kvBuffers[i]
		if buf.Contains(kvAddress) {
			return buf.AddrBelongsToKey(kvAddress, key)
		}
	}

	if uint64(kvBufLen) >= bp.kvCapacity {
		// Pop front (the oldest entry)
		bp.kvBuffers = bp.kvBuffers[1:]
	}

	buf := make([]byte, bp.bufferSize)
	bytesRead, err := bp.File.ReadAt(buf, int64(kvAddress))
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}

	// update kv_buffers only upto actual data read (cater for partially filled buffer)
	bp.kvBuffers = append(bp.kvBuffers, NewBuffer(kvAddress, buf[:bytesRead], bp.bufferSize))

	keyInFile := buf[values.OffsetForKeyInKVArray : values.OffsetForKeyInKVArray+uint64(len(key))]
	isForKey := bytes.Contains(keyInFile, key)
	return isForKey, nil
}

// ReadIndex reads the index at the given address and returns it
//
// If the address is less than [HEADER_SIZE_IN_BYTES] or [BufferPool.key_values_start_point],
// an ErrOutOfBounds error is returned
func (bp *BufferPool) ReadIndex(addr uint64) ([]byte, error) {
	err := internal.ValidateBounds(addr, addr+headers.IndexEntrySizeInBytes, headers.HeaderSizeInBytes, bp.keyValuesStartPoint, "out of index bounds")
	if err != nil {
		return nil, err
	}

	blockLeftOffset := bp.getBlockLeftOffset(addr, headers.HeaderSizeInBytes)
	buf, ok := bp.indexBuffers[blockLeftOffset]
	if ok {
		return buf.ReadAt(addr, headers.IndexEntrySizeInBytes)
	}

	data := make([]byte, bp.bufferSize)
	// Index buffers should have preset boundaries matching
	// 		StartOfIndex - StartOfIndex + BlockSize,
	//		StartOfIndex + BlockSize - StartOfIndex + (2*BlockSize)
	//		StartOfIndex + (2*BlockSize) - StartOfIndex + (3*BlockSize) ...
	_, err = bp.File.ReadAt(data, int64(blockLeftOffset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if uint64(len(bp.indexBuffers)) >= bp.indexCapacity {
		biggestLeftOffset := uint64(0)
		for lftOffset := range bp.indexBuffers {
			if lftOffset >= biggestLeftOffset {
				biggestLeftOffset = lftOffset
			}
		}

		// delete the buffer with the biggest left offset as those with lower left offsets
		// are expected to have more keys
		delete(bp.indexBuffers, biggestLeftOffset)
		bp.indexBuffers[blockLeftOffset] = NewBuffer(blockLeftOffset, data, bp.bufferSize)
	} else {
		bp.indexBuffers[blockLeftOffset] = NewBuffer(blockLeftOffset, data, bp.bufferSize)
	}

	start := addr - blockLeftOffset
	return data[start : start+headers.IndexEntrySizeInBytes], nil
}

// GetManyKeyValues gets all the key-value pairs that correspond to the given list of key-value addresses
func (bp *BufferPool) GetManyKeyValues(addrs []uint64) ([]KeyValuePair, error) {
	results := make([]KeyValuePair, 0, len(addrs))

	for _, addr := range addrs {
		addr := int64(addr)
		size, err := bp.readKvSize(addr)
		if err != nil {
			return nil, err
		}

		buf, err := bp.readKvBytes(addr, size)
		if err != nil {
			return nil, err
		}

		entry, err := values.ExtractKeyValueEntryFromByteArray(buf, 0)
		if err != nil {
			return nil, err
		}

		if !entry.IsDeleted && !values.IsExpired(entry) {
			results = append(results, KeyValuePair{
				K: entry.Key,
				V: entry.Value,
			})
		}
	}

	return results, nil
}

// readKvBytes reads the key-value byte array directly from file given address and size
func (bp *BufferPool) readKvBytes(addr int64, size uint32) ([]byte, error) {
	buf := make([]byte, size)

	_, err := bp.File.ReadAt(buf, addr)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return buf, nil
}

// readSize reads the size of a key-value entry directly from file
func (bp *BufferPool) readKvSize(addr int64) (uint32, error) {
	buf := make([]byte, 4)

	_, err := bp.File.ReadAt(buf, addr)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	return internal.Uint32FromByteArray(buf)
}

// readIndexBlock returns the next index block
func (bp *BufferPool) readIndexBlock(blockNum int64, blockSize int64) ([]byte, error) {
	buf := make([]byte, blockSize)
	offset := int64(headers.HeaderSizeInBytes) + (blockNum * blockSize)

	bytesRead, err := bp.File.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return buf[:bytesRead], nil
}

// getBlockLeftOffset returns the left offset for the block in which the address is to be found
func (bp *BufferPool) getBlockLeftOffset(addr uint64, minOffset uint64) uint64 {
	blockPosition := (addr - minOffset) / bp.bufferSize
	blockLeftOffset := (blockPosition * bp.bufferSize) + minOffset
	return blockLeftOffset
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
		bp.File.Name() == other.File.Name() &&
		len(bp.kvBuffers) == len(other.kvBuffers) &&
		len(bp.indexBuffers) == len(other.indexBuffers)
	if !isMetaDataEqual {
		return false
	}

	for i, buf := range bp.kvBuffers {
		if !buf.Eq(other.kvBuffers[i]) {
			return false
		}
	}

	for i, buf := range bp.indexBuffers {
		if !buf.Eq(other.indexBuffers[i]) {
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

// extractKeyAsByteArrayFromFile extracts the byte array for the key from a given file
func extractKeyAsByteArrayFromFile(file *os.File, kvAddr uint64, keySize int64) ([]byte, error) {
	offset := int64(kvAddr + values.OffsetForKeyInKVArray)
	buf := make([]byte, keySize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// getKvByteArray reads a byte array for a key-value entry at the given address in the file
func getKvByteArray(file *os.File, addrBytes []byte) ([]byte, error) {
	addrAsUInt64, err := internal.Uint64FromByteArray(addrBytes)
	if err != nil {
		return nil, err
	}
	addr := int64(addrAsUInt64)

	// get size of the whole key value entry
	sizeBytes := make([]byte, 4)
	_, err = file.ReadAt(sizeBytes, addr)
	if err != nil {
		return nil, err
	}

	size, err := internal.Uint32FromByteArray(sizeBytes)
	if err != nil {
		return nil, err
	}

	// get the key value entry itself, basing on the size it has
	data := make([]byte, size)
	_, err = file.ReadAt(data, addr)
	return data, err
}
