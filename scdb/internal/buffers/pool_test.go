package buffers

import (
	"bytes"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/entries"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestNewBufferPool(t *testing.T) {
	fileName := "testdb_pool.scdb"
	testCapacity := uint64(60)
	testMaxKeys := uint64(360)
	testRedundantBlocks := uint16(4)
	testBufferSize := uint32(2048)
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("NewBufferPoolForNonExistingFile", func(t *testing.T) {
		type expectedRecord struct {
			bufferSize      uint64
			maxKeys         uint64
			redundantBlocks uint16
			filePath        string
			fileSize        uint64
		}
		type testRecord struct {
			capacity        *uint64
			filePath        string
			maxKeys         *uint64
			redundantBlocks *uint16
			bufferSize      *uint32
			expected        expectedRecord
		}

		testData := []testRecord{
			{nil, fileName, nil, nil, nil, expectedRecord{
				bufferSize:      uint64(os.Getpagesize()),
				maxKeys:         entries.DefaultMaxKeys,
				redundantBlocks: entries.DefaultRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(nil, nil, nil).KeyValuesStartPoint,
			}},
			{&testCapacity, fileName, nil, nil, nil, expectedRecord{
				bufferSize:      uint64(os.Getpagesize()),
				maxKeys:         entries.DefaultMaxKeys,
				redundantBlocks: entries.DefaultRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(nil, nil, nil).KeyValuesStartPoint,
			}},
			{nil, fileName, nil, nil, nil, expectedRecord{
				bufferSize:      uint64(os.Getpagesize()),
				maxKeys:         entries.DefaultMaxKeys,
				redundantBlocks: entries.DefaultRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(nil, nil, nil).KeyValuesStartPoint,
			}},
			{nil, fileName, &testMaxKeys, nil, nil, expectedRecord{
				bufferSize:      uint64(os.Getpagesize()),
				maxKeys:         testMaxKeys,
				redundantBlocks: entries.DefaultRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(&testMaxKeys, nil, nil).KeyValuesStartPoint,
			}},
			{nil, fileName, nil, &testRedundantBlocks, nil, expectedRecord{
				bufferSize:      uint64(os.Getpagesize()),
				maxKeys:         entries.DefaultMaxKeys,
				redundantBlocks: testRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(nil, &testRedundantBlocks, nil).KeyValuesStartPoint,
			}},
			{nil, fileName, nil, nil, &testBufferSize, expectedRecord{
				bufferSize:      uint64(testBufferSize),
				maxKeys:         entries.DefaultMaxKeys,
				redundantBlocks: entries.DefaultRedundantBlocks,
				filePath:        fileName,
				fileSize:        entries.NewDbFileHeader(nil, nil, &testBufferSize).KeyValuesStartPoint,
			}},
		}

		// delete the file so that BufferPool::new() can reinitialize it.
		_ = os.Remove(fileName)

		for _, record := range testData {
			got, err := NewBufferPool(record.capacity, record.filePath, record.maxKeys, record.redundantBlocks, record.bufferSize)
			if err != nil {
				t.Fatalf("error creating new buffer pool: %s", err)
			}

			assert.Equal(t, record.expected.bufferSize, got.bufferSize)
			assert.Equal(t, record.expected.maxKeys, got.maxKeys)
			assert.Equal(t, record.expected.redundantBlocks, got.redundantBlocks)
			assert.Equal(t, record.expected.filePath, got.FilePath)
			assert.Equal(t, record.expected.fileSize, got.FileSize)

			err = os.Remove(got.FilePath)
			if err != nil {
				t.Fatalf("error removing database file: %s", got.FilePath)
			}
		}
	})

	t.Run("NewBufferPoolForExistingFile", func(t *testing.T) {
		type testRecord struct {
			capacity        *uint64
			filePath        string
			maxKeys         *uint64
			redundantBlocks *uint16
			bufferSize      *uint32
		}

		testData := []testRecord{
			{nil, fileName, nil, nil, nil},
			{&testCapacity, fileName, nil, nil, nil},
			{nil, fileName, nil, nil, nil},
			{nil, fileName, &testMaxKeys, nil, nil},
			{nil, fileName, nil, &testRedundantBlocks, nil},
			{nil, fileName, nil, nil, &testBufferSize},
		}

		for _, record := range testData {
			first, err := NewBufferPool(record.capacity, record.filePath, record.maxKeys, record.redundantBlocks, record.bufferSize)
			if err != nil {
				t.Fatalf("error creating new buffer pool: %s", err)
			}

			second, err := NewBufferPool(record.capacity, record.filePath, record.maxKeys, record.redundantBlocks, record.bufferSize)
			if err != nil {
				t.Fatalf("error creating new buffer pool: %s", err)
			}

			assert.True(t, first.Eq(second))

			err = os.Remove(first.FilePath)
			if err != nil {
				t.Fatalf("error removing database file: %s", first.FilePath)
			}
		}
	})
}

func TestBufferPool_Close(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating new buffer pool: %s", err)
	}

	err = pool.Close()
	if err != nil {
		t.Fatalf("error closing pool: %s", err)
	}

	assert.Nil(t, pool.kvBuffers)
	assert.Nil(t, pool.indexBuffers)
	// Close has already been called on File
	assert.NotNil(t, pool.File.Close())
}

func TestBufferPool_Append(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("BufferPool.AppendForNonExistingBufferAppendsToFileDirectly", func(t *testing.T) {
		data := []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}
		dataLength := uint64(len(data))

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating buffer pool: %s", err)
		}
		initialFileSize := pool.FileSize

		_, err = pool.Append(data)
		if err != nil {
			t.Fatalf("error appending data to pool: %s", err)
		}

		finalFileSize := pool.FileSize
		dataInFile, bytesRead := readFromFile(t, fileName, int64(initialFileSize), dataLength)
		actualFileSize := getActualFileSize(t, fileName)

		assert.Equal(t, initialFileSize+dataLength, finalFileSize)
		assert.Equal(t, finalFileSize, actualFileSize)
		assert.Equal(t, dataLength, bytesRead)
		assert.Equal(t, data, dataInFile)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool.AppendForExistingBufferAppendsToBothFileAndBuffer", func(t *testing.T) {
		initialData := []byte{76, 67, 56}
		initialDataLength := uint64(len(initialData))
		data := []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}
		dataLength := uint64(len(data))

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating buffer pool: %s", err)
		}

		initialOffset := pool.FileSize
		writeToFile(t, fileName, int64(initialOffset), initialData)
		pool.FileSize += initialDataLength
		headerArray, _ := readFromFile(t, fileName, 0, 100)
		initialFileSize := pool.FileSize

		appendKvBuffer(pool, initialOffset, initialData)
		appendKvBuffer(pool, 0, headerArray)

		_, err = pool.Append(data)
		if err != nil {
			t.Fatalf("error appending to pool: %s", err)
		}

		dataInFile, bytesRead := readFromFile(t, fileName, int64(initialOffset+initialDataLength), dataLength)
		actualFileSize := getActualFileSize(t, fileName)
		finalFileSize := pool.FileSize
		firstBuf := pool.kvBuffers[0]

		// assert things in file
		assert.Equal(t, finalFileSize, initialFileSize+dataLength)
		assert.Equal(t, finalFileSize, actualFileSize)
		assert.Equal(t, bytesRead, dataLength)
		assert.Equal(t, dataInFile, data)

		// assert things in buffer
		assert.Equal(t, firstBuf.RightOffset, finalFileSize)
		assert.Equal(t, firstBuf.Data, internal.ConcatByteArrays(initialData, data))

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})
}

func TestBufferPool_UpdateIndex(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("BufferPool_UpdateIndexForNoneExistingBufferUpdatesFileDirectly", func(t *testing.T) {
		oldIndex := uint64(890)
		newIndex := uint64(6987)

		data := internal.Uint64ToByteArray(oldIndex)
		dataLength := uint64(len(data))
		newData := internal.Uint64ToByteArray(newIndex)
		newDataLength := uint64(len(newData))
		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		offset := entries.HeaderSizeInBytes + 5
		initialFileSize := pool.FileSize
		writeToFile(t, fileName, int64(offset), data)

		err = pool.UpdateIndex(offset, newData)
		if err != nil {
			t.Fatalf("error updating index: %s", err)
		}

		finalFileSize := pool.FileSize
		dataInFile, bytesRead := readFromFile(t, fileName, int64(offset), newDataLength)
		actualFileSize := getActualFileSize(t, fileName)

		assert.Equal(t, finalFileSize, initialFileSize)
		assert.Equal(t, finalFileSize, actualFileSize)
		assert.Equal(t, bytesRead, dataLength)
		assert.Equal(t, dataInFile, newData)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_UpdateIndexForExistingBufferUpdatesBothFileAndBuffer", func(t *testing.T) {
		oldIndex := uint64(890)
		newIndex := uint64(6987)

		initialData := internal.Uint64ToByteArray(oldIndex)
		newData := internal.Uint64ToByteArray(newIndex)
		newDataLength := uint64(len(newData))

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}

		initialOffset := entries.HeaderSizeInBytes
		initialFileSize := pool.FileSize

		writeToFile(t, fileName, int64(initialOffset), initialData)
		appendIndexBuffer(pool, initialOffset, initialData)

		err = pool.UpdateIndex(initialOffset, newData)
		if err != nil {
			t.Fatalf("error updating index: %s", err)
		}

		dataInFile, bytesRead := readFromFile(t, fileName, int64(initialOffset), newDataLength)
		actualFileSize := getActualFileSize(t, fileName)
		finalFileSize := pool.FileSize
		buf := pool.indexBuffers[initialOffset]

		// assert things in file
		assert.Equal(t, initialFileSize, finalFileSize)
		assert.Equal(t, finalFileSize, actualFileSize)
		assert.Equal(t, newDataLength, bytesRead)
		assert.Equal(t, newData, dataInFile)

		// assert things in buffer
		assert.Equal(t, buf.RightOffset, initialOffset+newDataLength)
		assert.Equal(t, buf.Data, newData)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_UpdateIndexForOutOfBoundsIndexReturnsAnError", func(t *testing.T) {
		oldIndex := uint64(890)
		newIndex := uint64(6783)
		initialData := internal.Uint64ToByteArray(oldIndex)
		newData := internal.Uint64ToByteArray(newIndex)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}

		appendIndexBuffer(pool, entries.HeaderSizeInBytes+2, initialData)
		addresses := []uint64{
			pool.keyValuesStartPoint + 3,
			pool.keyValuesStartPoint + 50,
			entries.HeaderSizeInBytes - 6,
		}

		for _, addr := range addresses {
			err = pool.UpdateIndex(addr, newData)
			assert.NotNil(t, err)
		}

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})
}

func TestBufferPool_ClearFile(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	initialData := []byte{76, 67, 56}
	initialDataLength := uint64(len(initialData))

	pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating new buffer pool: %s", err)
	}

	expected, err := NewBufferPool(nil, fileName, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating new buffer pool: %s", err)
	}

	initialOffset := getActualFileSize(t, fileName)
	writeToFile(t, fileName, int64(initialOffset), initialData)
	pool.FileSize += initialDataLength
	headerArray, _ := readFromFile(t, fileName, 0, 100)

	appendKvBuffer(pool, initialOffset, initialData)
	appendKvBuffer(pool, 0, headerArray)

	headerPreClear, err := entries.ExtractDbFileHeaderFromFile(pool.File)
	if err != nil {
		t.Fatalf("error extracting header from pool file: %s", err)
	}

	kv1 := entries.NewKeyValueEntry([]byte("kv"), []byte("bar"), 0)
	kv2 := entries.NewKeyValueEntry([]byte("foo"), []byte("baracuda"), uint64(time.Now().Unix()*2))

	insertKeyValueEntry(t, pool, headerPreClear, kv1)
	insertKeyValueEntry(t, pool, headerPreClear, kv2)

	err = pool.ClearFile()
	if err != nil {
		t.Fatalf("error clearing file: %s", err)
	}
	finalFileSize := getActualFileSize(t, fileName)

	header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
	if err != nil {
		t.Fatalf("error extracting header from pool file: %s", err)
	}

	// the index should all be reset to zero
	blockSize := int64(header.NetBlockSize)
	numOfBlocks := int64(header.NumberOfIndexBlocks)
	zeroStr := string(make([]byte, blockSize))
	for i := int64(0); i < numOfBlocks; i++ {
		indexBlock, err := pool.readIndexBlock(i, blockSize)
		if err != nil {
			t.Fatalf("error reading index: %s", err)
		}

		assert.Equal(t, string(indexBlock), zeroStr)
	}

	// the metadata of the pool should be reset
	assert.True(t, pool.Eq(expected))
	// the file should have gone back to its original file size
	assert.Equal(t, initialOffset, finalFileSize)
}

func TestBufferPool_CompactFile(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	// pre-clean up for right results
	_ = os.Remove(fileName)

	futureTimestamp := uint64(time.Now().Unix() * 2)
	neverExpires := entries.NewKeyValueEntry([]byte("never_expires"), []byte("bar"), 0)
	deleted := entries.NewKeyValueEntry([]byte("deleted"), []byte("bok"), 0)
	// 1666023836u64 is some past timestamp in October 2022
	expired := entries.NewKeyValueEntry([]byte("expired"), []byte("bar"), 1666023836)
	notExpired := entries.NewKeyValueEntry([]byte("not_expired"), []byte("bar"), futureTimestamp)

	// Limit the max_keys to 10 otherwise the memory will be consumed when we try to get all data in file
	maxKeys := uint64(10)
	pool, err := NewBufferPool(nil, fileName, &maxKeys, nil, nil)
	if err != nil {
		t.Fatalf("error creating new buffer pool: %s", err)
	}

	appendKvBuffer(pool, 0, []byte{76, 79})

	header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
	if err != nil {
		t.Fatalf("error extracting header from file: %s", err)
	}

	// insert key value pairs in pool
	insertKeyValueEntry(t, pool, header, neverExpires)
	insertKeyValueEntry(t, pool, header, deleted)
	insertKeyValueEntry(t, pool, header, expired)
	insertKeyValueEntry(t, pool, header, notExpired)

	// delete the key-value to be deleted
	deleteKeyValue(t, pool, header, deleted)

	initialFileSize := getActualFileSize(t, fileName)

	err = pool.CompactFile()
	if err != nil {
		t.Fatalf("error compacting db file: %s", err)
	}

	finalFileSize := getActualFileSize(t, fileName)
	dataInFile, _ := readFromFile(t, fileName, 0, finalFileSize)
	poolFileSize := pool.FileSize

	bufferLen := len(pool.kvBuffers)

	expectedFileSizeReduction := uint64(deleted.Size + expired.Size)
	expiredKvAddr := getKvAddress(t, pool, header, expired)
	deletedKvAddr := getKvAddress(t, pool, header, deleted)

	assert.Equal(t, bufferLen, 0)
	assert.Equal(t, poolFileSize, finalFileSize)
	assert.Equal(t, initialFileSize-finalFileSize, expectedFileSizeReduction)
	assert.Equal(t, expiredKvAddr, uint64(0))
	assert.Equal(t, deletedKvAddr, uint64(0))

	assert.True(t, keyValueExists(t, dataInFile, header, neverExpires))
	assert.True(t, keyValueExists(t, dataInFile, header, notExpired))
	assert.False(t, keyValueExists(t, dataInFile, header, expired))
	assert.False(t, keyValueExists(t, dataInFile, header, deleted))
}

func TestBufferPool_GetValue(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("BufferPool_GetValueForNonExistingBufferGetsValueFromFileDirectly", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("kv"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)

		kvAddress := getKvAddress(t, pool, header, kv)
		got, err := pool.GetValue(kvAddress, kv.Key)
		if err != nil {
			t.Fatalf("error getting value: %s", err)
		}

		assert.Equal(t, kv, got)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_GetValueFromExistingBufferGetsValueFromBuffer", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("kv"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)
		kvAddress := getKvAddress(t, pool, header, kv)
		_, err = pool.GetValue(kvAddress, kv.Key)
		if err != nil {
			t.Fatalf("error getting value: %s", err)
		}

		// delete underlying file first
		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing db file: %s", err)
		}

		// the second get must be getting value from memory
		got, err := pool.GetValue(kvAddress, kv.Key)
		if err != nil {
			t.Fatalf("error getting value: %s", err)
		}

		assert.Equal(t, kv, got)
	})

	t.Run("BufferPool_GetValueForExpiredValueReturnsNil", func(t *testing.T) {
		// 1666023836u64 is some past timestamp in October 2022 so this is expired
		kv := entries.NewKeyValueEntry([]byte("expires"), []byte("bar"), 1666023836)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)
		kvAddress := getKvAddress(t, pool, header, kv)

		got, err := pool.GetValue(kvAddress, kv.Key)
		if err != nil {
			t.Fatalf("error getting value: %s", err)
		}

		assert.Nil(t, got)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_GetValueForDeletedValueReturnsNil", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("deleted"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)
		deleteKeyValue(t, pool, header, kv)

		kvAddress := getKvAddress(t, pool, header, kv)

		got, err := pool.GetValue(kvAddress, kv.Key)
		if err != nil {
			t.Fatalf("error getting value: %s", err)
		}

		assert.Equal(t, kvAddress, uint64(0))
		assert.Nil(t, got)
		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})
}

func TestBufferPool_AddrBelongsToKey(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("BufferPool_AddrBelongsToKeyChecksIfKeyValueEntryAtGivenAddressHasGivenKey", func(t *testing.T) {
		kv1 := entries.NewKeyValueEntry([]byte("never"), []byte("bar"), 0)
		kv2 := entries.NewKeyValueEntry([]byte("foo"), []byte("baracuda"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv1)
		insertKeyValueEntry(t, pool, header, kv2)

		kv1Addr := getKvAddress(t, pool, header, kv1)
		kv2Addr := getKvAddress(t, pool, header, kv2)

		isKv1AddrForKv1, err := pool.AddrBelongsToKey(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}
		isKv2AddrForKv2, err := pool.AddrBelongsToKey(kv2Addr, kv2.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}
		isKv1AddrForKv2, err := pool.AddrBelongsToKey(kv1Addr, kv2.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}
		isKv2AddrForKv1, err := pool.AddrBelongsToKey(kv2Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}

		assert.True(t, isKv1AddrForKv1)
		assert.True(t, isKv2AddrForKv2)
		assert.False(t, isKv1AddrForKv2)
		assert.False(t, isKv2AddrForKv1)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_AddrBelongsToKeyForAnExpiredKeyReturnsTrue", func(t *testing.T) {
		// 1666023836 is some past timestamp in October 2022 so this is expired
		kv := entries.NewKeyValueEntry([]byte("expires"), []byte("bar"), 1666023836)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)
		kvAddr := getKvAddress(t, pool, header, kv)

		isKvAddrForKv, err := pool.AddrBelongsToKey(kvAddr, kv.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}

		assert.True(t, isKvAddrForKv)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_AddrBelongsToKeyForOutOfBoundsAddressReturnsFalse", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("foo"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)
		outOfBoundsAddr := getActualFileSize(t, fileName)

		isOutOfBoundsAddrForKv, err := pool.AddrBelongsToKey(outOfBoundsAddr, kv.Key)
		if err != nil {
			t.Fatalf("error calling BufferPool.AddrBelongsToKey: %s", err)
		}

		assert.False(t, isOutOfBoundsAddrForKv)
		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})
}

func TestBufferPool_TryDeleteKvEntry(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	kv1 := entries.NewKeyValueEntry([]byte("never"), []byte("bar"), 0)
	kv2 := entries.NewKeyValueEntry([]byte("foo"), []byte("baracuda"), 0)

	pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating new buffer pool: %s", err)
	}
	header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
	if err != nil {
		t.Fatalf("error extracting db file header from file: %s", err)
	}

	t.Run("BufferPool_TryDeleteKvEntryFromBufferDoesJustThat", func(t *testing.T) {
		insertKeyValueEntry(t, pool, header, kv1)
		insertKeyValueEntry(t, pool, header, kv2)

		kv1Addr := getKvAddress(t, pool, header, kv1)

		isDeletedForKv1AddrAndKv2Key, err := pool.TryDeleteKvEntry(kv1Addr, kv2.Key)
		if err != nil {
			t.Fatalf("error trying to delete kv1 with kv2 key: %s", err)
		}

		kv1ValuePostFailedDelete, err := pool.GetValue(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error getting value for kv1: %s", err)
		}

		isDeletedForKv1AddrAndKv1Key, err := pool.TryDeleteKvEntry(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error trying to delete kv1 with kv1 key: %s", err)
		}

		kv1ValuePostSuccessfulDelete, err := pool.GetValue(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error getting value for kv1: %s", err)
		}

		assert.False(t, isDeletedForKv1AddrAndKv2Key)
		assert.Equal(t, kv1, kv1ValuePostFailedDelete)
		assert.True(t, isDeletedForKv1AddrAndKv1Key)
		assert.Nil(t, kv1ValuePostSuccessfulDelete)
	})

	t.Run("BufferPool_TryDeleteKvEtnryFromFileDoesJustThat", func(t *testing.T) {
		insertKeyValueEntry(t, pool, header, kv1)
		insertKeyValueEntry(t, pool, header, kv2)

		// clear the buffers
		pool.kvBuffers = pool.kvBuffers[:0]
		pool.indexBuffers = make(map[uint64]*Buffer, pool.indexCapacity)

		kv1Addr := getKvAddress(t, pool, header, kv1)

		isDeletedForKv1AddrAndKv2Key, err := pool.TryDeleteKvEntry(kv1Addr, kv2.Key)
		if err != nil {
			t.Fatalf("error trying to delete kv1 with kv2 key: %s", err)
		}

		kv1ValuePostFailedDelete, err := pool.GetValue(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error getting value for kv1: %s", err)
		}

		// clear the buffers
		pool.kvBuffers = pool.kvBuffers[:0]
		pool.indexBuffers = make(map[uint64]*Buffer, pool.indexCapacity)

		isDeletedForKv1AddrAndKv1Key, err := pool.TryDeleteKvEntry(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error trying to delete kv1 with kv1 key: %s", err)
		}

		kv1ValuePostSuccessfulDelete, err := pool.GetValue(kv1Addr, kv1.Key)
		if err != nil {
			t.Fatalf("error getting value for kv1: %s", err)
		}

		assert.False(t, isDeletedForKv1AddrAndKv2Key)
		assert.Equal(t, kv1, kv1ValuePostFailedDelete)
		assert.True(t, isDeletedForKv1AddrAndKv1Key)
		assert.Nil(t, kv1ValuePostSuccessfulDelete)
	})
}

func TestBufferPool_ReadIndex(t *testing.T) {
	fileName := "testdb_pool.scdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("BufferPool_ReadIndexReadsIndexAtGivenAddressIfAddressIsWithinTheIndexBands", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("kv"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)

		indexAddr := header.GetIndexOffset(kv.Key)
		kvAddr := getKvAddress(t, pool, header, kv)

		got, err := pool.ReadIndex(indexAddr)
		if err != nil {
			t.Fatalf("error reading index: %s", err)
		}
		expected := internal.Uint64ToByteArray(kvAddr)

		assert.Equal(t, expected, got)

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})

	t.Run("BufferPool_ReadIndexReturnsErrorIfGivenAddressIsOutsideTheIndexBands", func(t *testing.T) {
		kv := entries.NewKeyValueEntry([]byte("kv"), []byte("bar"), 0)

		pool, err := NewBufferPool(nil, fileName, nil, nil, nil)
		if err != nil {
			t.Fatalf("error creating new buffer pool: %s", err)
		}
		header, err := entries.ExtractDbFileHeaderFromFile(pool.File)
		if err != nil {
			t.Fatalf("error extracting db file header from file: %s", err)
		}

		insertKeyValueEntry(t, pool, header, kv)

		kvAddr := getKvAddress(t, pool, header, kv)
		valueAddr := kvAddr + uint64(entries.KeyValueMinSizeInBytes) + uint64(kv.KeySize)
		fileSize := getActualFileSize(t, fileName)

		testData := []uint64{kvAddr, valueAddr, fileSize}

		for _, addr := range testData {
			v, e := pool.ReadIndex(addr)
			assert.Nil(t, v)
			assert.NotNil(t, e)
		}

		err = os.Remove(fileName)
		if err != nil {
			t.Fatalf("error removing database file: %s", fileName)
		}
	})
}

// readFromFile reads from the file at the given file path at the given offset returning the number of bytes read
// and the data itself
func readFromFile(t *testing.T, filePath string, addr int64, bufSize uint64) ([]byte, uint64) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		t.Fatalf("error opening file: %s", err)
	}
	defer func() {
		_ = file.Close()
	}()

	buf := make([]byte, bufSize)
	n, err := file.ReadAt(buf, addr)
	if err != nil {
		t.Fatalf("error reading from file: %s", err)
	}

	return buf, uint64(n)
}

// getActualFileSize returns the actual file size of the file at the given path
func getActualFileSize(t *testing.T, filePath string) uint64 {
	stats, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("error getting file size: %s", err)
	}

	return uint64(stats.Size())
}

// appendKvBuffer creates a new Buffer with the given leftOffset and appends it to the pool's kvBuffers
func appendKvBuffer(pool *BufferPool, leftOffset uint64, data []byte) {
	pool.kvBuffers = append(pool.kvBuffers, NewBuffer(leftOffset, data, pool.bufferSize))
}

// appendIndexBuffer creates a new Buffer with the given leftOffset and appends it to the pool's indexBuffers
func appendIndexBuffer(pool *BufferPool, leftOffset uint64, data []byte) {
	pool.indexBuffers[leftOffset] = NewBuffer(leftOffset, data, pool.bufferSize)
}

// writeToFile writes the given data to the file at the given offset
func writeToFile(t *testing.T, filePath string, offset int64, data []byte) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("error opening file: %s", err)
	}
	defer func() {
		_ = file.Close()
	}()

	_, err = file.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("error writing to file: %s", err)
	}
}

// insertKeyValueEntry inserts a key value entry into the pool, updating the index also
func insertKeyValueEntry(t *testing.T, pool *BufferPool, header *entries.DbFileHeader, kv *entries.KeyValueEntry) {
	idxAddr := header.GetIndexOffset(kv.Key)
	kvAddr, err := pool.Append(kv.AsBytes())
	if err != nil {
		t.Fatalf("error appending kv: %s", err)
	}

	err = pool.UpdateIndex(idxAddr, internal.Uint64ToByteArray(kvAddr))
	if err != nil {
		t.Fatalf("error updating kv's index: %s", err)
	}
}

// getKvAddress returns the address for the given key value entry within the buffer pool
func getKvAddress(t *testing.T, pool *BufferPool, header *entries.DbFileHeader, kv *entries.KeyValueEntry) uint64 {
	kvAddr := make([]byte, entries.IndexEntrySizeInBytes)
	indexAddr := int64(header.GetIndexOffset(kv.Key))

	_, err := pool.File.ReadAt(kvAddr, indexAddr)
	if err != nil {
		t.Fatalf("error reading file: %s", err)
	}

	v, err := internal.Uint64FromByteArray(kvAddr)
	if err != nil {
		t.Fatalf("error converting byte array to uint64: %s", err)
	}

	return v
}

// deleteKeyValue deletes a given key value in the given pool
func deleteKeyValue(t *testing.T, pool *BufferPool, header *entries.DbFileHeader, kv *entries.KeyValueEntry) {
	indexAddr := header.GetIndexOffset(kv.Key)
	err := pool.UpdateIndex(indexAddr, internal.Uint64ToByteArray(0))
	if err != nil {
		t.Fatalf("error updating index: %s", err)
	}
}

// keyValueExists checks whether a given key value entry exists in the data array got from the file
func keyValueExists(t *testing.T, data []byte, header *entries.DbFileHeader, kv *entries.KeyValueEntry) bool {
	idxItemSize := entries.IndexEntrySizeInBytes
	idxAddr := header.GetIndexOffset(kv.Key)
	kvAddrByteArray := data[idxAddr : idxAddr+idxItemSize]
	zero := make([]byte, idxItemSize)

	if !bytes.Equal(kvAddrByteArray, zero) {
		_, err := internal.Uint64FromByteArray(kvAddrByteArray)
		if err != nil {
			t.Fatalf("error converting byte array to uint64: %s", err)
		}
		return true
	}
	return false
}
