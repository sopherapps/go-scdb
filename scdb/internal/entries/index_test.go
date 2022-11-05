package entries

import (
	"fmt"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIndex_Blocks(t *testing.T) {
	filePath := "testdb.scdb"
	entriesPerBlock := uint64(2) // 2 keys per block
	numberOfBlocks := uint64(4)  // 4 index blocks
	redundantBlocks := uint16(1) // 1 redundant block

	// derived props
	blockSize := uint32(entriesPerBlock * IndexEntrySizeInBytes)
	maxKeys := numberOfBlocks * entriesPerBlock // together with redundant blocks, keys in index = 10

	header := NewDbFileHeader(&maxKeys, &redundantBlocks, &blockSize)
	headerBytes := header.AsBytes()
	expectedIndexBlocks := [][]byte{
		{0, 39, 67, 5, 89, 39, 45, 78, 0, 39, 67, 236, 89, 39, 45, 78},
		{7, 39, 67, 236, 0, 39, 7, 78, 0, 39, 67, 236, 7, 39, 45, 78},
		{0, 39, 67, 6, 89, 23, 45, 78, 0, 0, 0, 0, 89, 39, 45, 78},
		{0, 39, 45, 12, 89, 23, 45, 78, 0, 0, 67, 0, 89, 39, 45, 78},
		{20, 39, 5, 6, 89, 23, 5, 78, 0, 0, 54, 0, 89, 39, 45, 78},
	}
	indexBytes := internal.ConcatByteArrays(expectedIndexBlocks...)

	t.Run("BlocksReturnsTheBlocksAsLazyIterator", func(t *testing.T) {
		defer func() {
			_ = os.Remove(filePath)
		}()
		fileData := internal.ConcatByteArrays(headerBytes, indexBytes)

		file, err := internal.GenerateFileWithTestData(filePath, fileData)
		if err != nil {
			t.Fatalf("error writing test data into file: %s", err)
		}
		defer func() {
			_ = file.Close()
		}()

		index := NewIndex(file, header)
		block := 0

		for result := range index.Blocks() {
			assert.Equal(t, nil, result.Err)
			assert.Equal(t, expectedIndexBlocks[block], result.Data)
			block += 1
		}
	})

	t.Run("BlocksReturnsErrIfItFailsToReadFile", func(t *testing.T) {
		defer func() {
			_ = os.Remove(filePath)
		}()
		fileData := internal.ConcatByteArrays(headerBytes, indexBytes)

		file, err := internal.GenerateFileWithTestData(filePath, fileData)
		if err != nil {
			t.Fatalf("error writing test data into file: %s", err)
		}
		defer func() {
			_ = file.Close()
		}()

		index := NewIndex(file, header)
		block := 0

		// Delete the file before accessing its blocks
		e := file.Close()
		if e != nil {
			t.Fatalf("error closing file: %s", e)
		}
		e = os.Remove(filePath)
		if e != nil {
			t.Fatalf("error removing file: %s", e)
		}

		var emptyArray []byte

		for result := range index.Blocks() {
			errString := fmt.Sprintf("%s", result.Err)
			assert.Equal(t, "read testdb.scdb: file already closed", errString)
			assert.Equal(t, emptyArray, result.Data)
			block += 1
		}

		// Loop was once
		assert.Equal(t, block, 1)
	})
}
