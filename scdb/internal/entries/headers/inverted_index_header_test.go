package headers

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
)

func TestNewInvertedIndexHeader(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	type testRecord struct {
		maxKeys         *uint64
		redundantBlocks *uint16
		maxIndexKeyLen  *uint32
		expected        *InvertedIndexHeader
	}
	var testMaxKeys uint64 = 24_000_000
	var testRedundantBlocks uint16 = 5
	var testMaxIndexKeyLen uint32 = 4
	expectedDefaultMaxKeys := DefaultMaxKeys * uint64(DefaultMaxIndexKeyLen)
	testTable := []testRecord{
		{nil, nil, nil, generateInvertedIndexHeader(expectedDefaultMaxKeys, DefaultRedundantBlocks, blockSize, DefaultMaxIndexKeyLen)},
		{&testMaxKeys, nil, nil, generateInvertedIndexHeader(testMaxKeys, DefaultRedundantBlocks, blockSize, DefaultMaxIndexKeyLen)},
		{nil, &testRedundantBlocks, nil, generateInvertedIndexHeader(expectedDefaultMaxKeys, testRedundantBlocks, blockSize, DefaultMaxIndexKeyLen)},
		{nil, nil, &testMaxIndexKeyLen, generateInvertedIndexHeader(DefaultMaxKeys*uint64(testMaxIndexKeyLen), DefaultRedundantBlocks, blockSize, testMaxIndexKeyLen)},
		{&testMaxKeys, &testRedundantBlocks, &testMaxIndexKeyLen, generateInvertedIndexHeader(testMaxKeys, testRedundantBlocks, blockSize, testMaxIndexKeyLen)},
	}

	for _, record := range testTable {
		got := NewInvertedIndexHeader(record.maxKeys, record.redundantBlocks, &blockSize, record.maxIndexKeyLen)
		assert.Equal(t, record.expected, got)
	}
}

func TestExtractInvertedIndexHeaderFromByteArray(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: ScdbIndex v0.001
	titleBytes := []byte{
		83, 99, 100, 98, 73, 110, 100, 101, 120, 32, 118, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 66)

	t.Run("ExtractInvertedIndexHeaderFromByteArrayDoesJustThat", func(t *testing.T) {
		type testRecord struct {
			data     []byte
			expected *InvertedIndexHeader
		}

		testData := []testRecord{
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize, 3),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					/* max_index_key_len 9 */
					[]byte{0, 0, 0, 9},
					reserveBytes),
				expected: generateInvertedIndexHeader(24_000_000, DefaultRedundantBlocks, blockSize, 9),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 9 */
					[]byte{0, 9},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(DefaultMaxKeys, 9, blockSize, 3),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 5 */
					[]byte{0, 5},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(24_000_000, 5, blockSize, 3),
			},
		}

		for _, record := range testData {
			got, err := ExtractInvertedIndexHeaderFromByteArray(record.data)
			if err != nil {
				t.Fatalf("error extracting header from byte array: %s", err)
			}

			assert.Equal(t, record.expected, got)
		}
	})

	t.Run("ExtractInvertedIndexHeaderFromByteArrayRaisesEErrOutOfBoundsWhenArrayIsTooShort", func(t *testing.T) {
		type testRecord struct {
			data     []byte
			expected *errors.ErrOutOfBounds
		}

		testData := []testRecord{
			{
				internal.ConcatByteArrays(
					// title truncated
					titleBytes[2:],
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					[]byte{0, 1},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 98. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					// block size truncated
					blockSizeAsBytes[:3],
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 1},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys truncated */
					[]byte{0, 15, 66, 64},
					[]byte{0, 9},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 96. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks truncated */
					[]byte{5},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					/* maxIndexKeyLen truncated */
					[]byte{0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					[]byte{0, 0, 0, 3},
					// reserve bytes truncated
					reserveBytes[:60]),
				errors.NewErrOutOfBounds("header length is 94. expected 100"),
			},
		}

		for _, record := range testData {
			_, err := ExtractInvertedIndexHeaderFromByteArray(record.data)
			assert.Equal(t, record.expected, err)
		}
	})
}

func TestExtractInvertedIndexHeaderFromFile(t *testing.T) {
	filePath := "testdb.scdb"
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: ScdbIndex v0.001
	titleBytes := []byte{
		83, 99, 100, 98, 73, 110, 100, 101, 120, 32, 118, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 66)

	t.Run("ExtractInvertedIndexHeaderFromFileDoesJustThat", func(t *testing.T) {
		defer func() {
			_ = os.Remove(filePath)
		}()
		type testRecord struct {
			data     []byte
			expected *InvertedIndexHeader
		}

		testData := []testRecord{
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize, 3),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(24_000_000, DefaultRedundantBlocks, blockSize, 3),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 9 */
					[]byte{0, 9},
					/* max_index_key_len 3 */
					[]byte{0, 0, 0, 3},
					reserveBytes),
				expected: generateInvertedIndexHeader(DefaultMaxKeys, 9, blockSize, 3),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 5 */
					[]byte{0, 5},
					/* max_index_key_len 8 */
					[]byte{0, 0, 0, 8},
					reserveBytes),
				expected: generateInvertedIndexHeader(24_000_000, 5, blockSize, 8),
			},
		}

		for _, record := range testData {
			file, err := internal.GenerateFileWithTestData(filePath, record.data)
			if err != nil {
				t.Fatalf("error generating file with data: %s", err)
			}

			got, err := ExtractInvertedIndexHeaderFromFile(file)
			if err != nil {
				t.Fatalf("error extracting header from file: %s", err)
			}
			_ = file.Close()

			assert.Equal(t, record.expected, got)
		}
	})

	t.Run("ExtractInvertedIndexHeaderFromFileRaisesEErrOutOfBoundsWhenFileContentIsTooShort", func(t *testing.T) {
		defer func() {
			_ = os.Remove(filePath)
		}()
		type testRecord struct {
			data     []byte
			expected *errors.ErrOutOfBounds
		}

		testData := []testRecord{
			{
				internal.ConcatByteArrays(
					// title truncated
					titleBytes[2:],
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					[]byte{0, 1},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 98. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					// block size truncated
					blockSizeAsBytes[:3],
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 1},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys truncated */
					[]byte{0, 15, 66, 64},
					[]byte{0, 9},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 96. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks truncated */
					[]byte{5},
					[]byte{0, 0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					/* maxIndexKeyLen truncated */
					[]byte{0, 0, 3},
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					[]byte{0, 0, 0, 3},
					// reserve bytes truncated
					reserveBytes[:60]),
				errors.NewErrOutOfBounds("header length is 94. expected 100"),
			},
		}

		for _, record := range testData {
			file, err := internal.GenerateFileWithTestData(filePath, record.data)
			if err != nil {
				t.Fatalf("error generating file with data: %s", err)
			}

			_, err = ExtractInvertedIndexHeaderFromFile(file)
			_ = file.Close()
			assert.Equal(t, record.expected, err)

			err = os.Remove(filePath)
			if err != nil {
				t.Fatalf("error deleting db file: %s", err)
			}
		}
	})

}

func TestInvertedIndexHeader_AsBytes(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: ScdbIndex v0.001
	titleBytes := []byte{
		83, 99, 100, 98, 73, 110, 100, 101, 120, 32, 118, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 66)
	type testRecord struct {
		expected []byte
		header   *InvertedIndexHeader
	}

	testMaxIndexKeyLen := uint32(9)

	testData := []testRecord{
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys DefaultMaxKeys */
				[]byte{0, 0, 0, 0, 0, 15, 66, 64},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				/* max_index_key_len 3 */
				[]byte{0, 0, 0, 3},
				reserveBytes),
			header: generateInvertedIndexHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize, 3),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 24_000_000 */
				[]byte{0, 0, 0, 0, 1, 110, 54, 0},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				/* max_index_key_len 3 */
				[]byte{0, 0, 0, 3},
				reserveBytes),
			header: generateInvertedIndexHeader(24_000_000, DefaultRedundantBlocks, blockSize, 3),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys DefaultMaxKeys */
				[]byte{0, 0, 0, 0, 0, 15, 66, 64},
				/* redundant_blocks 9 */
				[]byte{0, 9},
				/* max_index_key_len 3 */
				[]byte{0, 0, 0, 3},
				reserveBytes),
			header: generateInvertedIndexHeader(DefaultMaxKeys, 9, blockSize, 3),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 24_000_000 */
				[]byte{0, 0, 0, 0, 1, 110, 54, 0},
				/* redundant_blocks 5 */
				[]byte{0, 5},
				/* max_index_key_len 9 */
				[]byte{0, 0, 0, 9},
				reserveBytes),
			header: generateInvertedIndexHeader(24_000_000, 5, blockSize, 9),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 3_000_000 i.e. maxIndexKeyLen * DefaultMaxKeys*/
				[]byte{0, 0, 0, 0, 0, 45, 198, 192},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				/* max_index_key_len 3 */
				[]byte{0, 0, 0, 3},
				reserveBytes),
			header: NewInvertedIndexHeader(nil, nil, nil, nil),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 9_000_000 i.e. maxIndexKeyLen * DefaultMaxKeys*/
				[]byte{0, 0, 0, 0, 0, 137, 84, 64},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				/* max_index_key_len 9 */
				[]byte{0, 0, 0, 9},
				reserveBytes),
			header: NewInvertedIndexHeader(nil, nil, nil, &testMaxIndexKeyLen),
		},
	}

	for _, record := range testData {
		got := record.header.AsBytes()
		assert.Equal(t, record.expected, got)
	}
}

func TestInvertedIndexHeader_GetIndexOffset(t *testing.T) {
	dbHeader := NewInvertedIndexHeader(nil, nil, nil, nil)
	offset := GetIndexOffset(dbHeader, []byte("foo"))
	block1Start := HeaderSizeInBytes
	block1End := dbHeader.NetBlockSize + block1Start
	assert.LessOrEqual(t, block1Start, offset)
	assert.Less(t, offset, block1End)
}

func TestInvertedIndexHeader_GetIndexOffsetInNthBlock(t *testing.T) {
	dbHeader := NewInvertedIndexHeader(nil, nil, nil, nil)
	initialOffset := GetIndexOffset(dbHeader, []byte("foo"))
	numberOfBlocks := dbHeader.NumberOfIndexBlocks

	t.Run("GetIndexOffsetInNthBlockWorksAsExpected", func(t *testing.T) {
		for i := uint64(0); i < numberOfBlocks; i++ {
			blockStart := HeaderSizeInBytes + (i * dbHeader.NetBlockSize)
			blockEnd := dbHeader.NetBlockSize + blockStart
			offset, err := GetIndexOffsetInNthBlock(dbHeader, initialOffset, i)
			if err != nil {
				t.Fatalf("error getting index offset in nth block: %s", err)
			}
			assert.LessOrEqual(t, blockStart, offset)
			assert.Less(t, offset, blockEnd)
		}
	})

	t.Run("GetIndexOffsetReturnsErrOutOfBoundsIfNIsBeyondNumberOfIndexBlocksInHeader", func(t *testing.T) {
		for i := numberOfBlocks; i < numberOfBlocks+2; i++ {
			_, err := GetIndexOffsetInNthBlock(dbHeader, initialOffset, i)
			expectedError := errors.NewErrOutOfBounds(fmt.Sprintf("n %d is out of bounds", i))
			assert.Equal(t, expectedError, err)
		}
	})

}

// generateInvertedIndexHeader generates a InvertedIndexHeader basing on the inputs supplied.
// This is just a helper for tests
func generateInvertedIndexHeader(maxKeys uint64, redundantBlocks uint16, blockSize uint32, maxIndexKeyLen uint32) *InvertedIndexHeader {
	itemsPerIndexBlock := uint64(math.Floor(float64(blockSize) / float64(IndexEntrySizeInBytes)))
	netBlockSize := itemsPerIndexBlock * IndexEntrySizeInBytes
	numberOfIndexBlocks := uint64(math.Ceil(float64(maxKeys)/float64(itemsPerIndexBlock))) + uint64(redundantBlocks)
	valuesStartPoint := HeaderSizeInBytes + (netBlockSize * numberOfIndexBlocks)

	return &InvertedIndexHeader{
		Title:               []byte("ScdbIndex v0.001"),
		BlockSize:           blockSize,
		MaxKeys:             maxKeys,
		RedundantBlocks:     redundantBlocks,
		ItemsPerIndexBlock:  itemsPerIndexBlock,
		NumberOfIndexBlocks: numberOfIndexBlocks,
		ValuesStartPoint:    valuesStartPoint,
		NetBlockSize:        netBlockSize,
		MaxIndexKeyLen:      maxIndexKeyLen,
	}
}
