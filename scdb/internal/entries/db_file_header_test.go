package entries

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
)

func TestNewDbFileHeader(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	type testRecord struct {
		maxKeys         *uint64
		redundantBlocks *uint16
		expected        *DbFileHeader
	}
	var testMaxKeys uint64 = 24_000_000
	var testRedundantBlocks uint16 = 5
	testTable := []testRecord{
		{nil, nil, generateHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize)},
		{&testMaxKeys, nil, generateHeader(testMaxKeys, DefaultRedundantBlocks, blockSize)},
		{nil, &testRedundantBlocks, generateHeader(DefaultMaxKeys, testRedundantBlocks, blockSize)},
		{&testMaxKeys, &testRedundantBlocks, generateHeader(testMaxKeys, testRedundantBlocks, blockSize)},
	}

	for _, record := range testTable {
		got := NewDbFileHeader(record.maxKeys, record.redundantBlocks, &blockSize)
		assert.Equal(t, record.expected, got)
	}
}

func TestExtractDbFileHeaderFromByteArray(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: Scdb versn 0.001
	titleBytes := []byte{
		83, 99, 100, 98, 32, 118, 101, 114, 115, 110, 32, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 70)

	t.Run("ExtractDbFileHeaderFromByteArrayDoesJustThat", func(t *testing.T) {
		type testRecord struct {
			data     []byte
			expected *DbFileHeader
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
					reserveBytes),
				expected: generateHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					reserveBytes),
				expected: generateHeader(24_000_000, DefaultRedundantBlocks, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 9 */
					[]byte{0, 9},
					reserveBytes),
				expected: generateHeader(DefaultMaxKeys, 9, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 5 */
					[]byte{0, 5},
					reserveBytes),
				expected: generateHeader(24_000_000, 5, blockSize),
			},
		}

		for _, record := range testData {
			got, err := ExtractDbFileHeaderFromByteArray(record.data)
			if err != nil {
				t.Fatalf("error extracting header from byte array: %s", err)
			}

			assert.Equal(t, record.expected, got)
		}
	})

	t.Run("ExtractDbFileHeaderFromByteArrayRaisesEErrOutOfBoundsWhenArrayIsTooShort", func(t *testing.T) {
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
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					// reserve bytes truncated
					reserveBytes[:60]),
				errors.NewErrOutOfBounds("header length is 90. expected 100"),
			},
		}

		for _, record := range testData {
			_, err := ExtractDbFileHeaderFromByteArray(record.data)
			assert.Equal(t, record.expected, err)
		}
	})
}

func TestExtractDbFileHeaderFromFile(t *testing.T) {
	filePath := "testdb.scdb"
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: Scdb versn 0.001
	titleBytes := []byte{
		83, 99, 100, 98, 32, 118, 101, 114, 115, 110, 32, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 70)

	t.Run("ExtractDbFileHeaderFromFileDoesJustThat", func(t *testing.T) {
		defer func() {
			_ = os.Remove(filePath)
		}()
		type testRecord struct {
			data     []byte
			expected *DbFileHeader
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
					reserveBytes),
				expected: generateHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 1 */
					[]byte{0, 1},
					reserveBytes),
				expected: generateHeader(24_000_000, DefaultRedundantBlocks, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys DefaultMaxKeys */
					[]byte{0, 0, 0, 0, 0, 15, 66, 64},
					/* redundant_blocks 9 */
					[]byte{0, 9},
					reserveBytes),
				expected: generateHeader(DefaultMaxKeys, 9, blockSize),
			},
			{
				data: internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					/* max_keys 24_000_000 */
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					/* redundant_blocks 5 */
					[]byte{0, 5},
					reserveBytes),
				expected: generateHeader(24_000_000, 5, blockSize),
			},
		}

		for _, record := range testData {
			file, err := internal.GenerateFileWithTestData(filePath, record.data)
			if err != nil {
				t.Fatalf("error generating file with data: %s", err)
			}

			got, err := ExtractDbFileHeaderFromFile(file)
			if err != nil {
				t.Fatalf("error extracting header from file: %s", err)
			}
			_ = file.Close()

			assert.Equal(t, record.expected, got)
		}
	})

	t.Run("ExtractDbFileHeaderFromFileRaisesEErrOutOfBoundsWhenFileContentIsTooShort", func(t *testing.T) {
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
					reserveBytes),
				errors.NewErrOutOfBounds("header length is 99. expected 100"),
			},
			{
				internal.ConcatByteArrays(
					titleBytes,
					blockSizeAsBytes,
					[]byte{0, 0, 0, 0, 1, 110, 54, 0},
					[]byte{0, 5},
					// reserve bytes truncated
					reserveBytes[:60]),
				errors.NewErrOutOfBounds("header length is 90. expected 100"),
			},
		}

		for _, record := range testData {
			file, err := internal.GenerateFileWithTestData(filePath, record.data)
			if err != nil {
				t.Fatalf("error generating file with data: %s", err)
			}

			_, err = ExtractDbFileHeaderFromFile(file)
			_ = file.Close()
			assert.Equal(t, record.expected, err)

			err = os.Remove(filePath)
			if err != nil {
				t.Fatalf("error deleting db file: %s", err)
			}
		}
	})

}

func TestDbFileHeader_AsBytes(t *testing.T) {
	blockSize := uint32(os.Getpagesize())
	blockSizeAsBytes := internal.Uint32ToByteArray(blockSize)
	// title: Scdb versn 0.001
	titleBytes := []byte{
		83, 99, 100, 98, 32, 118, 101, 114, 115, 110, 32, 48, 46, 48, 48, 49,
	}
	reserveBytes := make([]byte, 70)
	type testRecord struct {
		expected []byte
		header   *DbFileHeader
	}

	testData := []testRecord{
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys DefaultMaxKeys */
				[]byte{0, 0, 0, 0, 0, 15, 66, 64},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				reserveBytes),
			header: generateHeader(DefaultMaxKeys, DefaultRedundantBlocks, blockSize),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 24_000_000 */
				[]byte{0, 0, 0, 0, 1, 110, 54, 0},
				/* redundant_blocks 1 */
				[]byte{0, 1},
				reserveBytes),
			header: generateHeader(24_000_000, DefaultRedundantBlocks, blockSize),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys DefaultMaxKeys */
				[]byte{0, 0, 0, 0, 0, 15, 66, 64},
				/* redundant_blocks 9 */
				[]byte{0, 9},
				reserveBytes),
			header: generateHeader(DefaultMaxKeys, 9, blockSize),
		},
		{
			expected: internal.ConcatByteArrays(
				titleBytes,
				blockSizeAsBytes,
				/* max_keys 24_000_000 */
				[]byte{0, 0, 0, 0, 1, 110, 54, 0},
				/* redundant_blocks 5 */
				[]byte{0, 5},
				reserveBytes),
			header: generateHeader(24_000_000, 5, blockSize),
		},
	}

	for _, record := range testData {
		got := record.header.AsBytes()
		assert.Equal(t, record.expected, got)
	}
}

func TestDbFileHeader_GetIndexOffset(t *testing.T) {
	dbHeader := NewDbFileHeader(nil, nil, nil)
	offset := dbHeader.GetIndexOffset([]byte("foo"))
	block1Start := HeaderSizeInBytes
	block1End := dbHeader.NetBlockSize + block1Start
	assert.LessOrEqual(t, block1Start, offset)
	assert.Less(t, offset, block1End)
}

func TestDbFileHeader_GetIndexOffsetInNthBlock(t *testing.T) {
	dbHeader := NewDbFileHeader(nil, nil, nil)
	initialOffset := dbHeader.GetIndexOffset([]byte("foo"))
	numberOfBlocks := dbHeader.NumberOfIndexBlocks

	t.Run("GetIndexOffsetInNthBlockWorksAsExpected", func(t *testing.T) {
		for i := uint64(0); i < numberOfBlocks; i++ {
			blockStart := HeaderSizeInBytes + (i * dbHeader.NetBlockSize)
			blockEnd := dbHeader.NetBlockSize + blockStart
			offset, err := dbHeader.GetIndexOffsetInNthBlock(initialOffset, i)
			if err != nil {
				t.Fatalf("error getting index offset in nth block: %s", err)
			}
			assert.LessOrEqual(t, blockStart, offset)
			assert.Less(t, offset, blockEnd)
		}
	})

	t.Run("GetIndexOffsetReturnsErrOutOfBoundsIfNIsBeyondNumberOfIndexBlocksInHeader", func(t *testing.T) {
		for i := numberOfBlocks; i < numberOfBlocks+2; i++ {
			_, err := dbHeader.GetIndexOffsetInNthBlock(initialOffset, i)
			expectedError := errors.NewErrOutOfBounds(fmt.Sprintf("n %d is out of bounds", i))
			assert.Equal(t, expectedError, err)
		}
	})

}

// generateHeader generates a DbFileHeader basing on the inputs supplied.
// This is just a helper for tests
func generateHeader(maxKeys uint64, redundantBlocks uint16, blockSize uint32) *DbFileHeader {
	itemsPerIndexBlock := uint64(math.Floor(float64(blockSize) / float64(IndexEntrySizeInBytes)))
	netBlockSize := itemsPerIndexBlock * IndexEntrySizeInBytes
	numberOfIndexBlocks := uint64(math.Ceil(float64(maxKeys)/float64(itemsPerIndexBlock))) + uint64(redundantBlocks)
	keyValuesStartPoint := HeaderSizeInBytes + (netBlockSize * numberOfIndexBlocks)

	return &DbFileHeader{
		Title:               []byte("Scdb versn 0.001"),
		BlockSize:           blockSize,
		MaxKeys:             maxKeys,
		RedundantBlocks:     redundantBlocks,
		ItemsPerIndexBlock:  itemsPerIndexBlock,
		NumberOfIndexBlocks: numberOfIndexBlocks,
		KeyValuesStartPoint: keyValuesStartPoint,
		NetBlockSize:        netBlockSize,
	}
}
