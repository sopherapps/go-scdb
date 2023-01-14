package headers

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"math"
	"os"
)

const IndexEntrySizeInBytes uint64 = 8
const HeaderSizeInBytes uint64 = 100
const DefaultMaxKeys uint64 = 1_000_000
const DefaultRedundantBlocks uint16 = 1

type Header interface {
	// GetItemsPerIndexBlock gets the number of items per index block
	GetItemsPerIndexBlock() uint64

	// GetNumberOfIndexBlocks gets the number of index blocks for given header
	GetNumberOfIndexBlocks() uint64

	// GetNetBlockSize gets the net size of each index block
	GetNetBlockSize() uint64

	// GetBlockSize gets the raw block size used for the file
	GetBlockSize() uint32

	// GetMaxKeys gets the maximum number of keys permitted for the given file-based map
	GetMaxKeys() uint64

	// GetRedundantBlocks gets the redundant blocks to add to the index blocks to reduce hash collisions
	// as the file of the file-based map gets full
	GetRedundantBlocks() uint16

	// AsBytes retrieves the byte array that represents the header.
	AsBytes() []byte

	// SetItemsPerIndexBlock sets the number of items per index block of the header
	SetItemsPerIndexBlock(u uint64)

	// SetNumberOfIndexBlocks sets the number of index blocks
	SetNumberOfIndexBlocks(u uint64)

	// SetNetBlockSize sets the net block size
	SetNetBlockSize(u uint64)

	// SetValuesStartPoint sets the values(or key-values) starting address in the file
	SetValuesStartPoint(u uint64)
}

// ReadHeaderFile reads the contents of a header file and returns
// the bytes there in
//
// The data got can be used to construct a Header instance
// for instance
func readHeaderFile(file *os.File) ([]byte, error) {
	buf := make([]byte, HeaderSizeInBytes)
	n, err := file.ReadAt(buf, 0)
	if n < int(HeaderSizeInBytes) {
		return nil, errors.NewErrOutOfBounds(fmt.Sprintf("header length is %d. expected %d", n, HeaderSizeInBytes))
	}

	if err != nil {
		return nil, err
	}

	return buf, nil
}

// GetIndexOffset computes the offset for the given key in the first index block.
// It uses the meta data in the header
// i.e. number of items per block and the `IndexEntrySizeInBytes`
func GetIndexOffset(h Header, key []byte) uint64 {
	hash := internal.GetHash(key, h.GetItemsPerIndexBlock())
	return HeaderSizeInBytes + (hash * IndexEntrySizeInBytes)
}

// GetIndexOffsetInNthBlock returns the index offset for the nth index block if `initialOffset` is the offset
// in the top most index block `n` starts at zero where zero is the top most index block
func GetIndexOffsetInNthBlock(h Header, initialOffset uint64, n uint64) (uint64, error) {
	if n >= h.GetNumberOfIndexBlocks() {
		return 0, errors.NewErrOutOfBounds(fmt.Sprintf("n %d is out of bounds", n))
	}
	num := initialOffset + (h.GetNetBlockSize() * n)
	return num, nil
}

// InitializeFile initializes the database/index file, giving it the header and the index placeholders
// and truncating it.
//
// It returns the new file size
func InitializeFile(file *os.File, header Header) (int64, error) {
	headerBytes := header.AsBytes()
	headerLength := int64(len(headerBytes))
	finalSize := headerLength + int64(header.GetNumberOfIndexBlocks()*header.GetNetBlockSize())

	// First shrink file to zero, to delete all data
	err := file.Truncate(0)
	if err != nil {
		return 0, err
	}

	// The expanded the file again
	err = file.Truncate(finalSize)
	if err != nil {
		return 0, err
	}

	_, err = file.WriteAt(headerBytes, 0)
	if err != nil {
		return 0, err
	}

	return finalSize, nil
}

// updateDerivedProps computes the properties that depend on the user-defined/default properties and update them
// on the header
func updateDerivedProps(h Header) {
	h.SetItemsPerIndexBlock(uint64(math.Floor(float64(h.GetBlockSize()) / float64(IndexEntrySizeInBytes))))
	h.SetNumberOfIndexBlocks(uint64(math.Ceil(float64(h.GetMaxKeys())/float64(h.GetItemsPerIndexBlock()))) + uint64(h.GetRedundantBlocks()))
	h.SetNetBlockSize(h.GetItemsPerIndexBlock() * IndexEntrySizeInBytes)
	h.SetValuesStartPoint(HeaderSizeInBytes + (h.GetNetBlockSize() * h.GetNumberOfIndexBlocks()))
}
