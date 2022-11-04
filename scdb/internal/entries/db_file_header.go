package entries

import (
	"fmt"
	"github.com/sopherapps/go-scbd/scdb"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"math"
	"os"
)

const IndexEntrySizeInBytes uint64 = 8
const HeaderSizeInBytes uint64 = 100
const DefaultMaxKeys uint64 = 1_000_000
const DefaultRedundantBlocks uint16 = 1

type DbFileHeader struct {
	Title               []byte
	BlockSize           uint32
	MaxKeys             uint64
	RedundantBlocks     uint16
	ItemsPerIndexBlock  uint64
	NumberOfIndexBlocks uint64
	KeyValuesStartPoint uint64
	NetBlockSize        uint64
}

// NewDbFileHeader Creates a new DbFileHeader
func NewDbFileHeader(maxKeys *uint64, redundantBlocks *uint16, blockSize *uint32) *DbFileHeader {
	header := DbFileHeader{
		Title: []byte("Scdb versn 0.001"),
	}

	if maxKeys != nil {
		header.MaxKeys = *maxKeys
	} else {
		header.MaxKeys = DefaultMaxKeys
	}

	if redundantBlocks != nil {
		header.RedundantBlocks = *redundantBlocks
	} else {
		header.RedundantBlocks = DefaultRedundantBlocks
	}

	if blockSize != nil {
		header.BlockSize = *blockSize
	} else {
		header.BlockSize = uint32(os.Getpagesize())
	}

	header.updateDerivedProps()

	return &header
}

// ExtractDbFileHeaderFromByteArray extracts the header from the data byte array
func ExtractDbFileHeaderFromByteArray(data []byte) (*DbFileHeader, error) {
	dataLength := len(data)
	if dataLength < int(HeaderSizeInBytes) {
		return nil, scdb.NewErrOutOfBounds(fmt.Sprintf("header length is %d. expected %d", dataLength, HeaderSizeInBytes))
	}

	title := data[:16]
	blockSize, err := internal.Uint32FromByteArray(data[16:20])
	if err != nil {
		return nil, err
	}
	maxKeys, err := internal.Uint64FromByteArray(data[20:28])
	if err != nil {
		return nil, err
	}
	redundantBlocks, err := internal.Uint16FromByteArray(data[28:30])
	if err != nil {
		return nil, err
	}

	header := DbFileHeader{
		Title:           title,
		BlockSize:       blockSize,
		MaxKeys:         maxKeys,
		RedundantBlocks: redundantBlocks,
	}

	header.updateDerivedProps()

	return &header, nil
}

// ExtractDbFileHeaderFromFile extracts the header from a database file
func ExtractDbFileHeaderFromFile(file *os.File) (*DbFileHeader, error) {
	buf := make([]byte, HeaderSizeInBytes)
	n, err := file.ReadAt(buf, 0)
	if n < int(HeaderSizeInBytes) {
		return nil, scdb.NewErrOutOfBounds(fmt.Sprintf("header length is %d. expected %d", n, HeaderSizeInBytes))
	}

	if err != nil {
		return nil, err
	}

	return ExtractDbFileHeaderFromByteArray(buf)
}

// updateDerivedProps computes the properties that depend on the user-defined/default properties and update them
// on self
func (h *DbFileHeader) updateDerivedProps() {
	h.ItemsPerIndexBlock = uint64(math.Floor(float64(h.BlockSize) / float64(IndexEntrySizeInBytes)))
	h.NumberOfIndexBlocks = uint64(math.Ceil(float64(h.MaxKeys)/float64(h.ItemsPerIndexBlock))) + uint64(h.RedundantBlocks)
	h.NetBlockSize = h.ItemsPerIndexBlock * IndexEntrySizeInBytes
	h.KeyValuesStartPoint = HeaderSizeInBytes + (h.NetBlockSize * h.NumberOfIndexBlocks)
}

// AsBytes retrieves the byte array that represents the header.
func (h *DbFileHeader) AsBytes() []byte {
	return internal.ConcatByteArrays(
		h.Title,
		internal.Uint32ToByteArray(h.BlockSize),
		internal.Uint64ToByteArray(h.MaxKeys),
		internal.Uint16ToByteArray(h.RedundantBlocks),
		make([]byte, 70),
	)
}

// GetIndexOffset computes the offset for the given key in the first index block.
// It uses the meta data in this header
// i.e. number of items per block and the `IndexEntrySizeInBytes`
func (h *DbFileHeader) GetIndexOffset(key []byte) uint64 {
	hash := internal.GetHash(key, h.ItemsPerIndexBlock)
	return HeaderSizeInBytes + (hash * IndexEntrySizeInBytes)
}

// GetIndexOffsetInNthBlock returns the index offset for the nth index block if `initial_offset` is the offset
// in the top most index block `n` starts at zero where zero is the top most index block
func (h *DbFileHeader) GetIndexOffsetInNthBlock(initialOffset uint64, n uint64) (uint64, error) {
	if n >= h.NumberOfIndexBlocks {
		return 0, scdb.NewErrOutOfBounds(fmt.Sprintf("n %d is out of bounds", n))
	}
	num := initialOffset + (h.NetBlockSize * n)
	return num, nil
}
