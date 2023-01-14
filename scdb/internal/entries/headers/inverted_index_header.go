package headers

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"os"
)

const DefaultMaxIndexKeyLen uint32 = 3

type InvertedIndexHeader struct {
	Title               []byte
	BlockSize           uint32
	MaxKeys             uint64
	RedundantBlocks     uint16
	ItemsPerIndexBlock  uint64
	NumberOfIndexBlocks uint64
	ValuesStartPoint    uint64
	NetBlockSize        uint64
	MaxIndexKeyLen      uint32
}

// NewInvertedIndexHeader creates a new InvertedIndexHeader
func NewInvertedIndexHeader(maxKeys *uint64, redundantBlocks *uint16, blockSize *uint32, maxIndexKeyLen *uint32) *InvertedIndexHeader {
	header := InvertedIndexHeader{
		Title: []byte("ScdbIndex v0.001"),
	}

	if maxIndexKeyLen != nil {
		header.MaxIndexKeyLen = *maxIndexKeyLen
	} else {
		header.MaxIndexKeyLen = DefaultMaxIndexKeyLen
	}

	if maxKeys != nil {
		header.MaxKeys = *maxKeys
	} else {
		header.MaxKeys = DefaultMaxKeys * uint64(header.MaxIndexKeyLen)
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

	updateDerivedProps(&header)

	return &header
}

// ExtractInvertedIndexHeaderFromByteArray extracts the inverted index header from the data byte array
func ExtractInvertedIndexHeaderFromByteArray(data []byte) (*InvertedIndexHeader, error) {
	dataLength := len(data)
	if dataLength < int(HeaderSizeInBytes) {
		return nil, errors.NewErrOutOfBounds(fmt.Sprintf("header length is %d. expected %d", dataLength, HeaderSizeInBytes))
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
	maxIndexKeyLen, err := internal.Uint32FromByteArray(data[30:34])
	if err != nil {
		return nil, err
	}

	header := InvertedIndexHeader{
		Title:           title,
		BlockSize:       blockSize,
		MaxKeys:         maxKeys,
		RedundantBlocks: redundantBlocks,
		MaxIndexKeyLen:  maxIndexKeyLen,
	}

	updateDerivedProps(&header)

	return &header, nil
}

// ExtractInvertedIndexHeaderFromFile extracts the header from an index file
func ExtractInvertedIndexHeaderFromFile(file *os.File) (*InvertedIndexHeader, error) {
	data, err := readHeaderFile(file)
	if err != nil {
		return nil, err
	}

	return ExtractInvertedIndexHeaderFromByteArray(data)
}

func (h *InvertedIndexHeader) AsBytes() []byte {
	return internal.ConcatByteArrays(
		h.Title,
		internal.Uint32ToByteArray(h.BlockSize),
		internal.Uint64ToByteArray(h.MaxKeys),
		internal.Uint16ToByteArray(h.RedundantBlocks),
		internal.Uint32ToByteArray(h.MaxIndexKeyLen),
		make([]byte, 66),
	)
}

func (h *InvertedIndexHeader) GetItemsPerIndexBlock() uint64 {
	return h.ItemsPerIndexBlock
}

func (h *InvertedIndexHeader) GetNumberOfIndexBlocks() uint64 {
	return h.NumberOfIndexBlocks
}

func (h *InvertedIndexHeader) GetNetBlockSize() uint64 {
	return h.NetBlockSize
}

func (h *InvertedIndexHeader) GetBlockSize() uint32 {
	return h.BlockSize
}

func (h *InvertedIndexHeader) GetMaxKeys() uint64 {
	return h.MaxKeys
}

func (h *InvertedIndexHeader) GetRedundantBlocks() uint16 {
	return h.RedundantBlocks
}

func (h *InvertedIndexHeader) SetItemsPerIndexBlock(u uint64) {
	h.ItemsPerIndexBlock = u
}

func (h *InvertedIndexHeader) SetNumberOfIndexBlocks(u uint64) {
	h.NumberOfIndexBlocks = u
}

func (h *InvertedIndexHeader) SetNetBlockSize(u uint64) {
	h.NetBlockSize = u
}

func (h *InvertedIndexHeader) SetValuesStartPoint(u uint64) {
	h.ValuesStartPoint = u
}
