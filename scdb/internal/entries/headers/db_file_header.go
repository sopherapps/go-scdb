package headers

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"os"
)

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

	updateDerivedProps(&header)

	return &header
}

// ExtractDbFileHeaderFromByteArray extracts the header from the data byte array
func ExtractDbFileHeaderFromByteArray(data []byte) (*DbFileHeader, error) {
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

	header := DbFileHeader{
		Title:           title,
		BlockSize:       blockSize,
		MaxKeys:         maxKeys,
		RedundantBlocks: redundantBlocks,
	}

	updateDerivedProps(&header)

	return &header, nil
}

// ExtractDbFileHeaderFromFile extracts the header from a database file
func ExtractDbFileHeaderFromFile(file *os.File) (*DbFileHeader, error) {
	data, err := readHeaderFile(file)
	if err != nil {
		return nil, err
	}

	return ExtractDbFileHeaderFromByteArray(data)
}

func (h *DbFileHeader) AsBytes() []byte {
	return internal.ConcatByteArrays(
		h.Title,
		internal.Uint32ToByteArray(h.BlockSize),
		internal.Uint64ToByteArray(h.MaxKeys),
		internal.Uint16ToByteArray(h.RedundantBlocks),
		make([]byte, 70),
	)
}

func (h *DbFileHeader) GetItemsPerIndexBlock() uint64 {
	return h.ItemsPerIndexBlock
}

func (h *DbFileHeader) GetNumberOfIndexBlocks() uint64 {
	return h.NumberOfIndexBlocks
}

func (h *DbFileHeader) GetNetBlockSize() uint64 {
	return h.NetBlockSize
}

func (h *DbFileHeader) GetBlockSize() uint32 {
	return h.BlockSize
}

func (h *DbFileHeader) GetMaxKeys() uint64 {
	return h.MaxKeys
}

func (h *DbFileHeader) GetRedundantBlocks() uint16 {
	return h.RedundantBlocks
}

func (h *DbFileHeader) SetItemsPerIndexBlock(u uint64) {
	h.ItemsPerIndexBlock = u
}

func (h *DbFileHeader) SetNumberOfIndexBlocks(u uint64) {
	h.NumberOfIndexBlocks = u
}

func (h *DbFileHeader) SetNetBlockSize(u uint64) {
	h.NetBlockSize = u
}

func (h *DbFileHeader) SetValuesStartPoint(u uint64) {
	h.KeyValuesStartPoint = u
}
