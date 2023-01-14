package inverted_index

import (
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/headers"
	"os"
)

type InvertedIndex struct {
	File             *os.File
	FilePath         string
	MaxIndexKeyLen   uint32
	ValuesStartPoint uint64
	FileSize         uint64
	header           *headers.InvertedIndexHeader
}

// NewInvertedIndex initializes a new Inverted Index
//
// The max keys used in the search file are `max_index_key_len` * `db_max_keys`
// Since we each db key will be represented in the index a number of `max_index_key_len` times
// for example the key `food` must have the following index keys: `f`, `fo`, `foo`, `food`.
func NewInvertedIndex(filePath string, maxIndexKeyLen *uint32, dbMaxKeys *uint64, dbRedundantBlocks *uint16) (*InvertedIndex, error) {
	blockSize := uint32(os.Getpagesize())

	dbFileExists, err := internal.PathExists(filePath)
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

	var header *headers.InvertedIndexHeader
	if !dbFileExists {
		header = headers.NewInvertedIndexHeader(dbMaxKeys, dbRedundantBlocks, &blockSize, maxIndexKeyLen)
		_, err = headers.InitializeFile(file, header)
		if err != nil {
			return nil, err
		}
	} else {
		header, err = headers.ExtractInvertedIndexHeaderFromFile(file)
		if err != nil {
			return nil, err
		}
	}

	fileSize, err := internal.GetFileSize(file)
	if err != nil {
		return nil, err
	}

	idx := InvertedIndex{
		File:             file,
		FilePath:         filePath,
		MaxIndexKeyLen:   header.MaxIndexKeyLen,
		ValuesStartPoint: header.ValuesStartPoint,
		FileSize:         fileSize,
		header:           header,
	}

	return &idx, nil
}

// Add adds a key's kv address in the corresponding prefixes' lists to update the inverted index
func (idx *InvertedIndex) Add(key []byte, kvAddr uint64, expiry uint64) error {
	return nil
}

// Search returns list of db key-value addresses corresponding to the given term
//
// # The addresses can then be used to get the list of key-values from the db
//
// It skips the first `skip` number of results and returns not more than
// `limit` number of items. This is to avoid using up more memory than can be handled by the
// host machine.
//
// If `limit` is 0, all items are returned since it would make no sense for someone to search
// for zero items.
func (idx *InvertedIndex) Search(term []byte, skip uint64, limit uint64) ([]uint64, error) {
	return nil, nil
}

// Remove deletes the key's kv address from all prefixes' lists in the inverted index
func (idx *InvertedIndex) Remove(key []byte) error {
	return nil
}

// Clear clears all the data in the search index, except the header, and its original
// variables
func (idx *InvertedIndex) Clear() error {
	header := headers.NewInvertedIndexHeader(&idx.header.MaxKeys, &idx.header.RedundantBlocks, &idx.header.BlockSize, &idx.header.MaxIndexKeyLen)
	fileSize, err := headers.InitializeFile(idx.File, header)
	if err != nil {
		return err
	}

	idx.FileSize = uint64(fileSize)
	return nil
}

// Eq checks if the other InvertedIndex instance equals the current inverted index
func (idx *InvertedIndex) Eq(other *InvertedIndex) bool {
	return idx.ValuesStartPoint == other.ValuesStartPoint &&
		idx.MaxIndexKeyLen == other.MaxIndexKeyLen &&
		idx.FilePath == other.FilePath &&
		idx.FileSize == other.FileSize
}

// Close closes the buffer pool, freeing up any resources
func (idx *InvertedIndex) Close() error {
	return idx.File.Close()
}
