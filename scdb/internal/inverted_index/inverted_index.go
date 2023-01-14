package inverted_index

import (
	"bytes"
	"errors"
	scdbErrs "github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/headers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/values"
	"io"
	"math"
	"os"
)

var zeroU64Bytes = make([]byte, headers.IndexEntrySizeInBytes)

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
	upperBound := uint32(math.Min(float64(len(key)), float64(idx.MaxIndexKeyLen))) + 1

	for i := uint32(1); i < upperBound; i++ {
		prefix := key[:i]

		indexBlock := uint64(0)
		indexOffset := headers.GetIndexOffset(idx.header, prefix)

		for {
			indexOffset, err := headers.GetIndexOffsetInNthBlock(idx.header, indexOffset, indexBlock)
			if err != nil {
				return err
			}

			addr, err := idx.readEntryAddress(indexOffset)
			if err != nil {
				return err
			}

			if bytes.Equal(addr, zeroU64Bytes) {
				err = idx.appendNewRootEntry(prefix, indexOffset, key, kvAddr, expiry)
				if err != nil {
					return err
				}

				break
			}

			isForPrefix, err := idx.addrBelongsToPrefix(addr, prefix)
			if err != nil {
				return err
			}

			if isForPrefix {
				err = idx.upsertEntry(prefix, addr, key, kvAddr, expiry)
				if err != nil {
					return err
				}

				break
			}

			indexBlock += 1
			if indexBlock >= idx.header.NumberOfIndexBlocks {
				return scdbErrs.NewErrCollisionSaturation(prefix)
			}
		}
	}

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
	prefixLen := uint32(math.Min(float64(len(term)), float64(idx.MaxIndexKeyLen)))
	prefix := term[:prefixLen]

	indexOffset := headers.GetIndexOffset(idx.header, prefix)

	for indexBlock := uint64(0); indexBlock < idx.header.NumberOfIndexBlocks; indexBlock++ {
		indexOffset, err := headers.GetIndexOffsetInNthBlock(idx.header, indexOffset, indexBlock)
		if err != nil {
			return nil, err
		}

		addr, err := idx.readEntryAddress(indexOffset)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(addr, zeroU64Bytes) {
			return []uint64{}, nil
		}

		isForPrefix, err := idx.addrBelongsToPrefix(addr, prefix)
		if err != nil {
			return nil, err
		}

		if isForPrefix {
			return idx.getMatchedKvAddrsForPrefix(term, addr, skip, limit)
		}
	}

	return []uint64{}, nil
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

// getMatchedKvAddrsForPrefix returns the kv_addresses of all items whose db key contain the given `term`
func (idx *InvertedIndex) getMatchedKvAddrsForPrefix(term []byte, prefixRootAddr []byte, skip uint64, limit uint64) ([]uint64, error) {
	matchedAddrs := make([]uint64, 0)
	skipped := uint64(0)
	shouldSlice := limit > 0

	rootAddr, err := internal.Uint64FromByteArray(prefixRootAddr)
	if err != nil {
		return nil, err
	}

	addr := rootAddr
	for {
		entryBytes, err := readEntryBytes(idx.File, addr)
		if err != nil {
			return nil, err
		}

		entry, err := values.ExtractInvertedIndexEntryFromByteArray(entryBytes, 0)
		if err != nil {
			return nil, err
		}

		if !values.IsExpired(entry) && bytes.Contains(entry.Key, term) {
			if skipped < skip {
				skipped++
			} else {
				matchedAddrs = append(matchedAddrs, entry.KvAddress)
			}

			if shouldSlice && uint64(len(matchedAddrs)) >= limit {
				break
			}
		}

		addr = entry.NextOffset
		// The zero check is for data corruption
		if addr == rootAddr || addr == 0 {
			break
		}
	}

	return matchedAddrs, nil
}

// readEntryAddress reads the index at the given address and returns it
func (idx *InvertedIndex) readEntryAddress(addr uint64) ([]byte, error) {
	err := internal.ValidateBounds(addr, addr+headers.IndexEntrySizeInBytes, headers.HeaderSizeInBytes, idx.ValuesStartPoint, "entry address out of bound")
	if err != nil {
		return nil, err
	}

	buf := make([]byte, headers.IndexEntrySizeInBytes)
	bytesRead, err := idx.File.ReadAt(buf, int64(addr))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return buf[:bytesRead], nil
}

// appendNewRootEntry appends a new root entry to the index file, and updates the inverted index's index
func (idx *InvertedIndex) appendNewRootEntry(prefix []byte, indexOffset uint64, key []byte, kvAddr uint64, expiry uint64) error {
	newAddr := idx.FileSize

	entry := values.NewInvertedIndexEntry(prefix, key, expiry, true, kvAddr, newAddr, newAddr)
	entryAsBytes := entry.AsBytes()
	_, err := idx.File.WriteAt(entryAsBytes, int64(newAddr))
	if err != nil {
		return err
	}

	// update index
	_, err = idx.File.WriteAt(internal.Uint64ToByteArray(newAddr), int64(indexOffset))
	if err != nil {
		return err
	}

	// update file size
	idx.FileSize = newAddr + uint64(len(entryAsBytes))
	return nil
}

// addrBelongsToPrefix checks to see if entry address belongs to the given `prefix` (i.e. index key)
//
// It returns false if the address is out of bounds
// or when the index key there is not equal to `prefix`.
func (idx *InvertedIndex) addrBelongsToPrefix(addr []byte, prefix []byte) (bool, error) {
	address, err := internal.Uint64FromByteArray(addr)
	if err != nil {
		return false, err
	}

	if address >= idx.FileSize {
		return false, nil
	}

	prefixLen := uint32(len(prefix))
	indexKeySizeBuf := make([]byte, 4)
	bytesRead, err := idx.File.ReadAt(indexKeySizeBuf, int64(address+4))
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}

	indexKeySize, err := internal.Uint32FromByteArray(indexKeySizeBuf[:bytesRead])
	if err != nil {
		return false, err
	}

	if prefixLen != indexKeySize {
		return false, nil
	}

	indexKeyBuf := make([]byte, prefixLen)
	bytesRead, err = idx.File.ReadAt(indexKeyBuf, int64(address+8))
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}

	return bytes.Equal(indexKeyBuf[:bytesRead], prefix), nil
}

// upsertEntry updates an existing entry whose prefix (or index key) is given and key is also as given.
//
// It starts at the root of the doubly-linked cyclic list for the given prefix,
// looks for the given key. If it finds it, it updates it. If it does not find it, it appends
// the new entry to the end of that list.
func (idx *InvertedIndex) upsertEntry(prefix []byte, rootAddr []byte, key []byte, kvAddr uint64, expiry uint64) error {
	rootAddrU64, err := internal.Uint64FromByteArray(rootAddr)
	if err != nil {
		return err
	}

	addr := rootAddrU64

	for {
		entryBytes, err := readEntryBytes(idx.File, addr)
		if err != nil {
			return err
		}

		entry, err := values.ExtractInvertedIndexEntryFromByteArray(entryBytes, 0)
		if err != nil {
			return err
		}

		if bytes.Equal(entry.Key, key) {
			entry.KvAddress = kvAddr
			entry.Expiry = expiry
			_, err := writeEntryToFile(idx.File, addr, entry)
			if err != nil {
				return err
			}
			break
		} else if entry.NextOffset == rootAddrU64 {
			// end of the list, append new item to the list
			newEntry := values.NewInvertedIndexEntry(prefix, key, expiry, false, kvAddr, rootAddrU64, addr)
			newEntryLen, err := writeEntryToFile(idx.File, idx.FileSize, newEntry)
			if err != nil {
				return err
			}

			// update the next offset of the current entry to this address
			err = entry.UpdateNextOffsetOnFile(idx.File, addr, idx.FileSize)
			if err != nil {
				return err
			}

			// update the root entry to have its previous offset point to the newly added entry
			rootEntryBytes, err := readEntryBytes(idx.File, rootAddrU64)
			if err != nil {
				return err
			}
			rootEntry, err := values.ExtractInvertedIndexEntryFromByteArray(rootEntryBytes, 0)
			if err != nil {
				return err
			}
			err = rootEntry.UpdatePreviousOffsetOnFile(idx.File, rootAddrU64, idx.FileSize)
			if err != nil {
				return err
			}

			// increment file size by the new entry's size
			idx.FileSize += uint64(newEntryLen)
			break
		}

		addr = entry.NextOffset
		if addr == rootAddrU64 || addr == 0 {
			// try to avoid looping forever in case of data corruption or something
			// The zero check is for data corruption
			break
		}
	}

	return nil
}

// writeEntryToFile writes a given entry to the file at the given address, returning the number of bytes written
func writeEntryToFile(file *os.File, addr uint64, entry *values.InvertedIndexEntry) (int, error) {
	entryAsBytes := entry.AsBytes()
	bytesWritten, err := file.WriteAt(entryAsBytes, int64(addr))
	if err != nil {
		return 0, err
	}

	return bytesWritten, nil
}

// readEntryBytes reads a byte array for an entry at the given address in a file.
// / It returns None if the data ended prematurely
func readEntryBytes(file *os.File, addr uint64) ([]byte, error) {
	address := int64(addr)
	sizeBuf := make([]byte, 4)
	bytesRead, err := file.ReadAt(sizeBuf, address)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	size, err := internal.Uint32FromByteArray(sizeBuf[:bytesRead])
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	bytesRead, err = file.ReadAt(buf, address)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return buf[:bytesRead], nil
}
