package scdb

import (
	"bytes"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/buffers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/headers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries/values"
	"github.com/sopherapps/go-scdb/scdb/internal/inverted_index"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// defaultDbFile is the default name of the database file that contains all the key-value pairs
const defaultDbFile string = "dump.scdb"

// defaultSearchIndexFile is the default name of the inverted index file that is for doing full-text search
const defaultSearchIndexFile string = "index.iscdb"

var zeroU64 = internal.Uint64ToByteArray(0)

// Store is a key-value store that persists key-value pairs to disk
//
// Store behaves like a HashMap that saves keys and value as byte arrays
// on disk. It allows for specifying how long each key-value pair should be
// kept for i.e. the time-to-live in seconds. If None is provided, they last indefinitely.
type Store struct {
	bufferPool  *buffers.BufferPool
	header      *headers.DbFileHeader
	searchIndex *inverted_index.InvertedIndex
	closeCh     chan bool
	mu          sync.Mutex
	isClosed    bool
}

// New creates a new Store at the given path
// The Store has a number of configurations that are passed into this New function
//
//   - `storePath` - required:
//     The path to a directory where scdb should store its data
//
//   - `maxKeys` - default: 1 million:
//     The maximum number of key-value pairs to store in store
//
//   - `redundantBlocks` - default: 1:
//     The store has an index to hold all the keys. This index is split
//     into a fixed number of blocks basing on the virtual memory page size
//     and the total number of keys to be held i.e. `max_keys`.
//     Sometimes, there may be hash collision errors as the store's
//     current stored keys approach `max_keys`. The closer it gets, the
//     more it becomes likely see those errors. Adding redundant blocks
//     helps mitigate this. Just be careful to not add too many (i.e. more than 2)
//     since the higher the number of these blocks, the slower the store becomes.
//
//   - `poolCapacity` - default: 5:
//     The number of buffers to hold in memory as cache's for the store. Each buffer
//     has the size equal to the virtual memory's page size, usually 4096 bytes.
//     Increasing this number will speed this store up but of course, the machine
//     has a limited RAM. When this number increases to a value that clogs the RAM, performance
//     suddenly degrades, and keeps getting worse from there on.
//
//   - `compactionInterval` - default 3600s (1 hour):
//     The interval at which the store is compacted to remove dangling
//     keys. Dangling keys result from either getting expired or being deleted.
//     When a `delete` operation is done, the actual key-value pair
//     is just marked as `deleted` but is not removed.
//     Something similar happens when a key-value is updated.
//     A new key-value pair is created and the old one is left unindexed.
//     Compaction is important because it reclaims this space and reduces the size
//     of the database file.
//
//   - `maxIndexKeyLen` - default 3:
//     The maximum number of characters in each key in the search inverted index
//     The inverted index is used for full-text search of keys to get all key-values
//     whose keys start with a given byte array.
func New(path string, maxKeys *uint64, redundantBlocks *uint16, poolCapacity *uint64, compactionInterval *uint32, maxIndexKeyLen *uint32) (*Store, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	dbFilePath := filepath.Join(path, defaultDbFile)
	bufferPool, err := buffers.NewBufferPool(poolCapacity, dbFilePath, maxKeys, redundantBlocks, nil)
	if err != nil {
		return nil, err
	}

	header, err := headers.ExtractDbFileHeaderFromFile(bufferPool.File)
	if err != nil {
		return nil, err
	}

	searchIndexFilePath := filepath.Join(path, defaultSearchIndexFile)
	searchIndex, err := inverted_index.NewInvertedIndex(searchIndexFilePath, maxIndexKeyLen, maxKeys, redundantBlocks)
	if err != nil {
		return nil, err
	}

	interval := 3_600 * time.Second
	if compactionInterval != nil {
		interval = time.Duration(*compactionInterval) * time.Second
	}

	store := &Store{
		bufferPool:  bufferPool,
		header:      header,
		searchIndex: searchIndex,
		closeCh:     make(chan bool),
	}

	go store.startBackgroundCompaction(interval)

	return store, nil
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiry := uint64(0)
	if ttl != nil {
		expiry = uint64(time.Now().Unix()) + *ttl
	}

	initialIdxOffset := headers.GetIndexOffset(s.header, k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := headers.GetIndexOffsetInNthBlock(s.header, initialIdxOffset, idxBlock)
		if err != nil {
			return err
		}

		kvOffsetInBytes, err := s.bufferPool.ReadIndex(indexOffset)
		if err != nil {
			return err
		}

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return err
		}

		// the offset is for the key if the key is not filled - thus new insert
		isOffsetForKey := kvOffset == 0

		// the offset could also be for this key if the key in file matches the key supplied - thus update
		if !isOffsetForKey {
			isOffsetForKey, err = s.bufferPool.AddrBelongsToKey(kvOffset, k)
			if err != nil {
				return err
			}
		}

		if isOffsetForKey {
			kv := values.NewKeyValueEntry(k, v, expiry)
			prevLastOffset, err := s.bufferPool.Append(kv.AsBytes())
			if err != nil {
				return err
			}

			err = s.bufferPool.UpdateIndex(indexOffset, internal.Uint64ToByteArray(prevLastOffset))
			if err != nil {
				return err
			}

			// Update the search index
			return s.searchIndex.Add(k, prevLastOffset, expiry)
		}

	}

	return errors.NewErrCollisionSaturation(k)
}

// Get returns the value corresponding to the given key
func (s *Store) Get(k []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	initialIdxOffset := headers.GetIndexOffset(s.header, k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := headers.GetIndexOffsetInNthBlock(s.header, initialIdxOffset, idxBlock)
		if err != nil {
			return nil, err
		}

		kvOffsetInBytes, err := s.bufferPool.ReadIndex(indexOffset)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(kvOffsetInBytes, zeroU64) {
			continue
		}

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return nil, err
		}

		value, err := s.bufferPool.GetValue(kvOffset, k)
		if err != nil {
			return nil, err
		}

		if value != nil {
			return value.Value, nil
		}
	}

	return nil, nil
}

// Search searches for unexpired keys that start with the given search term
//
// It skips the first `skip` (default: 0) number of results and returns not more than
// `limit` (default: 0) number of items. This is to avoid using up more memory than can be handled by the
// host machine.
//
// If `limit` is 0, all items are returned since it would make no sense for someone to search
// for zero items.
//
// returns a list of pairs of key-value i.e. `buffers.KeyValuePair`
func (s *Store) Search(term []byte, skip uint64, limit uint64) ([]buffers.KeyValuePair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	addrs, err := s.searchIndex.Search(term, skip, limit)
	if err != nil {
		return nil, err
	}

	return s.bufferPool.GetManyKeyValues(addrs)
}

// Delete removes the key-value for the given key
func (s *Store) Delete(k []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	initialIdxOffset := headers.GetIndexOffset(s.header, k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := headers.GetIndexOffsetInNthBlock(s.header, initialIdxOffset, idxBlock)
		if err != nil {
			return err
		}

		kvOffsetInBytes, err := s.bufferPool.ReadIndex(indexOffset)
		if err != nil {
			return err
		}

		if bytes.Equal(kvOffsetInBytes, zeroU64) {
			continue
		}

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return err
		}

		isOffsetForKey, err := s.bufferPool.TryDeleteKvEntry(kvOffset, k)
		if err != nil {
			return err
		}

		if isOffsetForKey {
			return nil
		} // else continue looping

	}
	// if it is not found, no error is thrown

	return s.searchIndex.Remove(k)
}

// Clear removes all data in the store
func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.bufferPool.ClearFile()
	if err != nil {
		return err
	}

	return s.searchIndex.Clear()
}

// Compact manually removes dangling key-value pairs in the database file
//
// Dangling keys result from either getting expired or being deleted.
// When a Store.Delete operation is done, the actual key-value pair
// is just marked as `deleted` but is not removed.
//
// Something similar happens when a key-value is updated.
// A new key-value pair is created and the old one is left un-indexed.
// Compaction is important because it reclaims this space and reduces the size
// of the database file.
//
// This is done automatically for you at the set `compactionInterval` but you
// may wish to do it manually for some reason.
//
// This is a very expensive operation so use it sparingly.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bufferPool.CompactFile(s.searchIndex)
}

// Close frees up any resources occupied by store.
// After this, the store is unusable. You have to re-instantiate it or just run into
// some crazy errors
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return nil
	}

	s.closeCh <- true
	close(s.closeCh)
	s.isClosed = true

	err := s.bufferPool.Close()
	if err != nil {
		return err
	}

	err = s.searchIndex.Close()
	if err != nil {
		return err
	}

	s.header = nil
	s.searchIndex = nil

	return nil
}

// startBackgroundCompaction starts the background compaction task that runs every `interval`
func (s *Store) startBackgroundCompaction(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			_ = s.Compact()
		case <-s.closeCh:
			ticker.Stop()
			return
		}
	}
}
