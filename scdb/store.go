package scdb

import (
	"bytes"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/buffers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultDbFile is the default name of the database file that contains all the key-value pairs
const DefaultDbFile string = "dump.scdb"

var ZeroU64 = internal.Uint64ToByteArray(0)

// Store is the public interface to the key-value store
// that allows us to do operations like Set, Get, Delete, Clear and Compact
type Store struct {
	bufferPool *buffers.BufferPool
	header     *entries.DbFileHeader
	closeCh    chan bool
	mu         sync.Mutex
	isClosed   bool
}

// New creates a new Store at the given path, with the given `compactionInterval`
func New(path string, maxKeys *uint64, redundantBlocks *uint16, poolCapacity *uint64, compactionInterval *uint32) (*Store, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	dbFilePath := filepath.Join(path, DefaultDbFile)
	bufferPool, err := buffers.NewBufferPool(poolCapacity, dbFilePath, maxKeys, redundantBlocks, nil)
	if err != nil {
		return nil, err
	}

	header, err := entries.ExtractDbFileHeaderFromFile(bufferPool.File)
	if err != nil {
		return nil, err
	}

	interval := 3_600 * time.Second
	if compactionInterval != nil {
		interval = time.Duration(*compactionInterval) * time.Second
	}

	store := &Store{
		bufferPool: bufferPool,
		header:     header,
		closeCh:    make(chan bool),
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

	initialIdxOffset := s.header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
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
			kv := entries.NewKeyValueEntry(k, v, expiry)
			prevLastOffset, err := s.bufferPool.Append(kv.AsBytes())
			if err != nil {
				return err
			}
			return s.bufferPool.UpdateIndex(indexOffset, internal.Uint64ToByteArray(prevLastOffset))
		}

	}

	return errors.NewErrCollisionSaturation(k)
}

// Get returns the value corresponding to the given key
func (s *Store) Get(k []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	initialIdxOffset := s.header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
		if err != nil {
			return nil, err
		}

		kvOffsetInBytes, err := s.bufferPool.ReadIndex(indexOffset)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(kvOffsetInBytes, ZeroU64) {
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

// Delete removes the key-value for the given key
func (s *Store) Delete(k []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	initialIdxOffset := s.header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
		if err != nil {
			return err
		}

		kvOffsetInBytes, err := s.bufferPool.ReadIndex(indexOffset)
		if err != nil {
			return err
		}

		if bytes.Equal(kvOffsetInBytes, ZeroU64) {
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

	return nil // if it is not found, no error is thrown
}

// Clear removes all data in the store
func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bufferPool.ClearFile()
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

	return s.bufferPool.CompactFile()
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

	s.header = nil

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
