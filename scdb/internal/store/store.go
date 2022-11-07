package store

import (
	"github.com/sopherapps/go-scbd/scdb/errors"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"github.com/sopherapps/go-scbd/scdb/internal/buffers"
	"github.com/sopherapps/go-scbd/scdb/internal/entries"
	"time"
)

// Store is the actual store of the key-value pairs
// It contains the BufferPool which in turn interfaces with both the memory
// and the disk to keep, retrieve and delete key-value pairs
type Store struct {
	BufferPool *buffers.BufferPool
	Header     *entries.DbFileHeader
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	expiry := uint64(0)
	if ttl != nil {
		expiry = uint64(time.Now().Unix()) + *ttl
	}

	initialIdxOffset := s.Header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.Header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.Header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
		if err != nil {
			return err
		}

		kvOffsetInBytes, err := s.BufferPool.ReadIndex(indexOffset)
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
			isOffsetForKey, err = s.BufferPool.AddrBelongsToKey(kvOffset, k)
			if err != nil {
				return err
			}
		}

		if isOffsetForKey {
			kv := entries.NewKeyValueEntry(k, v, expiry)
			prevLastOffset, err := s.BufferPool.Append(kv.AsBytes())
			if err != nil {
				return err
			}
			return s.BufferPool.UpdateIndex(indexOffset, internal.Uint64ToByteArray(prevLastOffset))
		}

	}

	return errors.NewErrCollisionSaturation(k)
}

// Get returns the value corresponding to the given key
func (s *Store) Get(k []byte) ([]byte, error) {
	initialIdxOffset := s.Header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.Header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.Header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
		if err != nil {
			return nil, err
		}

		kvOffsetInBytes, err := s.BufferPool.ReadIndex(indexOffset)
		if err != nil {
			return nil, err
		}

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return nil, err
		}

		if kvOffset != 0 {
			value, err := s.BufferPool.GetValue(kvOffset, k)
			if err != nil {
				return nil, err
			}

			if value.IsStale {
				return nil, nil
			} else {
				return value.Data, nil
			}
		}
	}

	return nil, nil
}

// Delete removes the key-value for the given key
func (s *Store) Delete(k []byte) error {
	initialIdxOffset := s.Header.GetIndexOffset(k)

	for idxBlock := uint64(0); idxBlock < s.Header.NumberOfIndexBlocks; idxBlock++ {
		indexOffset, err := s.Header.GetIndexOffsetInNthBlock(initialIdxOffset, idxBlock)
		if err != nil {
			return err
		}

		kvOffsetInBytes, err := s.BufferPool.ReadIndex(indexOffset)
		if err != nil {
			return err
		}

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return err
		}

		if kvOffset != 0 {
			isOffsetForKey, err := s.BufferPool.TryDeleteKvEntry(kvOffset, k)
			if err != nil {
				return err
			}

			if isOffsetForKey {
				return nil
			} // else continue looping
		}
	}

	return nil // if it is not found, no error is thrown
}

// Clear removes all data in the store
func (s *Store) Clear() error {
	return s.BufferPool.ClearFile()
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
	return s.BufferPool.CompactFile()
}

// Close frees up any resources occupied by store.
// After this, the store is unusable. You have to re-instantiate it or just run into
// some crazy errors
func (s *Store) Close() error {
	err := s.BufferPool.Close()
	if err != nil {
		return err
	}

	s.Header = nil
	return nil
}
