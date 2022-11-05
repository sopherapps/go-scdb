package scdb

import (
	"github.com/sopherapps/go-scbd/scdb/errors"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"github.com/sopherapps/go-scbd/scdb/internal/buffers"
	"github.com/sopherapps/go-scbd/scdb/internal/entries"
	"os"
	"path/filepath"
	"time"
)

const DefaultDbFile string = "dump.scdb"

type Store struct {
	bufferPool *buffers.BufferPool
	header     *entries.DbFileHeader
	//scheduler *TaskHandler
}

// New creates a new Store at the given path
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

	store := &Store{
		bufferPool: bufferPool,
		header:     header,
	}

	return store, nil
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
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

		kvOffset, err := internal.Uint64FromByteArray(kvOffsetInBytes)
		if err != nil {
			return nil, err
		}

		if kvOffset != 0 {
			value, err := s.bufferPool.GetValue(kvOffset, k)
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

func (s *Store) Delete(k []byte) error {
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

		if kvOffset != 0 {
			isOffsetForKey, err := s.bufferPool.TryDeleteKvEntry(kvOffset, k)
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
	return s.bufferPool.ClearFile()
}

func (s *Store) Compact() error {
	//TODO implement me
	panic("implement me")
}
