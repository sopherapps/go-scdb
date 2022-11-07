package store

import (
	"github.com/sopherapps/go-scdb/scdb/errors"
	"github.com/sopherapps/go-scdb/scdb/internal"
	"github.com/sopherapps/go-scdb/scdb/internal/buffers"
	"github.com/sopherapps/go-scdb/scdb/internal/entries"
	"os"
	"path/filepath"
	"time"
)

// DefaultDbFile is the default name of the database file that contains all the key-value pairs
const DefaultDbFile string = "dump.scdb"

// Store is the actual store of the key-value pairs
// It contains the BufferPool which in turn interfaces with both the memory
// and the disk to keep, retrieve and delete key-value pairs
type Store struct {
	BufferPool         *buffers.BufferPool
	Header             *entries.DbFileHeader
	CompactionInterval time.Duration
	C                  chan Op
}

// NewStore creates a new Store at the given path, with the given `compactionInterval`
func NewStore(path string, maxKeys *uint64, redundantBlocks *uint16, poolCapacity *uint64, compactionInterval *uint32) (*Store, error) {
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

	return &Store{
		BufferPool:         bufferPool,
		Header:             header,
		CompactionInterval: interval,
		C:                  make(chan Op),
	}, nil
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

// Open opens the Store and starts receiving an Op's as sent on
// the Store.C channel.
// It also starts the background compaction task that runs every Store.CompactionInterval Duration
func (s *Store) Open() {
	ticker := time.NewTicker(s.CompactionInterval)
	for {
		select {
		case <-ticker.C:
			_ = s.Compact()
		case op := <-s.C:
			switch op.Type {
			case CompactOp:
				err := s.Compact()
				op.RespChan <- OpResult{Err: err}
			case ClearOp:
				err := s.Clear()
				op.RespChan <- OpResult{Err: err}
			case DeleteOp:
				err := s.Delete(op.Key)
				op.RespChan <- OpResult{Err: err}
			case GetOp:
				v, err := s.Get(op.Key)
				op.RespChan <- OpResult{Err: err, Value: v}
			case SetOp:
				err := s.Set(op.Key, op.Value, op.Ttl)
				op.RespChan <- OpResult{Err: err}
			case CloseOp:
				ticker.Stop()
				err := s.Close()
				op.RespChan <- OpResult{Err: err}
				return
			}
		}
	}
}
