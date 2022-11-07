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

type ActionType uint

const (
	setAction ActionType = iota
	getAction
	deleteAction
	clearAction
	compactAction
	closeAction
	getStoreInstance
)

type Action struct {
	Type     ActionType
	key      []byte
	value    []byte
	ttl      *uint64
	respChan chan ActionResult
}

type ActionResult struct {
	err   error
	value []byte
	store *innerStore
}

type Store struct {
	ch       chan Action
	isClosed bool
}

type innerStore struct {
	bufferPool *buffers.BufferPool
	header     *entries.DbFileHeader
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

	interval := 3_600 * time.Second
	if compactionInterval != nil {
		interval = time.Duration(*compactionInterval) * time.Second
	}

	actionCh := make(chan Action)

	go func(actionChannel chan Action) {
		store := &innerStore{
			bufferPool: bufferPool,
			header:     header,
		}

		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				_ = store.compact()
			case act := <-actionChannel:
				switch act.Type {
				case compactAction:
					err := store.compact()
					act.respChan <- ActionResult{err: err}
				case clearAction:
					err = store.clear()
					act.respChan <- ActionResult{err: err}
				case deleteAction:
					err = store.delete(act.key)
					act.respChan <- ActionResult{err: err}
				case getAction:
					v, err := store.get(act.key)
					act.respChan <- ActionResult{err: err, value: v}
				case setAction:
					err = store.set(act.key, act.value, act.ttl)
					act.respChan <- ActionResult{err: err}
				case getStoreInstance:
					act.respChan <- ActionResult{store: store}
				case closeAction:
					ticker.Stop()
					err = store.close()
					act.respChan <- ActionResult{err: err}
					return
				}
			}
		}
	}(actionCh)

	return &Store{
		ch: actionCh,
	}, nil
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *innerStore) set(k []byte, v []byte, ttl *uint64) error {
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
func (s *innerStore) get(k []byte) ([]byte, error) {
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

// Delete removes the key-value for the given key
func (s *innerStore) delete(k []byte) error {
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
func (s *innerStore) clear() error {
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
func (s *innerStore) compact() error {
	return s.bufferPool.CompactFile()
}

// Close frees up any resources occupied by store.
// After this, the store is unusable. You have to re-instantiate it or just run into
// some crazy errors
func (s *innerStore) close() error {
	err := s.bufferPool.Close()
	if err != nil {
		return err
	}

	s.header = nil
	return nil
}

///

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     setAction,
		key:      k,
		value:    v,
		ttl:      ttl,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.err
}

// Get returns the value corresponding to the given key
func (s *Store) Get(k []byte) ([]byte, error) {
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     getAction,
		key:      k,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.value, resp.err
}

// Delete removes the key-value for the given key
func (s *Store) Delete(k []byte) error {
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     deleteAction,
		key:      k,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.err
}

// Clear removes all data in the store
func (s *Store) Clear() error {
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     clearAction,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.err
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
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     compactAction,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.err
}

// Close frees up any resources occupied by store.
// After this, the store is unusable. You have to re-instantiate it or just run into
// some crazy errors
func (s *Store) Close() error {
	if s.isClosed {
		return nil
	}

	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     closeAction,
		respChan: respCh,
	}
	resp := <-respCh
	if resp.err != nil {
		return resp.err
	}

	close(s.ch)
	s.isClosed = true

	return nil
}

// getInnerStore returns the instance of the inner store
// to take a peek into it especially for tests
func (s *Store) getInnerStore() *innerStore {
	respCh := make(chan ActionResult)
	s.ch <- Action{
		Type:     getStoreInstance,
		respChan: respCh,
	}
	resp := <-respCh
	return resp.store
}
