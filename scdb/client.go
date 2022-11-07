package scdb

import (
	"github.com/sopherapps/go-scbd/scdb/internal/store"
)

// Store is the public interface to the key-value store
// that allows us to do operations like Set, Get, Delete, Clear and Compact
// on the internal.Store
type Store struct {
	store    *store.Store
	isClosed bool
}

// New creates a new Store at the given path
func New(path string, maxKeys *uint64, redundantBlocks *uint16, poolCapacity *uint64, compactionInterval *uint32) (*Store, error) {
	s, err := store.NewStore(path, maxKeys, redundantBlocks, poolCapacity, compactionInterval)
	if err != nil {
		return nil, err
	}

	go s.Open()
	return &Store{store: s}, nil
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.SetOp,
		Key:      k,
		Value:    v,
		Ttl:      ttl,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Err
}

// Get returns the value corresponding to the given key
func (s *Store) Get(k []byte) ([]byte, error) {
	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.GetOp,
		Key:      k,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Value, resp.Err
}

// Delete removes the key-value for the given key
func (s *Store) Delete(k []byte) error {
	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.DeleteOp,
		Key:      k,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Err
}

// Clear removes all data in the store
func (s *Store) Clear() error {
	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.ClearOp,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Err
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
	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.CompactOp,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Err
}

// Close frees up any resources occupied by store.
// After this, the store is unusable. You have to re-instantiate it or just run into
// some crazy errors
func (s *Store) Close() error {
	if s.isClosed {
		return nil
	}

	respCh := make(chan store.OpResult)
	s.store.C <- store.Op{
		Type:     store.CloseOp,
		RespChan: respCh,
	}
	resp := <-respCh
	if resp.Err != nil {
		return resp.Err
	}

	close(s.store.C)
	s.isClosed = true

	return nil
}
