package scdb

import (
	"github.com/sopherapps/go-scbd/scdb/internal/buffers"
	"github.com/sopherapps/go-scbd/scdb/internal/entries"
	"github.com/sopherapps/go-scbd/scdb/internal/store"
	"os"
	"path/filepath"
	"time"
)

// DefaultDbFile is the default name of the database file that contains all the key-value pairs
const DefaultDbFile string = "dump.scdb"

// Store is the public interface to the key-value store
// that allows us to do operations like Set, Get, Delete, Clear and Compact
// on the internal.Store
type Store struct {
	ch       chan store.Op
	isClosed bool
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

	actionCh := make(chan store.Op)
	s := &store.Store{
		BufferPool: bufferPool,
		Header:     header,
	}

	go openStore(s, actionCh, interval)

	return &Store{
		ch: actionCh,
	}, nil
}

// openStore opens the internal.Store and starts receiving an internal.Op's as sent on
// the `opCh` channel. It also starts the background compaction task that runs every `compactionInterval` Duration
func openStore(s *store.Store, opCh chan store.Op, compactionInterval time.Duration) {
	ticker := time.NewTicker(compactionInterval)
	for {
		select {
		case <-ticker.C:
			_ = s.Compact()
		case op := <-opCh:
			switch op.Type {
			case store.CompactOp:
				err := s.Compact()
				op.RespChan <- store.OpResult{Err: err}
			case store.ClearOp:
				err := s.Clear()
				op.RespChan <- store.OpResult{Err: err}
			case store.DeleteOp:
				err := s.Delete(op.Key)
				op.RespChan <- store.OpResult{Err: err}
			case store.GetOp:
				v, err := s.Get(op.Key)
				op.RespChan <- store.OpResult{Err: err, Value: v}
			case store.SetOp:
				err := s.Set(op.Key, op.Value, op.Ttl)
				op.RespChan <- store.OpResult{Err: err}
			case store.GetStoreOp:
				op.RespChan <- store.OpResult{Store: s}
			case store.CloseOp:
				ticker.Stop()
				err := s.Close()
				op.RespChan <- store.OpResult{Err: err}
				return
			}
		}
	}
}

// Set sets the given key value in the store
// This is used to insert or update any key-value pair in the store
func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	respCh := make(chan store.OpResult)
	s.ch <- store.Op{
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
	s.ch <- store.Op{
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
	s.ch <- store.Op{
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
	s.ch <- store.Op{
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
	s.ch <- store.Op{
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
	s.ch <- store.Op{
		Type:     store.CloseOp,
		RespChan: respCh,
	}
	resp := <-respCh
	if resp.Err != nil {
		return resp.Err
	}

	close(s.ch)
	s.isClosed = true

	return nil
}

// getInnerStore returns the instance of the inner store
// to take a peek into it especially for tests
func (s *Store) getInnerStore() *store.Store {
	respCh := make(chan store.OpResult)
	s.ch <- store.Op{
		Type:     store.GetStoreOp,
		RespChan: respCh,
	}
	resp := <-respCh
	return resp.Store
}
