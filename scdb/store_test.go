package scdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

type testRecord struct {
	k []byte
	v []byte
}

var RECORDS = []testRecord{
	{[]byte("hey"), []byte("English")},
	{[]byte("hi"), []byte("English")},
	{[]byte("salut"), []byte("French")},
	{[]byte("bonjour"), []byte("French")},
	{[]byte("hola"), []byte("Spanish")},
	{[]byte("oi"), []byte("Portuguese")},
	{[]byte("mulimuta"), []byte("Runyoro")},
}

func TestStore_Get(t *testing.T) {
	dbPath := "testdb_get"
	removeStore(t, dbPath)
	store := createStore(t, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()
	insertRecords(t, store, RECORDS, nil)

	t.Run("GetReturnsValueForGivenKey", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		assertStoreContains(t, store, RECORDS)
	})

	t.Run("GetReturnsNilForNonExistentKey", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		nonExistentKeys := [][]byte{[]byte("blue"), []byte("green"), []byte("red")}
		assertKeysDontExist(t, store, nonExistentKeys)
	})
}

func TestStore_Set(t *testing.T) {
	dbPath := "testdb_set"
	removeStore(t, dbPath)

	t.Run("SetWithoutTTLInsertsKeyValuesThatNeverExpire", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS, nil)
		assertStoreContains(t, store, RECORDS)
	})

	t.Run("SetWithTTLInsertsKeyValuesThatExpireAfterTTLSeconds", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		var ttl uint64 = 1

		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)

		time.Sleep(2 * time.Second)

		nonExistentKeys := extractKeysFromRecords(RECORDS[3:])
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, nonExistentKeys)
	})

	t.Run("SetAnExistingKeyUpdatesIt", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		updates := []testRecord{
			{[]byte("hey"), []byte("Jane")},
			{[]byte("hi"), []byte("John")},
			{[]byte("hola"), []byte("Santos")},
			{[]byte("oi"), []byte("Ronaldo")},
			{[]byte("mulimuta"), []byte("Aliguma")},
		}
		expected := []testRecord{
			{[]byte("hey"), []byte("Jane")},
			{[]byte("hi"), []byte("John")},
			{[]byte("salut"), []byte("French")},
			{[]byte("bonjour"), []byte("French")},
			{[]byte("hola"), []byte("Santos")},
			{[]byte("oi"), []byte("Ronaldo")},
			{[]byte("mulimuta"), []byte("Aliguma")},
		}

		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS, nil)
		insertRecords(t, store, updates, nil)
		assertStoreContains(t, store, expected)
	})

	t.Run("FileIsPersistedToAfterSet", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		func() {
			store := createStore(t, dbPath, nil)
			defer func() {
				_ = store.Close()
			}()
			insertRecords(t, store, RECORDS, nil)
		}()

		// the old store is expected to be garbage collected around here.
		runtime.GC()

		// Open another store
		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		assertStoreContains(t, store, RECORDS)
	})
}

func TestStore_Delete(t *testing.T) {
	dbPath := "testdb_delete"
	removeStore(t, dbPath)

	t.Run("DeleteRemovesKeyValuePair", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		keysToDelete := extractKeysFromRecords(RECORDS[3:])

		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS, nil)
		deleteRecords(t, store, keysToDelete)
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, keysToDelete)
	})

	t.Run("FileIsPersistedToAfterDelete", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		keysToDelete := extractKeysFromRecords(RECORDS[3:])

		func() {
			store := createStore(t, dbPath, nil)
			defer func() {
				_ = store.Close()
			}()
			insertRecords(t, store, RECORDS, nil)
			deleteRecords(t, store, keysToDelete)
		}()

		// the old store is expected to be garbage collected around here.
		runtime.GC()

		// open another store
		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, keysToDelete)
	})
}

func TestStore_Clear(t *testing.T) {
	dbPath := "testdb_clear"
	removeStore(t, dbPath)

	t.Run("ClearDeletesAllDataInStore", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS, nil)

		err := store.Clear()
		if err != nil {
			t.Fatalf("error clearing store: %s", err)
		}

		allKeys := extractKeysFromRecords(RECORDS)
		assertKeysDontExist(t, store, allKeys)
	})

	t.Run("FileIsPersistedToAfterClear", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		func() {
			store := createStore(t, dbPath, nil)
			defer func() {
				_ = store.Close()
			}()
			insertRecords(t, store, RECORDS, nil)
			err := store.Clear()
			if err != nil {
				t.Fatalf("error clearing store: %s", err)
			}
		}()

		// the old store is expected to be garbage collected around here.
		runtime.GC()

		// Create new store
		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		allKeys := extractKeysFromRecords(RECORDS)
		assertKeysDontExist(t, store, allKeys)
	})
}

func TestStore_Compact(t *testing.T) {
	dbPath := "testdb_compact"
	removeStore(t, dbPath)

	t.Run("CompactRemovesDanglingExpiredAndDeletedKeyValuePairsFromFile", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		var ttl uint64 = 1

		store := createStore(t, dbPath, nil)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)
		deleteRecords(t, store, [][]byte{RECORDS[2].k})

		initialFileSize := getFileSize(t, dbPath)

		time.Sleep(3 * time.Second)
		err := store.Compact()
		if err != nil {
			t.Fatalf("error compacting store: %s", err)
		}

		finalFileSize := getFileSize(t, dbPath)

		assert.Less(t, finalFileSize, initialFileSize)
		nonExistentKeys := extractKeysFromRecords(RECORDS[2:])
		assertStoreContains(t, store, RECORDS[:2])
		assertKeysDontExist(t, store, nonExistentKeys)
	})

	t.Run("BackgroundTaskCompactsAtCompactionInterval", func(t *testing.T) {
		defer func() {
			removeStore(t, dbPath)
		}()
		var ttl uint64 = 1
		var compactionInterval uint32 = 2

		store := createStore(t, dbPath, &compactionInterval)
		defer func() {
			_ = store.Close()
		}()
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)
		deleteRecords(t, store, [][]byte{RECORDS[2].k})

		initialFileSize := getFileSize(t, dbPath)

		time.Sleep(3 * time.Second)

		finalFileSize := getFileSize(t, dbPath)

		assert.Less(t, finalFileSize, initialFileSize)
		nonExistentKeys := extractKeysFromRecords(RECORDS[2:])
		assertStoreContains(t, store, RECORDS[:2])
		assertKeysDontExist(t, store, nonExistentKeys)
	})
}

func TestStore_Close(t *testing.T) {
	dbPath := "testdb_close"
	removeStore(t, dbPath)
	defer func() {
		removeStore(t, dbPath)
	}()

	var ttl uint64 = 1
	var compactionInterval uint32 = 2

	store := createStore(t, dbPath, &compactionInterval)
	defer func() {
		_ = store.Close()
	}()
	insertRecords(t, store, RECORDS[:3], nil)
	insertRecords(t, store, RECORDS[3:], &ttl)
	deleteRecords(t, store, [][]byte{RECORDS[2].k})

	err := store.Close()
	if err != nil {
		t.Fatalf("error closing store: %s", err)
	}

	initialFileSize := getFileSize(t, dbPath)

	time.Sleep(2 * time.Second)

	finalFileSize := getFileSize(t, dbPath)

	// no compaction done because background tasks have been stopped
	assert.Equal(t, initialFileSize, finalFileSize)

	assert.Nil(t, store.header)
	assert.True(t, store.isClosed)
	// already closed buffer pool will throw error
	assert.Error(t, store.bufferPool.Close())
}

func BenchmarkStore_Clear(b *testing.B) {
	dbPath := "testdb_clear"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	ttl := uint64(3_600)

	b.Run("Clear", func(b *testing.B) {
		insertRecordsForBenchmarks(b, store, RECORDS, nil)

		for i := 0; i < b.N; i++ {
			_ = store.Clear()
		}
	})

	b.Run(fmt.Sprintf("Clear with ttl: %d", ttl), func(b *testing.B) {
		insertRecordsForBenchmarks(b, store, RECORDS, nil)

		for i := 0; i < b.N; i++ {
			_ = store.Clear()
		}
	})
}

func BenchmarkStore_Compact(b *testing.B) {
	dbPath := "testdb_compact"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	ttl := uint64(1)

	b.Run("Compact", func(b *testing.B) {
		insertRecordsForBenchmarks(b, store, RECORDS[:3], nil)
		insertRecordsForBenchmarks(b, store, RECORDS[3:], &ttl)
		deleteRecordsForBenchmarks(b, store, [][]byte{RECORDS[3].k})
		time.Sleep(2 * time.Second)

		for i := 0; i < b.N; i++ {
			_ = store.Compact()
		}
	})
}

func BenchmarkStore_DeleteWithoutTtl(b *testing.B) {
	dbPath := "testdb_delete"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	insertRecordsForBenchmarks(b, store, RECORDS, nil)

	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Delete key %s", record.k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = store.Delete(record.k)
			}
		})
	}
}

func BenchmarkStore_DeleteWithTtl(b *testing.B) {
	dbPath := "testdb_delete"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	ttl := uint64(3_600)
	insertRecordsForBenchmarks(b, store, RECORDS, &ttl)

	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Delete key %s", record.k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = store.Delete(record.k)
			}
		})
	}
}

func BenchmarkStore_GetWithoutTtl(b *testing.B) {
	dbPath := "testdb_get"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	insertRecordsForBenchmarks(b, store, RECORDS, nil)

	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Get %s", record.k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.Get(record.k)
			}
		})
	}
}

func BenchmarkStore_GetWithTtl(b *testing.B) {
	dbPath := "testdb_get"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	ttl := uint64(3_600)

	insertRecordsForBenchmarks(b, store, RECORDS, &ttl)

	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Get %s", record.k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.Get(record.k)
			}
		})
	}
}

func BenchmarkStore_SetWithoutTtl(b *testing.B) {
	dbPath := "testdb_set"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()
	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Set %s %s", record.k, record.v), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = store.Set(record.k, record.v, nil)
			}
		})
	}

}

func BenchmarkStore_SetWithTtl(b *testing.B) {
	dbPath := "testdb_set"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	ttl := uint64(3_600)

	for _, record := range RECORDS {
		b.Run(fmt.Sprintf("Set %s %s", record.k, record.v), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = store.Set(record.k, record.v, &ttl)
			}
		})
	}

}

// removeStore is a utility to remove the old store just before a given test is run
func removeStore(t *testing.T, path string) {
	err := os.RemoveAll(path)
	if err != nil {
		t.Fatalf("error removing store: %s", err)
	}
}

// removeStoreForBenchmarks is a utility to remove the old store just before a given test is run
func removeStoreForBenchmarks(b *testing.B, path string) {
	err := os.RemoveAll(path)
	if err != nil {
		b.Fatalf("error removing store: %s", err)
	}
}

// createStore is a utility to create a store at the given path
func createStore(t *testing.T, path string, compactionInterval *uint32) *Store {
	store, err := New(path, nil, nil, nil, compactionInterval)
	if err != nil {
		t.Fatalf("error opening store: %s", err)
	}
	return store
}

// createStoreForBenchmarks is a utility to create a store at the given path
func createStoreForBenchmarks(b *testing.B, path string, compactionInterval *uint32) *Store {
	store, err := New(path, nil, nil, nil, compactionInterval)
	if err != nil {
		b.Fatalf("error opening store: %s", err)
	}
	return store
}

// insertRecords inserts the data into the store
func insertRecords(t *testing.T, store *Store, data []testRecord, ttl *uint64) {
	for _, record := range data {
		err := store.Set(record.k, record.v, ttl)
		if err != nil {
			t.Fatalf("error inserting key value: %s", err)
		}
	}
}

// insertRecordsForBenchmarks inserts the data into the store
func insertRecordsForBenchmarks(b *testing.B, store *Store, data []testRecord, ttl *uint64) {
	for _, record := range data {
		err := store.Set(record.k, record.v, ttl)
		if err != nil {
			b.Fatalf("error inserting key value: %s", err)
		}
	}
}

// deleteRecords deletes the given keys from the store
func deleteRecords(t *testing.T, store *Store, keys [][]byte) {
	for _, k := range keys {
		err := store.Delete(k)
		if err != nil {
			t.Fatalf("error deleting key: %s", err)
		}
	}
}

// deleteRecordsForBenchmarks deletes the given keys from the store
func deleteRecordsForBenchmarks(b *testing.B, store *Store, keys [][]byte) {
	for _, k := range keys {
		err := store.Delete(k)
		if err != nil {
			b.Fatalf("error deleting key: %s", err)
		}
	}
}

// extractKeysFromRecords extracts the keys in the given slice of testRecord
func extractKeysFromRecords(records []testRecord) [][]byte {
	keys := make([][]byte, len(records))
	for _, record := range records {
		keys = append(keys, record.k)
	}

	return keys
}

// getFileSize retrieves the size of a given file
func getFileSize(t *testing.T, dbPath string) int64 {
	filePath := path.Join(dbPath, "dump.scdb")
	stats, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("error getting file size: %s", err)
	}

	return stats.Size()
}

// assertStoreContains asserts that the store contains these given records
func assertStoreContains(t *testing.T, store *Store, records []testRecord) {
	for _, record := range records {
		got, err := store.Get(record.k)
		assert.Nil(t, err)
		assert.Equal(t, record.v, got)
	}
}

// assertKeysDontExist asserts that the keys don't exist in the store
func assertKeysDontExist(t *testing.T, store *Store, keys [][]byte) {
	for _, k := range keys {
		got, err := store.Get(k)
		assert.Nil(t, err)
		assert.Nil(t, got)
	}
}
