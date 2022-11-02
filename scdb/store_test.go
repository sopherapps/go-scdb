package scdb

import (
	"fmt"
	"os"
	"path"
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
	store := createStore(t, dbPath, nil)
	clearStore(t, store)
	insertRecords(t, store, RECORDS, nil)

	t.Run("GetReturnsValueForGivenKey", func(t *testing.T) {
		assertStoreContains(t, store, RECORDS)
	})

	t.Run("GetReturnsNilForNonExistentKey", func(t *testing.T) {
		nonExistentKeys := [][]byte{[]byte("blue"), []byte("green"), []byte("red")}
		assertKeysDontExist(t, store, nonExistentKeys)
	})
}

func TestStore_Set(t *testing.T) {
	dbPath := "testdb_set"

	t.Run("SetWithoutTTLInsertsKeyValuesThatNeverExpire", func(t *testing.T) {
		store := createStore(t, dbPath, nil)
		clearStore(t, store)
		insertRecords(t, store, RECORDS, nil)
		assertStoreContains(t, store, RECORDS)
	})

	t.Run("SetWithTTLInsertsKeyValuesThatExpireAfterTTLSeconds", func(t *testing.T) {
		var ttl uint64 = 1

		store := createStore(t, dbPath, nil)
		clearStore(t, store)
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)

		time.Sleep(2 * time.Second)

		nonExistentKeys := extractKeysFromRecords(RECORDS[3:])
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, nonExistentKeys)
	})

	t.Run("SetAnExistingKeyUpdatesIt", func(t *testing.T) {
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
		clearStore(t, store)
		insertRecords(t, store, RECORDS, nil)
		insertRecords(t, store, updates, nil)
		assertStoreContains(t, store, expected)
	})

	t.Run("FileIsPersistedToAfterSet", func(t *testing.T) {
		func() {
			store := createStore(t, dbPath, nil)
			clearStore(t, store)
			insertRecords(t, store, RECORDS, nil)
			// the store at this point is garbage collected
		}()

		// Open another store
		store := createStore(t, dbPath, nil)
		assertStoreContains(t, store, RECORDS)
	})
}

func TestStore_Delete(t *testing.T) {
	dbPath := "testdb_delete"

	t.Run("DeleteRemovesKeyValuePair", func(t *testing.T) {
		keysToDelete := extractKeysFromRecords(RECORDS[3:])

		store := createStore(t, dbPath, nil)
		clearStore(t, store)
		insertRecords(t, store, RECORDS, nil)
		deleteRecords(t, store, keysToDelete)
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, keysToDelete)
	})

	t.Run("FileIsPersistedToAfterDelete", func(t *testing.T) {
		keysToDelete := extractKeysFromRecords(RECORDS[3:])

		func() {
			store := createStore(t, dbPath, nil)
			clearStore(t, store)

			insertRecords(t, store, RECORDS, nil)
			deleteRecords(t, store, keysToDelete)
			// the store is expected to be garbage collected around here.
		}()

		// open another store
		store := createStore(t, dbPath, nil)
		assertStoreContains(t, store, RECORDS[:3])
		assertKeysDontExist(t, store, keysToDelete)
	})
}

func TestStore_Clear(t *testing.T) {
	dbPath := "testdb_clear"

	t.Run("ClearDeletesAllDataInStore", func(t *testing.T) {
		store := createStore(t, dbPath, nil)
		insertRecords(t, store, RECORDS, nil)

		err := store.Clear()
		if err != nil {
			t.Fatalf("error clearing store: %s", err)
		}

		allKeys := extractKeysFromRecords(RECORDS)
		assertKeysDontExist(t, store, allKeys)
	})

	t.Run("FileIsPersistedToAfterClear", func(t *testing.T) {
		func() {
			store := createStore(t, dbPath, nil)
			insertRecords(t, store, RECORDS, nil)
			err := store.Clear()
			if err != nil {
				t.Fatalf("error clearing store: %s", err)
			}
			// the store is expected to be garbage collected around here.
		}()

		// Create new store
		store := createStore(t, dbPath, nil)
		allKeys := extractKeysFromRecords(RECORDS)
		assertKeysDontExist(t, store, allKeys)
	})
}

func TestStore_Compact(t *testing.T) {
	dbPath := "testdb_compact"

	t.Run("CompactRemovesDanglingExpiredAndDeletedKeyValuePairsFromFile", func(t *testing.T) {
		var ttl uint64 = 1

		store := createStore(t, dbPath, nil)
		clearStore(t, store)
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)
		deleteRecords(t, store, [][]byte{RECORDS[2].k})

		initialFileSize := getFileSize(t, dbPath)

		time.Sleep(2 * time.Second)
		err := store.Compact()
		if err != nil {
			t.Fatalf("error compacting store: %s", err)
		}

		finalFileSize := getFileSize(t, dbPath)

		if finalFileSize >= initialFileSize {
			t.Errorf("final file size %v should be less than initial file size %v", finalFileSize, initialFileSize)
		}

		nonExistentKeys := extractKeysFromRecords(RECORDS[2:])
		assertStoreContains(t, store, RECORDS[:2])
		assertKeysDontExist(t, store, nonExistentKeys)
	})

	t.Run("BackgroundTaskCompactsAtCompactionInterval", func(t *testing.T) {
		var ttl uint64 = 1
		var compactionInterval uint32 = 2

		store := createStore(t, dbPath, &compactionInterval)
		clearStore(t, store)
		insertRecords(t, store, RECORDS[:3], nil)
		insertRecords(t, store, RECORDS[3:], &ttl)
		deleteRecords(t, store, [][]byte{RECORDS[2].k})

		initialFileSize := getFileSize(t, dbPath)

		time.Sleep(2 * time.Second)

		finalFileSize := getFileSize(t, dbPath)

		if finalFileSize >= initialFileSize {
			t.Errorf("final file size %v should be less than initial file size %v", finalFileSize, initialFileSize)
		}

		nonExistentKeys := extractKeysFromRecords(RECORDS[2:])
		assertStoreContains(t, store, RECORDS[:2])
		assertKeysDontExist(t, store, nonExistentKeys)
	})
}

// clearStore is a utility to clear the store usually just before a given test is run
func clearStore(t *testing.T, store *Store) {
	err := store.Clear()
	if err != nil {
		t.Fatalf("error clearing store: %s", err)
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

// insertRecords inserts the data into the store
func insertRecords(t *testing.T, store *Store, data []testRecord, ttl *uint64) {
	for _, record := range data {
		err := store.Set(record.k, record.v, ttl)
		if err != nil {
			t.Fatalf("error inserting without ttl: %s", err)
		}
	}
}

// deleteRecords deletes the given keys from the store
func deleteRecords(t *testing.T, store *Store, keys [][]byte) {
	for _, k := range keys {
		err := store.Delete(k)
		if err != nil {
			t.Fatalf("error inserting without ttl: %s", err)
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

// areByteArraysEqual checks whether two byte arrays are equal
func areByteArraysEqual(first []byte, second []byte) bool {
	return fmt.Sprintf("%v", first) == fmt.Sprintf("%v", second)
}

// assertStoreContains asserts that the store contains these given records
func assertStoreContains(t *testing.T, store *Store, records []testRecord) {
	for _, record := range records {
		got, err := store.Get(record.k)
		if err != nil {
			t.Fatalf("error retrieving key for %s: %s", record.k, err)
		}

		if !areByteArraysEqual(got, record.v) {
			t.Errorf("value for key '%s', expected: %v, got: %v", record.k, record.v, got)
		}
	}
}

// assertKeysDontExist asserts that the keys don't exist in the store
func assertKeysDontExist(t *testing.T, store *Store, keys [][]byte) {
	for _, k := range keys {
		got, err := store.Get(k)
		if err != nil {
			t.Fatalf("error retrieving key for %s: %s", k, err)
		}

		if got != nil {
			t.Errorf("value for key '%s', expected: %v, got: %v", k, nil, got)
		}
	}
}
