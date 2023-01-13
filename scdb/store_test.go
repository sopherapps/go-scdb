package scdb

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/internal/buffers"
	"github.com/stretchr/testify/assert"
	"log"
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

var SEARCH_RECORDS = []testRecord{
	{[]byte("foo"), []byte("eng")},
	{[]byte("fore"), []byte("span")},
	{[]byte("food"), []byte("lug")},
	{[]byte("bar"), []byte("port")},
	{[]byte("band"), []byte("nyoro")},
	{[]byte("pig"), []byte("dan")},
}

var SEARCH_TERMS = [][]byte{
	[]byte("f"),
	[]byte("fo"),
	[]byte("foo"),
	[]byte("for"),
	[]byte("b"),
	[]byte("ba"),
	[]byte("bar"),
	[]byte("ban"),
	[]byte("pigg"),
	[]byte("p"),
	[]byte("pi"),
	[]byte("pig"),
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

func TestStore_Search(t *testing.T) {
	dbPath := "testdb_search"
	removeStore(t, dbPath)
	store := createStore(t, dbPath, nil)
	defer func() {
		_ = store.Close()
		removeStore(t, dbPath)
	}()

	type testParams struct {
		term     []byte
		skip     uint64
		limit    uint64
		expected []buffers.KeyValuePair
	}

	t.Run("SearchWithoutPaginationReturnsAllMatchedKeyValues", func(t *testing.T) {
		table := []testParams{
			{[]byte("f"), 0, 0, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 0, 0, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("foo"), 0, 0, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("food"), 0, 0, []buffers.KeyValuePair{{[]byte("food"), []byte("lug")}}},
			{[]byte("for"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("b"), 0, 0, []buffers.KeyValuePair{{[]byte("bar"), []byte("port")}, {[]byte("band"), []byte("nyoro")}}},
			{[]byte("ba"), 0, 0, []buffers.KeyValuePair{{[]byte("bar"), []byte("port")}, {[]byte("band"), []byte("nyoro")}}},
			{[]byte("bar"), 0, 0, []buffers.KeyValuePair{{[]byte("bar"), []byte("port")}}},
			{[]byte("ban"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("band"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("p"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pi"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pig"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pigg"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bandana"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bare"), 0, 0, []buffers.KeyValuePair{}},
		}

		insertRecords(t, store, SEARCH_RECORDS, nil)
		for _, rec := range table {
			got, err := store.Search(rec.term, rec.skip, rec.limit)
			if err != nil {
				t.Fatalf("error searching: %s", err)
			}

			assert.Equal(t, rec.expected, got)
		}
	})

	t.Run("SearchWithPaginationSkipsSomeAndReturnsNotMoreThanLimit", func(t *testing.T) {
		table := []testParams{
			{[]byte("fo"), 0, 0, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 0, 8, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 1, 8, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 1, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 0, 2, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}, {[]byte("fore"), []byte("span")}}},
			{[]byte("fo"), 1, 2, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}, {[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 0, 1, []buffers.KeyValuePair{{[]byte("foo"), []byte("eng")}}},
			{[]byte("fo"), 2, 1, []buffers.KeyValuePair{{[]byte("food"), []byte("lug")}}},
			{[]byte("fo"), 1, 1, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
		}

		insertRecords(t, store, SEARCH_RECORDS, nil)
		for _, rec := range table {
			got, err := store.Search(rec.term, rec.skip, rec.limit)
			if err != nil {
				t.Fatalf("error searching: %s", err)
			}

			assert.Equal(t, rec.expected, got)
		}
	})

	t.Run("SearchAfterExpirationReturnsNoExpiredKeysValues", func(t *testing.T) {
		table := []testParams{
			{[]byte("f"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("fo"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("foo"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("for"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("b"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("ba"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("bar"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("ban"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("band"), 0, 0, []buffers.KeyValuePair{{[]byte("band"), []byte("nyoro")}}},
			{[]byte("p"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pi"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pig"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pigg"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("food"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bandana"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bare"), 0, 0, []buffers.KeyValuePair{}},
		}
		recordsToExpire := []testRecord{RECORDS[0], RECORDS[2], RECORDS[3]}
		ttl := uint64(1)
		insertRecords(t, store, SEARCH_RECORDS, nil)
		insertRecords(t, store, recordsToExpire, &ttl)

		// wait for some items to expire
		time.Sleep(2 * time.Second)
		for _, rec := range table {
			got, err := store.Search(rec.term, rec.skip, rec.limit)
			if err != nil {
				t.Fatalf("error searching: %s", err)
			}

			assert.Equal(t, rec.expected, got)
		}
	})

	t.Run("SearchAfterDeleteReturnsNoDeletedKeyValues", func(t *testing.T) {
		table := []testParams{
			{[]byte("f"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("fo"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("foo"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("for"), 0, 0, []buffers.KeyValuePair{{[]byte("fore"), []byte("span")}}},
			{[]byte("b"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("ba"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bar"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("ban"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("band"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("p"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pi"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pig"), 0, 0, []buffers.KeyValuePair{{[]byte("pig"), []byte("dan")}}},
			{[]byte("pigg"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("food"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bandana"), 0, 0, []buffers.KeyValuePair{}},
			{[]byte("bare"), 0, 0, []buffers.KeyValuePair{}},
		}
		keysToDelete := [][]byte{[]byte("foo"), []byte("food"), []byte("bar"), []byte("band")}

		insertRecords(t, store, SEARCH_RECORDS, nil)
		deleteRecords(t, store, keysToDelete)

		//for (term, expected) in test_data:
		for _, rec := range table {
			got, err := store.Search(rec.term, rec.skip, rec.limit)
			if err != nil {
				t.Fatalf("error searching: %s", err)
			}

			assert.Equal(t, rec.expected, got)
		}
	})

	t.Run("SearchAfterClearReturnsAnEmptyList", func(t *testing.T) {
		insertRecords(t, store, SEARCH_RECORDS, nil)
		err := store.Clear()
		if err != nil {
			t.Fatalf("error clearing: %s", err)
		}

		for _, term := range SEARCH_TERMS {
			got, err := store.Search(term, 0, 0)
			if err != nil {
				t.Fatalf("error searching: %s", err)
			}

			assert.Equal(t, []buffers.KeyValuePair{}, got)
		}
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

	insertRecordsForBenchmarks(b, store, RECORDS[:3], nil)
	insertRecordsForBenchmarks(b, store, RECORDS[3:], &ttl)
	deleteRecordsForBenchmarks(b, store, [][]byte{RECORDS[3].k})
	time.Sleep(2 * time.Second)

	b.Run("Compact", func(b *testing.B) {
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

func BenchmarkStore_SearchWithoutPagination(b *testing.B) {
	dbPath := "testdb_search"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	insertRecordsForBenchmarks(b, store, SEARCH_RECORDS, nil)

	for _, term := range SEARCH_TERMS {
		b.Run(fmt.Sprintf("Search (no pagination) %s", term), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.Search(term, 0, 0)
			}
		})
	}
}

func BenchmarkStore_SearchWithPagination(b *testing.B) {
	dbPath := "testdb_search"
	defer removeStoreForBenchmarks(b, dbPath)

	// Create new store
	store := createStoreForBenchmarks(b, dbPath, nil)
	defer func() {
		_ = store.Close()
	}()

	insertRecordsForBenchmarks(b, store, SEARCH_RECORDS, nil)

	for _, term := range SEARCH_TERMS {
		b.Run(fmt.Sprintf("Search (paginated) %s", term), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = store.Search(term, 1, 2)
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

func ExampleNew() {
	var maxKeys uint64 = 1_000_000
	var redundantBlocks uint16 = 1
	var poolCapacity uint64 = 10
	var compactionInterval uint32 = 1_800
	var maxIndexKeyLen uint32 = 3

	store, err := New(
		"testdb",
		&maxKeys,
		&redundantBlocks,
		&poolCapacity,
		&compactionInterval,
		&maxIndexKeyLen)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
	}()
}

func ExampleStore_Set() {
	store, err := New("testdb", nil, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
	}()

	err = store.Set([]byte("foo"), []byte("bar"), nil)
	if err != nil {
		log.Fatalf("error setting key value without ttl: %s", err)
	}

	ttl := uint64(3_600)
	err = store.Set([]byte("fake"), []byte("bear"), &ttl)
	if err != nil {
		log.Fatalf("error setting key value with ttl: %s", err)
	}
}

func ExampleStore_Get() {
	store, err := New("testdb", nil, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
	}()

	err = store.Set([]byte("foo"), []byte("bar"), nil)
	if err != nil {
		log.Fatalf("error setting key value: %s", err)
	}

	value, err := store.Get([]byte("foo"))
	if err != nil {
		log.Fatalf("error getting key: %s", err)
	}

	fmt.Printf("%s", value)
	// Output: bar
}

func ExampleStore_Search() {
	store, err := New("testdb", nil, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
	}()

	data := []buffers.KeyValuePair{
		{K: []byte("hi"), V: []byte("ooliyo")},
		{K: []byte("high"), V: []byte("haiguru")},
		{K: []byte("hind"), V: []byte("enyuma")},
		{K: []byte("hill"), V: []byte("akasozi")},
		{K: []byte("him"), V: []byte("ogwo")},
	}

	for _, rec := range data {
		err = store.Set(rec.K, rec.V, nil)
		if err != nil {
			log.Fatalf("error setting key value: %s", err)
		}
	}

	// without pagination
	kvs, err := store.Search([]byte("hi"), 0, 0)
	if err != nil {
		log.Fatalf("error searching 'hi': %s", err)
	}

	fmt.Printf("\nno pagination: %v", kvs)

	// with pagination: get last three
	kvs, err = store.Search([]byte("hi"), 2, 3)
	if err != nil {
		log.Fatalf("error searching (paginated) 'hi': %s", err)
	}

	fmt.Printf("\nskip 2, limit 3: %v", kvs)

	// Output:
	// no pagination: [hi: ooliyo high: haiguru hind: enyuma hill: akasozi him: ogwo]
	// skip 2, limit 3: [hind: enyuma hill: akasozi him: ogwo]
}

func ExampleStore_Delete() {
	store, err := New("testdb", nil, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
	}()

	err = store.Delete([]byte("foo"))
	if err != nil {
		log.Fatalf("error deleting key: %s", err)
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
	store, err := New(path, nil, nil, nil, compactionInterval, nil)
	if err != nil {
		t.Fatalf("error opening store: %s", err)
	}
	return store
}

// createStoreForBenchmarks is a utility to create a store at the given path
func createStoreForBenchmarks(b *testing.B, path string, compactionInterval *uint32) *Store {
	store, err := New(path, nil, nil, nil, compactionInterval, nil)
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
