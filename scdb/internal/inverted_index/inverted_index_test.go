package inverted_index

import (
	"github.com/sopherapps/go-scdb/scdb/internal/entries/headers"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

type testSearchParams struct {
	term     []byte
	skip     uint64
	limit    uint64
	expected []uint64
}

type testAddParams struct {
	k      []byte
	addr   uint64
	expiry uint64
}

func TestNewInvertedIndex(t *testing.T) {
	fileName := "testdb.iscdb"
	testMaxKeys := uint64(360)
	testMaxIndexKeyLen := uint32(10)
	testRedundantBlocks := uint16(4)
	defer func() {
		_ = os.Remove(fileName)
	}()

	t.Run("NewInvertedIndexForNonExistingFile", func(t *testing.T) {
		type expectedRecord struct {
			maxIndexKeyLen   uint32
			valuesStartPoint uint64
			filePath         string
			fileSize         uint64
		}
		type testRecord struct {
			maxIndexKeyLen  *uint32
			filePath        string
			maxKeys         *uint64
			redundantBlocks *uint16
			expected        expectedRecord
		}

		testData := []testRecord{
			{nil, fileName, nil, nil, expectedRecord{
				maxIndexKeyLen:   headers.DefaultMaxIndexKeyLen,
				valuesStartPoint: headers.NewInvertedIndexHeader(nil, nil, nil, nil).ValuesStartPoint,
				filePath:         fileName,
				fileSize:         headers.NewInvertedIndexHeader(nil, nil, nil, nil).ValuesStartPoint,
			}},
			{&testMaxIndexKeyLen, fileName, nil, nil, expectedRecord{
				maxIndexKeyLen:   testMaxIndexKeyLen,
				valuesStartPoint: headers.NewInvertedIndexHeader(nil, nil, nil, &testMaxIndexKeyLen).ValuesStartPoint,
				filePath:         fileName,
				fileSize:         headers.NewInvertedIndexHeader(nil, nil, nil, &testMaxIndexKeyLen).ValuesStartPoint,
			}},
			{nil, fileName, &testMaxKeys, nil, expectedRecord{
				maxIndexKeyLen:   headers.DefaultMaxIndexKeyLen,
				valuesStartPoint: headers.NewInvertedIndexHeader(&testMaxKeys, nil, nil, nil).ValuesStartPoint,
				filePath:         fileName,
				fileSize:         headers.NewInvertedIndexHeader(&testMaxKeys, nil, nil, nil).ValuesStartPoint,
			}},
			{nil, fileName, nil, &testRedundantBlocks, expectedRecord{
				maxIndexKeyLen:   headers.DefaultMaxIndexKeyLen,
				valuesStartPoint: headers.NewInvertedIndexHeader(nil, &testRedundantBlocks, nil, nil).ValuesStartPoint,
				filePath:         fileName,
				fileSize:         headers.NewInvertedIndexHeader(nil, &testRedundantBlocks, nil, nil).ValuesStartPoint,
			}},
		}

		// delete the file so that BufferPool::new() can reinitialize it.
		_ = os.Remove(fileName)

		for _, record := range testData {
			got, err := NewInvertedIndex(record.filePath, record.maxIndexKeyLen, record.maxKeys, record.redundantBlocks)
			if err != nil {
				t.Fatalf("error creating new inverted index: %s", err)
			}

			assert.Equal(t, record.expected.maxIndexKeyLen, got.MaxIndexKeyLen)
			assert.Equal(t, record.expected.valuesStartPoint, got.ValuesStartPoint)
			assert.Equal(t, record.expected.filePath, got.FilePath)
			assert.Equal(t, record.expected.fileSize, got.FileSize)

			err = os.Remove(got.FilePath)
			if err != nil {
				t.Fatalf("error removing inverted index file: %s", got.FilePath)
			}
		}
	})

	t.Run("NewInvertedIndexForExistingFile", func(t *testing.T) {
		type testRecord struct {
			filePath        string
			maxIndexKeyLen  *uint32
			maxKeys         *uint64
			redundantBlocks *uint16
		}

		testData := []testRecord{
			{fileName, nil, nil, nil},
			{fileName, &testMaxIndexKeyLen, nil, nil},
			{fileName, nil, &testMaxKeys, nil},
			{fileName, nil, nil, &testRedundantBlocks},
		}

		for _, record := range testData {
			first, err := NewInvertedIndex(record.filePath, record.maxIndexKeyLen, record.maxKeys, record.redundantBlocks)
			if err != nil {
				t.Fatalf("error creating new inverted index: %s", err)
			}

			second, err := NewInvertedIndex(record.filePath, record.maxIndexKeyLen, record.maxKeys, record.redundantBlocks)
			if err != nil {
				t.Fatalf("error creating new inverted index: %s", err)
			}

			assert.True(t, first.Eq(second))

			err = os.Remove(first.FilePath)
			if err != nil {
				t.Fatalf("error removing inverted index file: %s", first.FilePath)
			}
		}
	})
}

func TestInvertedIndex_Add(t *testing.T) {
	fileName := "testdb.iscdb"
	now := uint64(time.Now().Unix())

	addParams := []testAddParams{
		{[]byte("foo"), 20, 0},
		{[]byte("food"), 60, now + 3600},
		{[]byte("fore"), 160, 0},
		{[]byte("bar"), 600, now - 3600}, // expired
		{[]byte("bare"), 90, now + 7200},
		{[]byte("barricade"), 900, 0},
		{[]byte("pig"), 80, 0},
	}

	t.Run("AddAddsTheKeyAndAddrToInvertedIndex", func(t *testing.T) {
		defer func() {
			_ = os.Remove(fileName)
		}()
		table := []testSearchParams{
			{[]byte("f"), 0, 0, []uint64{20, 60, 160}},
			{[]byte("fo"), 0, 0, []uint64{20, 60, 160}},
			{[]byte("foo"), 0, 0, []uint64{20, 60}},
			{[]byte("for"), 0, 0, []uint64{160}},
			{[]byte("food"), 0, 0, []uint64{60}},
			{[]byte("fore"), 0, 0, []uint64{160}},
			{[]byte("b"), 0, 0, []uint64{90, 900}},
			{[]byte("ba"), 0, 0, []uint64{90, 900}},
			{[]byte("bar"), 0, 0, []uint64{90, 900}},
			{[]byte("bare"), 0, 0, []uint64{90}},
			{[]byte("barr"), 0, 0, []uint64{900}},
			{[]byte("p"), 0, 0, []uint64{80}},
			{[]byte("pi"), 0, 0, []uint64{80}},
			{[]byte("pig"), 0, 0, []uint64{80}},
		}

		searchIdx := createSearchIndex(t, fileName, addParams)
		testSearchResults(t, searchIdx, table)
	})

	t.Run("AddUpdatesPreexistingKeyAndAddrToInvertedIndex", func(t *testing.T) {
		defer func() {
			_ = os.Remove(fileName)
		}()

		updates := []testAddParams{
			{[]byte("foo"), 20, now - 30},    // expired
			{[]byte("bare"), 90, now - 7200}, // expired
			{[]byte("bar"), 500, now + 3600},
		}
		table := []testSearchParams{
			{[]byte("f"), 0, 0, []uint64{60, 160}},
			{[]byte("fo"), 0, 0, []uint64{60, 160}},
			{[]byte("foo"), 0, 0, []uint64{60}},
			{[]byte("for"), 0, 0, []uint64{160}},
			{[]byte("food"), 0, 0, []uint64{60}},
			{[]byte("fore"), 0, 0, []uint64{160}},
			{[]byte("b"), 0, 0, []uint64{500, 900}},
			{[]byte("ba"), 0, 0, []uint64{500, 900}},
			{[]byte("bar"), 0, 0, []uint64{500, 900}},
			{[]byte("bare"), 0, 0, []uint64{}},
			{[]byte("barr"), 0, 0, []uint64{900}},
			{[]byte("p"), 0, 0, []uint64{80}},
			{[]byte("pi"), 0, 0, []uint64{80}},
			{[]byte("pig"), 0, 0, []uint64{80}},
		}

		searchIdx := createSearchIndex(t, fileName, addParams)
		for _, p := range updates {
			err := searchIdx.Add(p.k, p.addr, p.expiry)
			if err != nil {
				t.Fatalf("error updating key address %s: %s", p.k, err)
			}
		}

		testSearchResults(t, searchIdx, table)
	})
}

func TestInvertedIndex_Search(t *testing.T) {
	fileName := "testdb.iscdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	now := uint64(time.Now().Unix())

	addParams := []testAddParams{
		{[]byte("foo"), 20, 0},
		{[]byte("food"), 60, now + 3600},
		{[]byte("fore"), 160, 0},
		{[]byte("bar"), 600, now - 3600}, // expired
		{[]byte("bare"), 90, now + 7200},
		{[]byte("barricade"), 900, 0},
		{[]byte("pig"), 80, 0},
	}

	table := []testSearchParams{
		{[]byte("f"), 0, 0, []uint64{20, 60, 160}},
		{[]byte("f"), 1, 0, []uint64{60, 160}},
		{[]byte("f"), 2, 0, []uint64{160}},
		{[]byte("f"), 3, 0, []uint64{}},
		{[]byte("f"), 0, 3, []uint64{20, 60, 160}},
		{[]byte("f"), 0, 2, []uint64{20, 60}},
		{[]byte("f"), 1, 3, []uint64{60, 160}},
		{[]byte("f"), 1, 2, []uint64{60, 160}},
		{[]byte("f"), 2, 2, []uint64{160}},
		{[]byte("fo"), 0, 0, []uint64{20, 60, 160}},
		{[]byte("fo"), 1, 0, []uint64{60, 160}},
		{[]byte("fo"), 2, 0, []uint64{160}},
		{[]byte("fo"), 1, 1, []uint64{60}},
		{[]byte("bar"), 0, 0, []uint64{90, 900}},
		{[]byte("bar"), 1, 0, []uint64{900}},
		{[]byte("bar"), 1, 1, []uint64{900}},
		{[]byte("bar"), 1, 1, []uint64{900}},
		{[]byte("pi"), 0, 2, []uint64{80}},
		{[]byte("pi"), 1, 2, []uint64{}},
		{[]byte("pigg"), 1, 2, []uint64{}},
		{[]byte("ben"), 1, 2, []uint64{}},
	}

	searchIdx := createSearchIndex(t, fileName, addParams)
	testSearchResults(t, searchIdx, table)
}

func TestInvertedIndex_Remove(t *testing.T) {
	fileName := "testdb.iscdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	now := uint64(time.Now().Unix())
	addParams := []testAddParams{
		{[]byte("foo"), 20, 0},
		{[]byte("food"), 60, now + 3600},
		{[]byte("fore"), 160, 0},
		{[]byte("bar"), 600, now - 3500}, // expired
		{[]byte("bare"), 90, now + 7200},
		{[]byte("barricade"), 900, 0},
		{[]byte("pig"), 80, 0},
	}

	keysToRemove := [][]byte{[]byte("foo"), []byte("pig")}
	table := []testSearchParams{
		{[]byte("f"), 0, 0, []uint64{60, 160}},
		{[]byte("fo"), 0, 0, []uint64{60, 160}},
		{[]byte("foo"), 0, 0, []uint64{60}},
		{[]byte("for"), 0, 0, []uint64{160}},
		{[]byte("food"), 0, 0, []uint64{60}},
		{[]byte("fore"), 0, 0, []uint64{160}},
		{[]byte("b"), 0, 0, []uint64{90, 900}},
		{[]byte("ba"), 0, 0, []uint64{90, 900}},
		{[]byte("bar"), 0, 0, []uint64{90, 900}},
		{[]byte("bare"), 0, 0, []uint64{90}},
		{[]byte("barr"), 0, 0, []uint64{900}},
		{[]byte("p"), 0, 0, []uint64{}},
		{[]byte("pi"), 0, 0, []uint64{}},
		{[]byte("pig"), 0, 0, []uint64{}},
	}

	searchIdx := createSearchIndex(t, fileName, addParams)
	removeManyKeys(t, searchIdx, keysToRemove)
	testSearchResults(t, searchIdx, table)
}

func TestInvertedIndex_Clear(t *testing.T) {
	fileName := "testdb.iscdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	now := uint64(time.Now().Unix())
	addParams := []testAddParams{
		{[]byte("foo"), 20, 0},
		{[]byte("food"), 60, now + 3600},
		{[]byte("fore"), 160, 0},
		{[]byte("bar"), 600, now - 3600}, // expired
		{[]byte("bare"), 90, now + 7200},
		{[]byte("barricade"), 900, 0},
		{[]byte("pig"), 80, 0},
	}

	table := []testSearchParams{
		{[]byte("f"), 0, 0, []uint64{}},
		{[]byte("fo"), 0, 0, []uint64{}},
		{[]byte("foo"), 0, 0, []uint64{}},
		{[]byte("for"), 0, 0, []uint64{}},
		{[]byte("food"), 0, 0, []uint64{}},
		{[]byte("fore"), 0, 0, []uint64{}},
		{[]byte("b"), 0, 0, []uint64{}},
		{[]byte("ba"), 0, 0, []uint64{}},
		{[]byte("bar"), 0, 0, []uint64{}},
		{[]byte("bare"), 0, 0, []uint64{}},
		{[]byte("barr"), 0, 0, []uint64{}},
		{[]byte("p"), 0, 0, []uint64{}},
		{[]byte("pi"), 0, 0, []uint64{}},
		{[]byte("pig"), 0, 0, []uint64{}},
	}

	searchIdx := createSearchIndex(t, fileName, addParams)
	err := searchIdx.Clear()
	if err != nil {
		t.Fatalf("error clearing inverted index: %s", err)
	}

	testSearchResults(t, searchIdx, table)
}

func TestInvertedIndex_Close(t *testing.T) {
	fileName := "testdb.iscdb"
	defer func() {
		_ = os.Remove(fileName)
	}()

	idx, err := NewInvertedIndex(fileName, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating inverted index: %s", err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatalf("error closing inverted index: %s", err)
	}

	// Close has already been called on File
	assert.NotNil(t, idx.File.Close())
}

// removeManyKeys removes many keys from the inverted index
func removeManyKeys(t *testing.T, idx *InvertedIndex, keys [][]byte) {
	for _, key := range keys {
		err := idx.Remove(key)
		if err != nil {
			t.Fatalf("error removing key '%s': %s", key, err)
		}
	}
}

// testSearchResults tests whether when the inverted index is searched, it returns
// the expected data in `params`
func testSearchResults(t *testing.T, idx *InvertedIndex, params []testSearchParams) {
	for _, p := range params {
		got, err := idx.Search(p.term, p.skip, p.limit)
		if err != nil {
			t.Fatalf("error searching for '%s': %s", p.term, err)
		}

		assert.Equal(t, p.expected, got)
	}
}

// createSearchIndex creates an inverted index for test purposes, and adds a number of
// test records as passed by the `params`
func createSearchIndex(t *testing.T, filePath string, params []testAddParams) *InvertedIndex {
	idx, err := NewInvertedIndex(filePath, nil, nil, nil)
	if err != nil {
		t.Fatalf("error creating inverted index: %s", err)
	}

	err = idx.Clear()
	if err != nil {
		t.Fatalf("error clearing inverted index: %s", err)
	}

	// Add a series of keys and addresses
	for _, p := range params {
		err = idx.Add(p.k, p.addr, p.expiry)
		if err != nil {
			t.Fatalf("error adding key address %s: %s", p.k, err)
		}
	}

	return idx
}
