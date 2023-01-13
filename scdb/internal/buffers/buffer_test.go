package buffers

import (
	"github.com/sopherapps/go-scdb/scdb/internal/entries/values"
	"github.com/stretchr/testify/assert"
	"testing"
)

var KvDataArray = []byte{
	/* size: 22u32*/ 0, 0, 0, 23,
	/* key size: 3u32*/ 0, 0, 0, 3,
	/* key */ 102, 111, 111,
	/* is_deleted */ 0,
	/* expiry 0u64 */ 0, 0, 0, 0, 0, 0, 0, 0,
	/* value */ 98, 97, 114,
}

const CAPACITY uint64 = 4098

func TestBuffer_Contains(t *testing.T) {
	buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
	type testRecord struct {
		input    uint64
		expected bool
	}
	testData := []testRecord{
		{8, false},
		{80, true},
		{89, false},
		{876, false},
	}

	for _, record := range testData {
		assert.Equal(t, record.expected, buf.Contains(record.input))
	}
}

func TestBuffer_CanAppend(t *testing.T) {
	data := []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}
	offset := uint64(79)
	type testRecord struct {
		capacity uint64
		addr     uint64
		expected bool
	}
	testData := []testRecord{
		{CAPACITY, 8, false},
		{CAPACITY, 89, true},
		{10, 89, false},
		{11, 89, true},
		{CAPACITY, 90, false},
		{CAPACITY, 900, false},
		{CAPACITY, 17, false},
		{10, 83, false},
	}

	for _, record := range testData {
		buf := NewBuffer(offset, data, record.capacity)
		assert.Equal(t, record.expected, buf.CanAppend(record.addr))
	}
}

func TestBuffer_Append(t *testing.T) {
	buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
	buf.Append([]byte{98, 97, 114, 101, 114})

	assert.Equal(t, buf, &Buffer{
		capacity:    CAPACITY,
		Data:        []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104, 98, 97, 114, 101, 114},
		LeftOffset:  79,
		RightOffset: 94,
	})
}

func TestBuffer_Replace(t *testing.T) {
	t.Run("Buffer.ReplaceReplacesValueAtAddressWithNewValue", func(t *testing.T) {
		buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
		err := buf.Replace(82, []byte{98, 97, 114, 101, 114})
		if err != nil {
			t.Fatalf("error calling buf.Replace: %s", err)
		}

		assert.Equal(t, buf, &Buffer{
			capacity:    CAPACITY,
			Data:        []byte{72, 97, 108, 98, 97, 114, 101, 114, 97, 104},
			LeftOffset:  79,
			RightOffset: 89,
		})
	})

	t.Run("Buffer.ReplaceReturnsErrorWhenAddrIsOutOfBoundsOrDataSuppliedWouldSpillOutOfBounds", func(t *testing.T) {
		buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
		type testRecord struct {
			addr uint64
			data []byte
		}
		testData := []testRecord{
			{85, []byte{98, 97, 114, 101, 114}},
			{86, []byte{98, 97, 114, 101}},
			{90, []byte{98, 97, 114, 101, 114}},
			{100, []byte{98}},
			{70, []byte{98, 97, 114, 101, 114}},
		}

		for _, record := range testData {
			err := buf.Replace(record.addr, record.data)
			assert.NotNil(t, err)
		}
	})
}

func TestBuffer_GetValue(t *testing.T) {
	t.Run("Buffer.GetValueGetsValueAtTheAddrIfItsKeyIsSameAsThatPassed", func(t *testing.T) {
		type testRecord struct {
			addr     uint64
			key      []byte
			expected *values.KeyValueEntry
		}

		buf := NewBuffer(79, KvDataArray, CAPACITY)
		kv := values.NewKeyValueEntry([]byte("foo"), []byte("bar"), 0)
		testData := []testRecord{
			{79, []byte("foo"), kv},
			{79, []byte("bar"), nil},
		}

		for _, record := range testData {
			v, err := buf.GetValue(record.addr, record.key)
			if err != nil {
				t.Fatalf("error getting value: %s", err)
			}
			assert.Equal(t, record.expected, v)
		}
	})

	t.Run("Buffer.GetValueReturnsErrorIfAddrIsOutOfBoundsForBuffer", func(t *testing.T) {
		type testRecord struct {
			addr uint64
			key  []byte
		}
		buf := NewBuffer(79, KvDataArray, CAPACITY)
		testData := []testRecord{
			{84, []byte("foo")},
			{84, []byte("bar")},
		}

		for _, record := range testData {
			v, err := buf.GetValue(record.addr, record.key)
			assert.NotNil(t, err)
			assert.Nil(t, v)
		}
	})
}

func TestBuffer_ReadAt(t *testing.T) {
	t.Run("Buffer.ReadAtReturnsByteArrayOfGivenSizeStartingAtTheGivenAddress", func(t *testing.T) {
		buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
		v, err := buf.ReadAt(82, 5)
		if err != nil {
			t.Fatalf("error reading at: %s", err)
		}

		assert.Equal(t, v, []byte{108, 101, 108, 117, 106})
	})

	t.Run("Buffer.ReadAtReturnsErrorIfAddressIsOutOfBoundsOrSizeWouldSpillOutOfBounds", func(t *testing.T) {
		type testRecord struct {
			addr uint64
			size uint64
		}

		buf := NewBuffer(79, []byte{72, 97, 108, 108, 101, 108, 117, 106, 97, 104}, CAPACITY)
		testData := []testRecord{
			{85, 5},
			{86, 4},
			{90, 4},
			{100, 1},
			{70, 3},
		}

		for _, record := range testData {
			v, err := buf.ReadAt(record.addr, record.size)
			assert.NotNil(t, err)
			assert.Nil(t, v)
		}
	})
}

func TestBuffer_AddrBelongsToKey(t *testing.T) {
	t.Run("Buffer.AddrBelongsToKeyReturnsTrueIfKeyValueAtAddressIsForGivenKey", func(t *testing.T) {
		type testRecord struct {
			addr     uint64
			key      []byte
			expected bool
		}

		buf := NewBuffer(79, KvDataArray, CAPACITY)
		testData := []testRecord{
			{79, []byte("foo"), true},
			{79, []byte("bar"), false},
		}

		for _, record := range testData {
			v, err := buf.AddrBelongsToKey(record.addr, record.key)
			if err != nil {
				t.Fatalf("error calling buf.AddrBelongsToKey: %s", err)
			}

			assert.Equal(t, record.expected, v)
		}
	})

	t.Run("Buffer.AddrBelongsToKeyReturnsErrorIfAddressIsOutOfBoundsOrKeySpillsOutOfBounds", func(t *testing.T) {
		type testRecord struct {
			addr uint64
			key  []byte
		}

		buf := NewBuffer(79, KvDataArray, CAPACITY)
		testData := []testRecord{
			{790, []byte("foo")},
			{78, []byte("foo")},
			{80, []byte("foo.......................................")}, // long key
			{79, []byte("foo.......................................")}, // long key
			{78, []byte("bar")},
			{790, []byte("bar")},
		}

		for _, record := range testData {
			v, err := buf.AddrBelongsToKey(record.addr, record.key)
			assert.NotNil(t, err)
			assert.False(t, v)
		}
	})
}

func TestBuffer_TryDeleteKvEntry(t *testing.T) {
	t.Run("Buffer.TryDeleteKvEntryDoesThatForGivenAddressIfAddressIsForGivenKeyReturningTrueIfSo", func(t *testing.T) {
		type testRecord struct {
			addr               uint64
			key                []byte
			expectedValue      bool
			expectedFinalArray []byte
		}

		postDeleteData := make([]byte, len(KvDataArray))
		copy(postDeleteData, KvDataArray)
		postDeleteData[11] = 1

		testData := []testRecord{
			{79, []byte("foo"), true, postDeleteData},
			{79, []byte("bar"), false, KvDataArray},
		}

		for _, record := range testData {
			buf := NewBuffer(79, KvDataArray, CAPACITY)
			v, err := buf.TryDeleteKvEntry(record.addr, record.key)
			if err != nil {
				t.Fatalf("error trying to delete key value: %s", err)
			}

			assert.Equal(t, record.expectedValue, v)
			assert.Equal(t, record.expectedFinalArray, buf.Data)
		}
	})

	t.Run("Buffer.TryDeleteKvEntryReturnsErrorIfAddressIsOutOfBoundsOrKeyWouldSpillOutOfBounds", func(t *testing.T) {
		type testRecord struct {
			addr uint64
			key  []byte
		}
		buf := NewBuffer(79, KvDataArray, CAPACITY)
		testData := []testRecord{
			{790, []byte("foo")},
			{78, []byte("foo")},
			{80, []byte("foo.......................................")}, // long key
			{79, []byte("foo.......................................")}, // long key
			{78, []byte("bar")},
			{790, []byte("bar")},
		}

		for _, record := range testData {
			v, err := buf.TryDeleteKvEntry(record.addr, record.key)
			assert.NotNil(t, err)
			assert.False(t, v)
		}
	})
}
