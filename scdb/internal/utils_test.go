package internal

import (
	"github.com/sopherapps/go-scbd/scdb/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUint16ToByteArray(t *testing.T) {
	type testRecord struct {
		value    uint16
		expected []byte
	}
	testData := []testRecord{
		{39, []byte{0, 39}},
		{645, []byte{2, 133}},
		{23, []byte{0, 23}},
		{8000, []byte{31, 64}},
	}

	for _, record := range testData {
		got := Uint16ToByteArray(record.value)
		assert.Equal(t, record.expected, got)
	}
}

func TestUint16FromByteArray(t *testing.T) {
	type testRecord struct {
		expected uint16
		value    []byte
	}
	testData := []testRecord{
		{39, []byte{0, 39}},
		{645, []byte{2, 133}},
		{23, []byte{0, 23}},
		{8000, []byte{31, 64}},
	}

	for _, record := range testData {
		got, err := Uint16FromByteArray(record.value)
		if err != nil {
			t.Fatalf("error converting a byte array to uint16: %s", err)
		}

		assert.Equal(t, record.expected, got)
	}
}

func TestUint32ToByteArray(t *testing.T) {
	type testRecord struct {
		value    uint32
		expected []byte
	}
	testData := []testRecord{
		{3900, []byte{0, 0, 15, 60}},
		{64554, []byte{0, 0, 252, 42}},
		{23877, []byte{0, 0, 93, 69}},
		{8000866, []byte{0, 122, 21, 98}},
	}

	for _, record := range testData {
		got := Uint32ToByteArray(record.value)
		assert.Equal(t, record.expected, got)
	}
}

func TestUint32FromByteArray(t *testing.T) {
	type testRecord struct {
		expected uint32
		value    []byte
	}
	testData := []testRecord{
		{3900, []byte{0, 0, 15, 60}},
		{64554, []byte{0, 0, 252, 42}},
		{23877, []byte{0, 0, 93, 69}},
		{8000866, []byte{0, 122, 21, 98}},
	}

	for _, record := range testData {
		got, err := Uint32FromByteArray(record.value)
		if err != nil {
			t.Fatalf("error converting a byte array to uint32: %s", err)
		}

		assert.Equal(t, record.expected, got)
	}
}

func TestUint64ToByteArray(t *testing.T) {
	type testRecord struct {
		value    uint64
		expected []byte
	}
	testData := []testRecord{
		{3900, []byte{0, 0, 0, 0, 0, 0, 15, 60}},
		{64554, []byte{0, 0, 0, 0, 0, 0, 252, 42}},
		{23877, []byte{0, 0, 0, 0, 0, 0, 93, 69}},
		{8000866900, []byte{0, 0, 0, 1, 220, 227, 138, 84}},
	}

	for _, record := range testData {
		got := Uint64ToByteArray(record.value)
		assert.Equal(t, record.expected, got)
	}
}

func TestUint64FromByteArray(t *testing.T) {
	type testRecord struct {
		expected uint64
		value    []byte
	}
	testData := []testRecord{
		{3900, []byte{0, 0, 0, 0, 0, 0, 15, 60}},
		{64554, []byte{0, 0, 0, 0, 0, 0, 252, 42}},
		{23877, []byte{0, 0, 0, 0, 0, 0, 93, 69}},
		{8000866900, []byte{0, 0, 0, 1, 220, 227, 138, 84}},
	}

	for _, record := range testData {
		got, err := Uint64FromByteArray(record.value)
		if err != nil {
			t.Fatalf("error converting a byte array to uint64: %s", err)
		}

		assert.Equal(t, record.expected, got)
	}
}

func TestBoolToByteArray(t *testing.T) {
	type testRecord struct {
		value    bool
		expected []byte
	}
	testData := []testRecord{
		{true, []byte{1}},
		{false, []byte{0}},
	}

	for _, record := range testData {
		got := BoolToByteArray(record.value)
		assert.Equal(t, record.expected, got)
	}
}

func TestBoolFromByteArray(t *testing.T) {
	type testRecord struct {
		expected bool
		value    []byte
	}
	testData := []testRecord{
		{true, []byte{1}},
		{false, []byte{0}},
	}

	for _, record := range testData {
		got, err := BoolFromByteArray(record.value)
		if err != nil {
			t.Fatalf("error converting a byte array to bool: %s", err)
		}

		assert.Equal(t, record.expected, got)
	}
}

func TestConcatByteArrays(t *testing.T) {
	type testRecord struct {
		values   [][]byte
		expected []byte
	}
	testData := []testRecord{
		{[][]byte{{0, 39}, {67, 236, 89, 39}, {45}, {78}}, []byte{0, 39, 67, 236, 89, 39, 45, 78}},
		{[][]byte{{0, 39}, {45}, {78}}, []byte{0, 39, 45, 78}},
		{[][]byte{{0, 39}}, []byte{0, 39}},
	}

	for _, record := range testData {
		got := ConcatByteArrays(record.values...)
		assert.Equal(t, record.expected, got)
	}
}

func TestSafeSlice(t *testing.T) {
	data := []byte{0, 39, 67, 236, 89, 39, 45, 78}
	dataLength := uint64(len(data))
	type testRecord struct {
		start         uint64
		end           uint64
		expectedError error
		expectedSlice []byte
	}
	testData := []testRecord{
		{0, 3, nil, []byte{0, 39, 67}},
		{4, 8, nil, []byte{89, 39, 45, 78}},
		{0, 9, errors.NewErrOutOfBounds("slice 0 - 9 out of bounds for maxLength 8 for data [0 39 67 236 89 39 45 78]"), nil},
	}

	for _, record := range testData {
		got, err := SafeSlice(data, record.start, record.end, dataLength)
		assert.Equal(t, record.expectedError, err)
		assert.Equal(t, record.expectedSlice, got)
	}
}
