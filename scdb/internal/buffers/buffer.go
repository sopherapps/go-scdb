package buffers

import (
	"bytes"
	"github.com/sopherapps/go-scbd/scdb/internal"
	"github.com/sopherapps/go-scbd/scdb/internal/entries"
	"math"
)

type Value struct {
	Data    []byte
	IsStale bool
}

// Buffer is the in-memory cache for byte arrays read from file
// Its `LeftOffset` is the file_offset where the byte array `Data` is read from
// while its `RightOffset` is the *exclusive* upper bound file offset of the same.
// the `RightOffset` is not an offset within this buffer but is the left_offset of the buffer
// that would be got from the file to the immediate right of this buffer's data array
type Buffer struct {
	capacity    uint64
	Data        []byte
	LeftOffset  uint64
	RightOffset uint64
}

// NewBuffer creates a new Buffer with the given left_offset
// FIXME: Add test for this
func NewBuffer(leftOffset uint64, data []byte, capacity uint64) *Buffer {
	upperBound := uint64(math.Min(float64(len(data)), float64(capacity)))
	rightOffset := leftOffset + upperBound
	bufData := data[:upperBound]
	return &Buffer{
		capacity:    capacity,
		Data:        bufData,
		LeftOffset:  leftOffset,
		RightOffset: rightOffset,
	}
}

// ExtractValueFromKeyValueEntry extracts a *Value from a entries.KeyValueEntry
func ExtractValueFromKeyValueEntry(kv *entries.KeyValueEntry) *Value {
	return &Value{
		Data:    kv.Value,
		IsStale: kv.IsDeleted || kv.IsExpired(),
	}
}

// CanAppend checks if the given address can be appended to this buffer
// The buffer should be contiguous thus this is true if `address` is
// equal to the exclusive `RightOffset` and the capacity has not been reached yet.
func (b *Buffer) CanAppend(addr uint64) bool {
	return (b.RightOffset-b.LeftOffset) < b.capacity && addr == b.RightOffset
}

// Contains checks if the given address is in this buffer
func (b *Buffer) Contains(addr uint64) bool {
	return b.LeftOffset <= addr && addr < b.RightOffset
}

// Append appends the data to the end of the array
// It returns the address (or offset) where the data was appended
//
// It is possible for data appended to this buffer to make it exceed
// its capacity. However, after that Buffer.CanAppend will always return false
// So make sure you call `can_append()` always.
// This is a trade-off that allows us to limit the number of re-allocations for buffers
func (b *Buffer) Append(data []byte) uint64 {
	dataLength := len(data)
	b.Data = append(b.Data, data...)
	prevRightOffset := b.RightOffset
	b.RightOffset += uint64(dataLength)
	return prevRightOffset
}

// Replace replaces the data at the given address with the new data
func (b *Buffer) Replace(addr uint64, data []byte) error {
	dataLength := uint64(len(data))
	err := internal.ValidateBounds(addr, addr+dataLength, b.LeftOffset, b.RightOffset, "address out of bound")
	if err != nil {
		return err
	}

	start := addr - b.LeftOffset
	stop := start + dataLength
	copy(b.Data[start:stop], data)
	return nil
}

// GetValue returns the *Value at the given address if the key there corresponds to the given key
// Otherwise, it returns nil. This is to handle hash collisions.
func (b *Buffer) GetValue(addr uint64, key []byte) (*Value, error) {
	offset := addr - b.LeftOffset
	entry, err := entries.ExtractKeyValueEntryFromByteArray(b.Data, offset)
	if err != nil {
		return nil, err
	}

	if bytes.Equal(entry.Key, key) {
		return ExtractValueFromKeyValueEntry(entry), nil
	}
	return nil, nil
}

// ReadAt reads an arbitrary array at the given address and of given size and returns it
func (b *Buffer) ReadAt(addr uint64, size uint64) ([]byte, error) {
	err := internal.ValidateBounds(addr, addr+size, b.LeftOffset, b.RightOffset, "address out of bounds")
	if err != nil {
		return nil, err
	}

	lwr := addr - b.LeftOffset
	return b.Data[lwr : lwr+size], nil
}

// AddrBelongsToKey checks to see if the given address is for the given key
func (b *Buffer) AddrBelongsToKey(addr uint64, key []byte) (bool, error) {
	keySize := uint64(len(key))
	err := internal.ValidateBounds(addr, addr+keySize+entries.OffsetForKeyInKVArray, b.LeftOffset, b.RightOffset, "address out of bounds")
	if err != nil {
		return false, err
	}

	lw := addr - b.LeftOffset + entries.OffsetForKeyInKVArray
	keyInData := b.Data[lw : lw+keySize]
	return bytes.Equal(keyInData, key), nil
}

// TryDeleteKvEntry tries to delete the kv entry at the given address
// It returns false if the kv entry at the given address is not for the given key
func (b *Buffer) TryDeleteKvEntry(addr uint64, key []byte) (bool, error) {
	keySize := uint64(len(key))
	err := internal.ValidateBounds(addr, addr+keySize+entries.OffsetForKeyInKVArray, b.LeftOffset, b.RightOffset, "address out of bounds")
	if err != nil {
		return false, err
	}

	keyOffset := addr - b.LeftOffset + entries.OffsetForKeyInKVArray
	keyInData := b.Data[keyOffset : keyOffset+keySize]

	if bytes.Equal(keyInData, key) {
		isDeletedIdx := keyOffset + keySize
		b.Data[isDeletedIdx] = 1 // True
		return true, nil
	}

	return false, nil
}

// Eq checks to see if two buffers are equal
func (b *Buffer) Eq(other *Buffer) bool {
	return b.LeftOffset == other.LeftOffset &&
		b.RightOffset == other.RightOffset &&
		bytes.Equal(b.Data, other.Data)
}
