package entries

import (
	"github.com/sopherapps/go-scbd/scdb/internal"
	"time"
)

const KeyValueMinSizeInBytes uint32 = 4 + 4 + 8 + 1

type KeyValueEntry struct {
	Size      uint32
	KeySize   uint32
	Key       []byte
	Expiry    uint64
	IsDeleted bool
	Value     []byte
}

// NewKeyValueEntry creates a new KeyValueEntry
// `key` is the byte array of the key
// `value` is the byte array of the value
// `expiry` is the timestamp (in seconds from unix epoch)
func NewKeyValueEntry(key []byte, value []byte, expiry uint64) *KeyValueEntry {
	keySize := uint32(len(key))
	size := keySize + KeyValueMinSizeInBytes + uint32(len(value))

	return &KeyValueEntry{
		Size:      size,
		KeySize:   keySize,
		Key:       key,
		Expiry:    expiry,
		IsDeleted: false,
		Value:     value,
	}
}

// ExtractKeyValueEntryFromByteArray extracts the key value entry from the data byte array
func ExtractKeyValueEntryFromByteArray(data []byte, offset uint64) (*KeyValueEntry, error) {
	dataLength := uint64(len(data))
	sizeSlice, err := internal.SafeSlice(data, offset, offset+4, dataLength)
	if err != nil {
		return nil, err
	}
	size, err := internal.Uint32FromByteArray(sizeSlice)
	if err != nil {
		return nil, err
	}

	keySizeSlice, err := internal.SafeSlice(data, offset+4, offset+8, dataLength)
	if err != nil {
		return nil, err
	}
	keySize, err := internal.Uint32FromByteArray(keySizeSlice)
	if err != nil {
		return nil, err
	}

	kSize := uint64(keySize)
	key, err := internal.SafeSlice(data, offset+8, offset+8+kSize, dataLength)
	if err != nil {
		return nil, err
	}

	isDeletedSlice, err := internal.SafeSlice(data, offset+8+kSize, offset+9+kSize, dataLength)
	if err != nil {
		return nil, err
	}
	isDeleted, err := internal.BoolFromByteArray(isDeletedSlice)
	if err != nil {
		return nil, err
	}

	expirySlice, err := internal.SafeSlice(data, offset+9+kSize, offset+kSize+17, dataLength)
	if err != nil {
		return nil, err
	}
	expiry, err := internal.Uint64FromByteArray(expirySlice)
	if err != nil {
		return nil, err
	}

	valueSize := uint64(size - keySize - KeyValueMinSizeInBytes)
	value, err := internal.SafeSlice(data, offset+kSize+17, offset+kSize+17+valueSize, dataLength)
	if err != nil {
		return nil, err
	}

	entry := KeyValueEntry{
		Size:      size,
		KeySize:   keySize,
		Key:       key,
		Expiry:    expiry,
		IsDeleted: isDeleted,
		Value:     value,
	}

	return &entry, nil
}

// AsBytes retrieves the byte array that represents the key value entry.
func (kv *KeyValueEntry) AsBytes() []byte {
	return internal.ConcatByteArrays(
		internal.Uint32ToByteArray(kv.Size),
		internal.Uint32ToByteArray(kv.KeySize),
		kv.Key,
		internal.BoolToByteArray(kv.IsDeleted),
		internal.Uint64ToByteArray(kv.Expiry),
		kv.Value,
	)
}

// IsExpired returns true if key has lived for longer than its time-to-live
// It will always return false if time-to-live was never set
func (kv *KeyValueEntry) IsExpired() bool {
	if kv.Expiry == 0 {
		return false
	} else {
		return kv.Expiry < uint64(time.Now().Unix())
	}
}
