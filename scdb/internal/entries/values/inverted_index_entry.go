package values

import "github.com/sopherapps/go-scdb/scdb/internal"

const InvertedIndexEntryMinSizeInBytes uint32 = 4 + 4 + 1 + 1 + 8 + 8 + 8 + 8

type InvertedIndexEntry struct {
	Size           uint32
	IndexKeySize   uint32
	IndexKey       []byte
	Key            []byte
	IsDeleted      bool
	IsRoot         bool
	Expiry         uint64
	NextOffset     uint64
	PreviousOffset uint64
	KvAddress      uint64
}

// NewInvertedIndexEntry creates a new InvertedIndexEntry
func NewInvertedIndexEntry(indexKey []byte, key []byte, expiry uint64, isRoot bool, kvAddr uint64, nextOffset uint64, previousOffset uint64) *InvertedIndexEntry {
	keySize := uint32(len(key))
	indexKeySize := uint32(len(indexKey))
	size := keySize + indexKeySize + InvertedIndexEntryMinSizeInBytes

	return &InvertedIndexEntry{
		Size:           size,
		IndexKeySize:   indexKeySize,
		IndexKey:       indexKey,
		Key:            key,
		IsDeleted:      false,
		IsRoot:         isRoot,
		Expiry:         expiry,
		NextOffset:     nextOffset,
		PreviousOffset: previousOffset,
		KvAddress:      kvAddr,
	}
}

// ExtractInvertedIndexEntryFromByteArray extracts the key value entry from the data byte array
func ExtractInvertedIndexEntryFromByteArray(data []byte, offset uint64) (*InvertedIndexEntry, error) {
	dataLength := uint64(len(data))
	sizeSlice, err := internal.SafeSlice(data, offset, offset+4, dataLength)
	if err != nil {
		return nil, err
	}
	size, err := internal.Uint32FromByteArray(sizeSlice)
	if err != nil {
		return nil, err
	}

	indexKeySizeSlice, err := internal.SafeSlice(data, offset+4, offset+8, dataLength)
	if err != nil {
		return nil, err
	}
	indexKeySize, err := internal.Uint32FromByteArray(indexKeySizeSlice)
	if err != nil {
		return nil, err
	}

	indexKeySizeU64 := uint64(indexKeySize)
	indexKey, err := internal.SafeSlice(data, offset+8, offset+8+indexKeySizeU64, dataLength)
	if err != nil {
		return nil, err
	}

	keySizeU64 := uint64(size - indexKeySize - InvertedIndexEntryMinSizeInBytes)
	key, err := internal.SafeSlice(data, offset+8+indexKeySizeU64, offset+8+indexKeySizeU64+keySizeU64, dataLength)
	if err != nil {
		return nil, err
	}

	isDeletedSlice, err := internal.SafeSlice(data, offset+8+indexKeySizeU64+keySizeU64, offset+9+indexKeySizeU64+keySizeU64, dataLength)
	if err != nil {
		return nil, err
	}
	isDeleted, err := internal.BoolFromByteArray(isDeletedSlice)
	if err != nil {
		return nil, err
	}

	isRootSlice, err := internal.SafeSlice(data, offset+9+indexKeySizeU64+keySizeU64, offset+10+indexKeySizeU64+keySizeU64, dataLength)
	if err != nil {
		return nil, err
	}
	isRoot, err := internal.BoolFromByteArray(isRootSlice)
	if err != nil {
		return nil, err
	}

	expirySlice, err := internal.SafeSlice(data, offset+10+indexKeySizeU64+keySizeU64, offset+indexKeySizeU64+keySizeU64+18, dataLength)
	if err != nil {
		return nil, err
	}
	expiry, err := internal.Uint64FromByteArray(expirySlice)
	if err != nil {
		return nil, err
	}

	nextOffsetSlice, err := internal.SafeSlice(data, offset+18+indexKeySizeU64+keySizeU64, offset+indexKeySizeU64+keySizeU64+26, dataLength)
	if err != nil {
		return nil, err
	}
	nextOffset, err := internal.Uint64FromByteArray(nextOffsetSlice)
	if err != nil {
		return nil, err
	}

	prevOffsetSlice, err := internal.SafeSlice(data, offset+26+indexKeySizeU64+keySizeU64, offset+indexKeySizeU64+keySizeU64+34, dataLength)
	if err != nil {
		return nil, err
	}
	prevOffset, err := internal.Uint64FromByteArray(prevOffsetSlice)
	if err != nil {
		return nil, err
	}

	kvAddrSlice, err := internal.SafeSlice(data, offset+34+indexKeySizeU64+keySizeU64, offset+indexKeySizeU64+keySizeU64+42, dataLength)
	if err != nil {
		return nil, err
	}
	kvAddr, err := internal.Uint64FromByteArray(kvAddrSlice)
	if err != nil {
		return nil, err
	}

	entry := InvertedIndexEntry{
		Size:           size,
		IndexKeySize:   indexKeySize,
		IndexKey:       indexKey,
		Key:            key,
		IsDeleted:      isDeleted,
		IsRoot:         isRoot,
		Expiry:         expiry,
		NextOffset:     nextOffset,
		PreviousOffset: prevOffset,
		KvAddress:      kvAddr,
	}

	return &entry, nil
}

func (ide *InvertedIndexEntry) GetExpiry() uint64 {
	return ide.Expiry
}

func (ide *InvertedIndexEntry) AsBytes() []byte {
	return internal.ConcatByteArrays(
		internal.Uint32ToByteArray(ide.Size),
		internal.Uint32ToByteArray(ide.IndexKeySize),
		ide.IndexKey,
		ide.Key,
		internal.BoolToByteArray(ide.IsDeleted),
		internal.BoolToByteArray(ide.IsRoot),
		internal.Uint64ToByteArray(ide.Expiry),
		internal.Uint64ToByteArray(ide.NextOffset),
		internal.Uint64ToByteArray(ide.PreviousOffset),
		internal.Uint64ToByteArray(ide.KvAddress),
	)
}
