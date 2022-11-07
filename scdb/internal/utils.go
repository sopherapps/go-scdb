package internal

import (
	"encoding/binary"
	"fmt"
	"github.com/sopherapps/go-scdb/scdb/errors"
	"os"
)

// Uint16ToByteArray converts a uint16 to a BigEndian byte array
func Uint16ToByteArray(v uint16) []byte {
	output := make([]byte, 2)
	binary.BigEndian.PutUint16(output, v)
	return output
}

// Uint16FromByteArray converts a BigEndian byte array to a uint16
func Uint16FromByteArray(v []byte) (uint16, error) {
	dataLength := len(v)
	if dataLength < 2 {
		return 0, errors.NewErrOutOfBounds(fmt.Sprintf("byte array length is %d, expected to be 2", dataLength))
	}

	num := binary.BigEndian.Uint16(v)
	return num, nil
}

// Uint32ToByteArray converts a uint32 to a BigEndian byte array
func Uint32ToByteArray(v uint32) []byte {
	output := make([]byte, 4)
	binary.BigEndian.PutUint32(output, v)
	return output
}

// Uint32FromByteArray converts a BigEndian byte array to a uint32
func Uint32FromByteArray(v []byte) (uint32, error) {
	dataLength := len(v)
	if dataLength < 4 {
		return 0, errors.NewErrOutOfBounds(fmt.Sprintf("byte array length is %d, expected to be 4", dataLength))
	}

	num := binary.BigEndian.Uint32(v)
	return num, nil
}

// Uint64ToByteArray converts a uint64 to a BigEndian byte array
func Uint64ToByteArray(v uint64) []byte {
	output := make([]byte, 8)
	binary.BigEndian.PutUint64(output, v)
	return output
}

// Uint64FromByteArray converts a BigEndian byte array to a uint64
func Uint64FromByteArray(v []byte) (uint64, error) {
	dataLength := len(v)
	if dataLength < 8 {
		return 0, errors.NewErrOutOfBounds(fmt.Sprintf("byte array length is %d, expected to be 8", dataLength))
	}

	num := binary.BigEndian.Uint64(v)
	return num, nil
}

// BoolToByteArray converts a bool to a byte array
func BoolToByteArray(v bool) []byte {
	if v {
		return []byte{1}
	} else {
		return []byte{0}
	}
}

// BoolFromByteArray converts a BigEndian byte array to a bool
func BoolFromByteArray(v []byte) (bool, error) {
	dataLength := len(v)
	if dataLength < 1 {
		return false, errors.NewErrOutOfBounds(fmt.Sprintf("byte array length is %d, expected to be 2", dataLength))
	}

	value := false

	if v[0] == 1 {
		value = true
	}

	return value, nil
}

// ConcatByteArrays concatenates a number of byte arrays
func ConcatByteArrays(arrays ...[]byte) []byte {
	totalLength := 0
	for _, array := range arrays {
		totalLength += len(array)
	}
	output := make([]byte, 0, totalLength)

	for _, array := range arrays {
		output = append(output, array...)
	}

	return output
}

// SafeSlice slices a slice safely, throwing an error if it goes out of bounds
func SafeSlice(data []byte, start uint64, end uint64, maxLength uint64) ([]byte, error) {
	if start >= maxLength || end > maxLength {
		return nil, errors.NewErrOutOfBounds(fmt.Sprintf("slice %d - %d out of bounds for maxLength %d for data %v", start, end, maxLength, data))
	}

	return data[start:end], nil
}

// GenerateFileWithTestData creates a file at the given filePath if it does not exist
// and adds the given data overwriting any pre-existing data
func GenerateFileWithTestData(filePath string, data []byte) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// ValidateBounds checks if the given range is within bounds or else returns an InvalidData error
// FIXME: Add test for this
func ValidateBounds(actualLower uint64, actualUpper uint64, expectedLower uint64, expectedUpper uint64, msg string) error {
	if actualLower < expectedLower || actualUpper > expectedUpper {
		return errors.NewErrOutOfBounds(fmt.Sprintf("%s Span %d-%d is out of bounds for %d-%d", msg, actualLower, actualUpper, expectedLower, expectedUpper))
	}
	return nil
}
