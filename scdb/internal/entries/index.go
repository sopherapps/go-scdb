package entries

import (
	"errors"
	"io"
	"os"
)

type blockResult struct {
	data []byte
	err  error
}

// Index is the Representation of the collection
// of all Index Entries, iterable block by block
type Index struct {
	NumOfBlocks int64
	BlockSize   int64
	file        *os.File
}

// NewIndex creates a new Index instance
func NewIndex(file *os.File, header *DbFileHeader) *Index {
	return &Index{
		NumOfBlocks: int64(header.NumberOfIndexBlocks),
		BlockSize:   int64(header.NetBlockSize),
		file:        file,
	}
}

// Blocks returns an iterator to get all blocks in the index
func (idx *Index) Blocks() <-chan blockResult {

	ch := make(chan blockResult)

	go func() {
		offset := int64(HeaderSizeInBytes)

		for block := int64(0); block < idx.NumOfBlocks; block++ {
			buf := make([]byte, idx.BlockSize)

			_, err := idx.file.ReadAt(buf, offset)
			if err != nil && !errors.Is(err, io.EOF) {
				ch <- blockResult{data: nil, err: err}
				break
			}

			ch <- blockResult{data: buf}
			offset += idx.BlockSize
		}

		close(ch)
	}()

	return ch
}
