package createblocks

import (
	"fmt"
	"math"
	"os"
)

const BufferSize = 16384		// 16KB

/*
	Wrapping a buffered reader that reads and buffers a file in reverse
	TODO: read new hashes from file in a new goprocess?
		  might not make a big difference since it will most likely be disk IO bound
 */
type reverseFileReader struct {
	id       int
	filename string
	buf      []hashDigest
	position int
	limit    int
	capacity int
}

func NewReverseFileReader(id int, filename string) reverseFileReader {
	return reverseFileReader{
		id: id,
		filename: filename,
		buf: make([]hashDigest, BufferSize),
		position: 0,
		limit: 0,
		capacity: BufferSize,
	}
}

func (reader *reverseFileReader) Peek() hashDigest {
	return reader.buf[reader.position]
}

func (reader *reverseFileReader) Read() (hashDigest, error) {
	result := reader.buf[reader.position]
	reader.position++
	if reader.position == reader.capacity {
		if err := reader.refill(); err != nil {
			return hashDigest{}, err
		}
		reader.position = 0
	}
	// TODO: refill buf
	// ex: if position == endPos (empty)
	// return nil, if.EOF
	return result, nil
}

func (reader *reverseFileReader) refill() error {
	file, err := os.OpenFile(reader.filename, os.O_RDONLY, 0444)
	if err != nil {
		return fmt.Errorf("could not open file %s: %v", reader.filename, err)
	}
	defer file.Close()

	_, err := file.Read()
	if err != nil {
		return err
	}
	return nil
}

func (reader *reverseFileReader) wrap(i int) int {
	if i >= reader.capacity {
		i -= reader.capacity
	}
	return i
}