package model

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strconv"
)

// Wrapping a buffered reader that reads and buffers a file in reverse.
// This lets on truncate the file after reading in hashes.
//
// TODO: read new hashes from file in a new goprocess?
// 	might not make a big difference since it will most likely be disk IO bound
type ReverseFileReader struct {
	id       int
	filename string
	buf      []HashDigest
	position int
	limit    int
	capacity int
}

// Creates a new ReverseFileReader for the specified file.
//
// position: current index of the buffer where the next "Read" will read from.
// limit: index of the last valid item in the buffer.
// capacity: max capacity of the buffer.
func NewReverseFileReader(id int, filename string, bufferSize int) *ReverseFileReader {
	return &ReverseFileReader{
		id:       id,
		filename: filename + strconv.Itoa(id),
		buf:      make([]HashDigest, bufferSize),
		position: 0,
		limit:    0,
		capacity: bufferSize,
	}
}

// Returns the HashDigest at the front of the buffer.
func (reader *ReverseFileReader) Peek() HashDigest {
	return reader.buf[reader.position]
}

// Returns the HashDigest at the front of the buffer and increments the position.
// If the buffer is empty, it will either be refilled or
// an io.EOF will be returned if there are no more hashes in the file on disk
func (reader *ReverseFileReader) Read() (HashDigest, error) {
	result := reader.buf[reader.position]
	reader.position++

	// TODO: if the previous read read the remaining of the file exactly and at the same time
	// 	filled the capacity of the buffer exactly, it will get caught in the first if
	// 	instead of the else if (does it matter(?))
	if reader.position == reader.capacity {
		if err := reader.Refill(); err != nil {
			return HashDigest{}, err
		}
	} else if reader.position == reader.limit+1 {
		return HashDigest{}, io.EOF
	}

	return result, nil
}

// Refills the buffer with hashes from disk
func (reader *ReverseFileReader) Refill() error {
	file, err := os.OpenFile(reader.filename, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("could not open file %s: %v", reader.filename, err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not get fileStat of file %s: %v", reader.filename, err)
	}

	var newFilePos int64
	var result []byte

	// TODO: fix double mem?
	if fileStat.Size() < int64(reader.capacity*md5.Size) {
		newFilePos = 0
		result = make([]byte, fileStat.Size())
	} else {
		newFilePos, err = file.Seek(-int64(reader.capacity*md5.Size), 2) // 2 = seek from end of file
		if err != nil {
			return fmt.Errorf("could not seek in file %s: %v", reader.filename, err)
		}
		result = make([]byte, reader.capacity*md5.Size)
	}

	n, err := file.ReadAt(result, newFilePos)
	if err != nil {
		return fmt.Errorf("could not read from file %s at position %d: %v", reader.filename, newFilePos, err)
	}
	if n != len(result) {
		return fmt.Errorf("amount of bytes read from %s at position %d does not"+
			"correspond to the amount of bytes that was supposed to be read (want: %d, got: %d): %v",
			reader.filename, newFilePos, len(result), n, err)
	}

	err = file.Truncate(fileStat.Size() - int64(n))
	if err != nil {
		return fmt.Errorf("could not truncate file %s to size %d from size %d: %v",
			reader.filename, fileStat.Size()-int64(n), fileStat.Size(), err)
	}
	file.Close()

	if fileStat.Size() < int64(reader.capacity*md5.Size) {
		if err := os.Remove(reader.filename); err != nil {
			return fmt.Errorf("could not remove file %s: %v", reader.filename, err)
		}
	}

	reader.position = 0
	reader.limit = n / md5.Size
	for i := 0; i < reader.limit; i++ {
		var digest HashDigest
		for j := 0; j < md5.Size; j++ {
			digest[j] = result[i*md5.Size+j]
		}

		// read into the buffer in reverse order so that it is stored as ASC in the buffer
		reader.buf[reader.limit-1-i] = digest
	}

	return nil
}
