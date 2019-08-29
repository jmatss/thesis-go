package createblocks

import (
	"crypto/md5"
	"fmt"
	"os"
)

const BufferSize = 16384 / md5.Size // 16KB

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

func NewReverseFileReader(id int, filename string) *reverseFileReader {
	return &reverseFileReader{
		id:       id,
		filename: filename,
		buf:      make([]hashDigest, BufferSize),
		position: 0,
		limit:    0,
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
	} else if reader.position == reader.limit {
		return hashDigest{}, nil
		// TODO: done, exit out
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

	newFilePos, err := file.Seek(-int64(reader.capacity*md5.Size), 2) // 2 = seek from end of file
	if err != nil {
		return fmt.Errorf("could not seek in file %s: %v", reader.filename, err)
	}

	// TODO: fix double mem?
	// if there are fewer hashes in file than size of buffer, make smaller buffer to fit exactly the amount of read hashes
	var result []byte
	if newFilePos < 0 {
		result = make([]byte, reader.capacity*md5.Size+int(newFilePos))
		newFilePos = 0
	} else {
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

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not get stat of file %s: %v", reader.filename, err)
	}

	err = file.Truncate(stat.Size() - int64(n))
	if err != nil {
		return fmt.Errorf("could not truncate file %s to size %d from size %d: %v",
			reader.filename, stat.Size()-int64(n), stat.Size(), err)
	}
	reader.position = 0
	reader.limit = n / md5.Size

	for i := 0; i < reader.limit; i++ {
		var digest hashDigest
		for j := 0; j < md5.Size; j++ {
			digest[j] = result[i*md5.Size+j]
		}
		reader.buf[i] = digest
	}

	return nil
}

func (reader *reverseFileReader) wrap(i int) int {
	if i >= reader.capacity {
		i -= reader.capacity
	}
	return i
}
