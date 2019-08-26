package createblocks

import (
	"crypto/md5"
	"io"
	"os"
	"strconv"
	"testing"
)

func TestCreate(t *testing.T) {
	// remove list from previous test if needed
	filename := "D:\\listdir\\testlist"
	os.Remove(filename + strconv.Itoa(0))

	/*
		Test to make sure that the file is created correctly, that the size is correct
	*/
	start := 0
	end := 256
	const amountOfBlocks = 1
	amountOfThreads := 4
	bufferSize := 1024

	if _, err := Create(start, end, amountOfBlocks, amountOfThreads, bufferSize, filename); err != nil {
		t.Errorf("could not create blocks: %v", err)
	}

	filename = filename + strconv.Itoa(0)
	file, err := os.Open(filename)
	if err != nil {
		t.Errorf("couldn't open file \"%s\"", filename)
		return
	}
	defer func() {
		file.Close()
		filename := "D:\\listdir\\testlist"
		os.Remove(filename + strconv.Itoa(0))
	}()

	stat, err := file.Stat()
	if err != nil {
		t.Errorf("couldn't get stat of file \"%s\"", filename)
		return
	}
	if int64(end-start+1)*md5.Size != stat.Size() {
		t.Errorf("length of file \"%s\" on disk is incorrect: "+
			"expected %d, got %d", filename, int64(end-start+1)*md5.Size, stat.Size())
		return
	}

	/*
		Test to make sure that the list is sorted and can be read (somewhat) correctly
	*/
	totReadBytes := 0
	b := Block{0, start, end, make([]hashDigest, end-start+1)}
	for i := 0; i < end-start+1; i++ {
		var digest [md5.Size]byte
		readBytes, err := file.Read(digest[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf("couldn't read from file \"%s\"", filename)
		}
		totReadBytes += readBytes
		b.Hashes[i] = digest
	}
	if totReadBytes != (end-start+1)*md5.Size {
		t.Errorf("incorrect amount of bytes read in from \"%s\": "+
			"expected %d, got %d", filename, (end-start+1)*md5.Size, totReadBytes)
	}
	for i := 1; i < len(b.Hashes); i++ {
		if b.Less(i-1, i) {
			t.Errorf("file \"%s\" incorrectly sorted (i-1=%d < i=%d)", filename, i-1, i)
		}
	}
}
