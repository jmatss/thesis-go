package createsortedwordlist

import (
	"crypto/md5"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/jmatss/thesis-go/pkg/model"
)

func TestCreate(t *testing.T) {
	/*
		Test to make sure that the file is created correctly, that the size is correct
	*/
	filename := "testlist"
	start := 0
	end := 256
	amountOfThreads := 4
	bufferSize := end - start + 1

	if _, err := Create(start, end, amountOfThreads, bufferSize, filename); err != nil {
		t.Fatalf("could not create blocks: %v", err)
	}

	filename = filename + strconv.Itoa(0)
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("couldn't open file \"%s\"", filename)
	}
	defer func() {
		file.Close()
		os.Remove(filename)
	}()

	stat, err := file.Stat()
	if err != nil {
		t.Fatalf("couldn't get stat of file \"%s\"", filename)
	}
	if int64(end-start+1)*md5.Size != stat.Size() {
		t.Fatalf("length of file \"%s\" on disk is incorrect: "+
			"expected %d, got %d", filename, int64(end-start+1)*md5.Size, stat.Size())
	}

	/*
		Test to make sure that the list is sorted and can be read (somewhat) correctly
	*/
	totReadBytes := 0
	b := model.Block{0, start, end, make([]model.HashDigest, end-start+1)}
	for i := 0; i < end-start+1; i++ {
		var digest [md5.Size]byte
		readBytes, err := file.Read(digest[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("couldn't read from file \"%s\"", filename)
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
