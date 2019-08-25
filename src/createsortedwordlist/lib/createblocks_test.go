package lib

import (
	"crypto/md5"
	"os"
	"strconv"
	"testing"
)

func TestCreateBlocks(t *testing.T) {
	filename := "D:\\listdir\\testlist"
	err := os.Remove(filename + strconv.Itoa(0))

	/*
		Test that correct amount of hashes is created and that the blocks are sorted
	*/
	start := 0
	end := 256
	amountOfBlocks := 1
	amountOfThreads := 4
	bufferSize := 1024

	blocks := CreateBlocks(start, end, amountOfBlocks, amountOfThreads, bufferSize, filename)

	totLen := 0
	for id, block := range blocks {
		b := *block
		totLen += len(b.Hashes)
		for i := 1; i < len(b.Hashes); i++ {
			if b.Less(i-1, i) {
				t.Errorf("Block %d incorrectly sorted (i-1=%d < i=%d)", id, i-1, i)
			}
		}
	}
	if totLen != end-start+1 {
		t.Errorf("length of created hashes in Block.createBlocks is incorrect: "+
			"expected len=%d, got len=%d", end-start+1, totLen)
	}

	/*
		Test to make sure that the file is created correctly and that the size is correct
	*/
	filename = filename + strconv.Itoa(0)
	file, err := os.Open(filename)
	if err != nil {
		t.Errorf("couldn't open file \"%s\"", filename)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		t.Errorf("couldn't get stat of file \"%s\"", filename)
		return
	}
	if int64(end-start+1)*md5.Size != stat.Size() {
		t.Errorf("length of file \"%s\" on disk is incorrect: "+
			"expected %d, got %d", filename, int64(end-start+1)*md5.Size, stat.Size())
	}

}
