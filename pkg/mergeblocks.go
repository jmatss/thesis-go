package createsortedwordlist

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"./model"
)

const (
	// arbitrary chosen sizes
	WriterBufferSize = 16384
	ChanSize         = 16384
)

// Merges all sorted files named "filename+i" into one single sorted file called "filename"
// Returns the size of the new file on disk in bytes
func Merge(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string, printAmount int) (int, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("not able to open file %s: %v", filename, err)
	}

	writeBuffer := bufio.NewWriterSize(file, WriterBufferSize)
	defer file.Close()

	// start handler that will do all comparison/merging
	// this main thread will fetch the min from the minResult channel and do a buffered file write
	minResult := make(chan model.HashDigest, ChanSize)
	go MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize, filename, minResult)

	count := 0
	for {
		// if min empty: time to exit, everything done
		// TODO: can not be sure that all hashes has been merged, implement better error detection
		min := <-minResult
		if min == (model.HashDigest{}) {
			break
		}

		if n, err := writeBuffer.Write(min[:]); err != nil {
			return 0, fmt.Errorf("not able to write digest %016x to file %s: %v",
				min, filename, err)
		} else if n != md5.Size {
			return 0, fmt.Errorf("incorrect amount of bytes written to file when writing "+
				"digest %016x to file %s, expected: %d, wrote: %d: %v",
				min, filename, md5.Size, n, err)
		}

		count++
		if count%printAmount == 0 {
			log.Printf("%d hashes merged.", count)
		}
	}

	// flush to file and return size of file on disk so that the caller can see if size is correct
	writeBuffer.Flush()
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(stat.Size()), nil
}
