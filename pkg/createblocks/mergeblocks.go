package createblocks

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
)

const WriterBufferSize = 1000

func Merge(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("not able to open file %s: %v", filename, err)
	}

	writeBuffer := bufio.NewWriterSize(file, WriterBufferSize)
	defer func() {
		writeBuffer.Flush()
		file.Close()
	}()

	// start handler that will do all comparisong/merging
	// this main thread will fetch the min from the minResult channel and do a buffered file write
	minResult := make(chan hashDigest)
	go MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize, filename, minResult)

	for {
		min := <-minResult
		// if min empty: time to exit, everything done
		if min == (hashDigest{}) {
			break
		}

		n, err := writeBuffer.Write(min[:])
		if err != nil {
			return fmt.Errorf("not able to write digest %016x to file %s: %v",
				min, filename, err)
		}
		if n != md5.Size {
			return fmt.Errorf("incorrect amount of bytes written to file when writing "+
				"digest %016x to file %s, expected: %d, wrote: %d: %v",
				min, filename, md5.Size, n, err)
		}
	}

	return nil
}
