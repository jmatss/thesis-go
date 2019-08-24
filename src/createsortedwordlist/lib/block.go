package lib

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
)

const (
	/*
		The md5 digest is 16 bytes. Compare the last 6 bytes (CompLength) that starts at byte 10 (CompStart)
	*/
	CompStart  = 10
	CompLength = 6
)

type hashDigest [md5.Size]byte

type Block struct { // implements sort.Interface
	id     int
	start  int
	end    int
	Hashes []hashDigest
}

func (b *Block) createSubBlock(startSubBlock, endSubBlock, startBlock int, finished chan error) {
	defer func() {
		if r := recover(); r != nil {
			finished <- r.(error)
		}
	}()
	for i := startSubBlock; i <= endSubBlock; i++ {
		serialNumber := fmt.Sprintf("%016d\n", i)
		b.Hashes[i-startBlock] = md5.Sum([]byte(serialNumber))
	}
	finished <- nil
}

func (b *Block) writeToFile(filename string, bufferSize int) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer func() {
		if err := file.Close(); err != nil {
			// TODO: do something
		}
	}()
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, bufferSize)
	for i := 0; i < len(b.Hashes); i++ {
		if _, err := writer.Write(b.Hashes[i][:]); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (b Block) Len() int {
	return len(b.Hashes)
}
func (b Block) Less(i, j int) bool {
	for k := CompStart; k < CompStart+CompLength; k++ {
		currentI, currentJ := b.Hashes[i][k], b.Hashes[j][k]
		if currentI < currentJ {
			return true
		} else if currentI > currentJ {
			return false
		}
	}
	return false // equal
}
func (b Block) Swap(i, j int) {
	b.Hashes[i], b.Hashes[j] = b.Hashes[j], b.Hashes[i]
}
