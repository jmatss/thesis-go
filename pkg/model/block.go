package model

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
	"sort"
)

const (
	/* The md5 digest is 16 bytes. Compare the last 6 bytes (CompLength) that starts at byte 10 (CompStart) */
	CompStart  = 10
	CompLength = 6
)

type HashDigest [md5.Size]byte

type Block struct {
	Id     int
	Start  int
	End    int
	Hashes []HashDigest
}

func (current HashDigest) Less(other HashDigest) bool {
	// Treat an empty HashDigest as greater than,
	// this allows non empty HashDigest to be inserted before empty ones.
	if current == (HashDigest{}) {
		return false
	} else if other == (HashDigest{}) {
		return true
	}

	for i := CompStart; i < CompStart+CompLength; i++ {
		currentI, otherI := current[i], other[i]
		if currentI < otherI {
			return true
		} else if currentI > otherI {
			return false
		}
	}
	return false // equal
}

func (b *Block) CreateSubBlock(startSubBlock, endSubBlock, startBlock int, finished chan error) {
	defer func() {
		if r := recover(); r != nil {
			finished <- r.(error)
		}
	}()

	for i := startSubBlock; i <= endSubBlock; i++ {
		serialNumber := fmt.Sprintf("%016x\n", i) // TODO: slow (change for better performance)
		b.Hashes[i-startBlock] = md5.Sum([]byte(serialNumber))
	}

	finished <- nil
}

func (b *Block) WriteToFile(filename string, bufferSize int) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		b.clear() // remove reference to Hashes so that it can be garbage collected
	}()

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

func (b *Block) Sort() {
	// TODO: make sort parallel
	sort.Sort(sort.Reverse(b))
}

func (b *Block) clear() {
	b.Hashes = nil // free memory
}

// sort.Interface
func (b Block) Len() int {
	return len(b.Hashes)
}

// TODO: More effective if it is turned into int instead and compared after?
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
