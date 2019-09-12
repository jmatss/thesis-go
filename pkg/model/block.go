package model

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
	"runtime"
	"sync"
)

const (
	/* The md5 digest is 16 bytes. Compare the last 6 bytes (CompLength) that starts at byte 10 (CompStart) */
	CompStart        = 10
	CompLength       = 6
	WriterBufferSize = 1 << 16
	GcTriggerAmount  = 30 // garbage collection triggered ~"GcTriggerAmount" times per CreateSubBlock
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

	// TODO: this GC is really ugly, find some other way to do this
	gcTrigger := (endSubBlock - startSubBlock + 1) / GcTriggerAmount
	for i := startSubBlock; i <= endSubBlock; i++ {
		if i%gcTrigger == 0 {
			runtime.GC()
		}
		serialNumber := fmt.Sprintf("%016x\n", i) // TODO: slow (change for better performance)
		b.Hashes[i-startBlock] = md5.Sum([]byte(serialNumber))
	}

	finished <- nil
}

func (b *Block) WriteToFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		b.clear() // remove reference to Hashes so that it can be garbage collected
		runtime.GC()
	}()

	writer := bufio.NewWriterSize(file, WriterBufferSize)
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

func (b *Block) Sort(amountOfThreads int) {
	semaphore := make(chan struct{}, amountOfThreads)
	b.quicksort(0, b.Len()-1, semaphore)
	close(semaphore)
}

func (b *Block) clear() {
	b.Hashes = nil // free memory
}

// sort.Interface
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

// DESC order, uses "highest/right most" element as pivot
func (b *Block) quicksort(low, high int, semaphore chan struct{}) {
	if low >= high {
		return
	}

	current := low
	pivot := high
	gtPivot := low

	for current < high {
		if b.Less(pivot, current) { // current >= pivot
			b.Swap(current, gtPivot)
			gtPivot++
		}
		current++
	}

	b.Swap(pivot, gtPivot) // pivot == current at this point
	pivot = gtPivot

	//  if semaphore allows: spawn two child quick sorts in new processes
	//  else: continue to do sorting in this go process
	select {
	case semaphore <- struct{}{}:
		wg := sync.WaitGroup{}
		wg.Add(2)

		go func(low, high int) {
			b.quicksort(low, high, semaphore)
			wg.Done()
		}(low, pivot-1)
		go func(low, high int) {
			b.quicksort(low, high, semaphore)
			wg.Done()
		}(pivot+1, high)

		wg.Wait()
		<-semaphore
	default:
		b.quicksort(low, pivot-1, semaphore)
		b.quicksort(pivot+1, high, semaphore)
	}
}
