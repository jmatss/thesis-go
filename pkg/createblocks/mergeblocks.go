package createblocks

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
)

const WriterBufferSize = 1000

func Merge(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string) error {
	p := make(priorityQueue, concurrentThreads)
	pq := syncPriorityQueue{pq: &p}
	MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize, filename, pq)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("could not open file %s: %v", filename, err)
	}
	writer := bufio.NewWriterSize(file, WriterBufferSize)
	for {
		min := pq.Pop().(hashDigest)
		// TODO: write in a new goprocess
		writer.Write()
	}
}

/*
Push(x interface{})
Pop() interface{}
Len() int
Less(i, j int) bool
Swap(i, j int)
*/
