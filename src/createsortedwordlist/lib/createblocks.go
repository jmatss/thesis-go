package lib

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// TODO: make new function that combines this function and createBlock
func CreateBlocks(start, end, amountOfBlocks, amountOfThreads, bufferSize int, filename string) []*Block {
	blocks := make([]*Block, amountOfBlocks)
	hashesPerBlock := (end - start + 1) / amountOfBlocks
	totTime := time.Now()

	currentStart := start
	for i := 0; i < amountOfBlocks; i++ {
		startTime := time.Now()
		currentEnd := currentStart + hashesPerBlock
		if i == amountOfBlocks-1 {
			currentEnd = end // last iteration, take rest of hashes
		}
		blocks[i] = CreateBlock(i, currentStart, currentEnd, amountOfThreads)
		log.Printf("Create block: %v", time.Since(startTime))

		startTime = time.Now()
		blocks[i].Sort()
		log.Printf("Block sorted: %v", time.Since(startTime))

		startTime = time.Now()
		err := blocks[i].writeToFile(filename+strconv.Itoa(i), bufferSize)
		if err != nil {
			panic(fmt.Errorf("unable to write block %d to file \"%s\": %v", i, filename+strconv.Itoa(i), err))
		}
		log.Printf("Block written to file: %v", time.Since(startTime))

		currentStart = currentEnd + 1
	}
	log.Printf("Tot elapsed time: %v", time.Since(totTime))

	return blocks
}

func CreateBlock(id, start, end, amountOfThreads int) *Block {
	block := Block{id, start, end, make([]hashDigest, end-start+1)}

	hashesPerThread := (end - start + 1) / amountOfThreads
	finished := make(chan error)

	currentStart := start
	for i := 0; i < amountOfThreads; i++ {
		currentEnd := currentStart + hashesPerThread
		if i == amountOfThreads-1 { // last iteration, take rest of hashes
			currentEnd = end
		}

		go block.createSubBlock(currentStart, currentEnd, start, finished)
		currentStart = currentEnd + 1
	}

	for i := 0; i < amountOfThreads; i++ {
		err := <-finished
		if err != nil {
			panic(fmt.Errorf("unable to create block %d: %v", id, err))
		}
	}

	return &block
}
