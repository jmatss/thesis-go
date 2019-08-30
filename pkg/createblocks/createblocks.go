package createblocks

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// Creates blocks and returns the amount of blocks created if they are created successfully
// TODO: make new function that combines this function and createBlock
// TODO: write to file in a new goprocess so that one can start with generating/sorting the next block at the same time
func Create(start, end, amountOfBlocks, amountOfThreads, bufferSize int, filename string) (int, error) {
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
			return 0, fmt.Errorf("unable to write block %d to file \"%s\": %v", i, filename+strconv.Itoa(i), err)
		}
		log.Printf("Block written to file: %v", time.Since(startTime))

		currentStart = currentEnd + 1
	}
	log.Printf("Tot elapsed time: %v", time.Since(totTime))

	return len(blocks), nil
}

func CreateBlock(id, start, end, amountOfThreads int) *Block {
	block := Block{id, start, end, make([]hashDigest, end-start+1)}

	finished := make(chan error)
	hashesPerThread := ((end - start + 1) / amountOfThreads) - 1
	// if there are fewer hashes than threads, only spawn threads so that they get one hash each
	if hashesPerThread <= 0 {
		amountOfThreads = end - start + 1
	}

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
