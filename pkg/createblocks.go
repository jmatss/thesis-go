package createsortedwordlist

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jmatss/thesis-go/pkg/model"
)

// Creates blocks and returns the amount of blocks created if they are created successfully
func Create(start, end, amountOfThreads, bufferSize int, filename string) (int, error) {
	amountOfBlocks := ((end - start) / bufferSize) + 1 // blocks needed to generate and sort all hashes
	blocks := make([]*model.Block, amountOfBlocks)

	hashesPerBlock := (end - start + 1) / amountOfBlocks
	currentStart := start
	for i := 0; i < amountOfBlocks; i++ {
		startTime := time.Now()
		currentEnd := currentStart + hashesPerBlock
		if i == amountOfBlocks-1 {
			currentEnd = end // last iteration, take rest of hashes
		}

		currentBlock := CreateBlock(i, currentStart, currentEnd, amountOfThreads)
		log.Printf("Created block%d, elapsed time: %v", i, time.Since(startTime))

		startTime = time.Now()
		currentBlock.Sort(amountOfThreads)
		log.Printf(" Block%d sorted: %v", i, time.Since(startTime))

		startTime = time.Now()
		if err := currentBlock.WriteToFile(filename + strconv.Itoa(i)); err != nil {
			return 0, fmt.Errorf("unable to write block %d to file \"%s\": %v", i, filename+strconv.Itoa(i), err)
		}
		log.Printf(" Block%d written to file: %v", i, time.Since(startTime))

		blocks[i] = currentBlock

		currentStart = currentEnd + 1
	}

	return len(blocks), nil
}

// Creates one block and returns a pointer to it
func CreateBlock(id, start, end, amountOfThreads int) *model.Block {
	block := model.Block{Id: id, Start: start, End: end, Hashes: make([]model.HashDigest, end-start+1)}

	finished := make(chan error, amountOfThreads)
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

		go block.CreateSubBlock(currentStart, currentEnd, start, finished)
		currentStart = currentEnd + 1
	}

	for i := 0; i < amountOfThreads; i++ {
		if err := <-finished; err != nil {
			panic(fmt.Errorf("unable to create block %d: %v", id, err))
		}
	}

	close(finished)
	return &block
}
