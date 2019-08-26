package main

import (
	"log"
	"time"

	"github.com/jmatss/thesis-go/pkg/createblocks"
)

const (
	MaxAmountOfConcurrentThreads = 8
	AmountOfBlocks               = 1
	Filename                     = "d:\\listdir\\list"
	BlockBufferSize              = 1e6 // max buffer size for block(s)
)

func main() {
	start := 0
	end := 0xffffff

	/*
		STAGE 1 - Create blocks that has a size that can fit in the memory
		Generate, sort and write hashes of the block to separate files
	*/
	startTime := time.Now()
	blocks, err := createblocks.Create(start, end, AmountOfBlocks, MaxAmountOfConcurrentThreads, BlockBufferSize, Filename)
	if err != nil {
		log.Fatalf("could not create blocks: %v", err)
	}
	if blocks != AmountOfBlocks {
		log.Fatalf("created incorrect amount of blocks, want: %d, got: %d", AmountOfBlocks, blocks)
	}
	log.Printf("all blocks created, elapsed time: %v\n", time.Since(startTime))

	/*
		STAGE 2 - Merge the blocks in to one single sorted file
	*/
	startTime = time.Now()
	if err := createblocks.Merge(blocks, MaxAmountOfConcurrentThreads, BlockBufferSize, Filename); err != nil {
		log.Fatalf("could not merge blocks: %v", err)
	}
	log.Printf("all blocks merged, elapsed time: %v\n", time.Since(startTime))

}
