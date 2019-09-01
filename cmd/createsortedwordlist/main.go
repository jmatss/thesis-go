package main

import (
	"crypto/md5"
	"log"
	"time"

	"github.com/jmatss/thesis-go/pkg"
)

const (
	MaxAmountOfConcurrentThreads = 8
	AmountOfBlocks               = 4
	Filename                     = "list"
	BlockBufferSize              = 1e6 // max buffer size for block(s)
	PrintAmount                  = 1e7 // print status message every "PrintAmount" merge iteration
)

func main() {
	start := 0
	end := 0xffffffff

	totTime := time.Now()

	/*
		STAGE 1 - Create blocks of a size that can fit simultaneously in memory
		Generate, sort and write hashes of the blocks to separate files
	*/
	startTime := time.Now()
	blocks, err := createsortedwordlist.Create(start, end, AmountOfBlocks, MaxAmountOfConcurrentThreads, BlockBufferSize, Filename)
	if err != nil {
		log.Fatalf("could not create blocks: %v", err)
	}
	if blocks != AmountOfBlocks {
		log.Fatalf("created incorrect amount of blocks, expected: %d, got: %d", AmountOfBlocks, blocks)
	}

	log.Printf("All blocks created, elapsed time: %v\n\n", time.Since(startTime))

	/*
		STAGE 2 - Merge the blocks into one single sorted file "FileName"
	*/
	startTime = time.Now()
	size, err := createsortedwordlist.Merge(blocks, MaxAmountOfConcurrentThreads, BlockBufferSize, Filename, PrintAmount)
	if err != nil {
		log.Fatalf("could not merge blocks: %v", err)
	}
	if size != (end-start+1)*md5.Size {
		log.Fatalf("file on disk incorrect size, expected: %d, got: %d", (end-start+1)*md5.Size, size)
	}

	log.Printf("All blocks merged, elapsed time: %v\n\n", time.Since(startTime))

	log.Printf("Everything done, total elapsed time: %v\n", time.Since(totTime))
}
