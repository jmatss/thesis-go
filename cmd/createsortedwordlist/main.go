package main

import (
	"crypto/md5"
	"flag"
	"log"
	"time"

	"../../pkg"
)

const (
	DefaultStart       = 0
	DefaultEnd         = 0xffffffff
	DefaultMaxThreads  = 8
	DefaultFilename    = "list"
	DefaultBufferSize  = 1 << 28 // in sizeof(hashDigest) (1<<28 * sizeof(hashDigest) = 4 GB)
	DefaultPrintAmount = 1e7
)

func main() {
	// Parse custom options
	start := flag.Int("start", DefaultStart, "Start value of serial number")
	end := flag.Int("end", DefaultEnd, "End value of serial number")
	maxThreads := flag.Int("threads", DefaultMaxThreads, "Max amount of threads")
	filename := flag.String("filename", DefaultFilename, "Output filename of wordlist")
	bufferSize := flag.Int("buffersize", DefaultBufferSize, "Buffer size for block(s) in hashDigest(*16 bytes)")
	printAmount := flag.Int("printamount", DefaultPrintAmount, "Print status message every \"PrintAmount\" merge iteration")
	flag.Parse()

	totTime := time.Now()

	/*
		STAGE 1 - Create blocks of a size that can fit simultaneously in memory
		Generate, sort and write hashes of the blocks to separate files
	*/
	startTime := time.Now()
	blocks, err := createsortedwordlist.Create(*start, *end, *maxThreads, *bufferSize, *filename)
	if err != nil {
		log.Fatalf("could not create blocks: %v", err)
	}

	log.Printf("All %d blocks created, elapsed time: %v\n\n", blocks, time.Since(startTime))

	/*
		STAGE 2 - Merge the blocks into one single sorted file "FileName"
	*/
	startTime = time.Now()
	size, err := createsortedwordlist.Merge(blocks, *maxThreads, *bufferSize, *filename, *printAmount)
	if err != nil {
		log.Fatalf("could not merge blocks: %v", err)
	}
	if size != (*end-*start+1)*md5.Size {
		log.Fatalf("file on disk incorrect size, expected: %d, got: %d", (*end-*start+1)*md5.Size, size)
	}

	log.Printf("All blocks merged, elapsed time: %v\n\n", time.Since(startTime))

	log.Printf("Everything done, total elapsed time: %v\n", time.Since(totTime))
}
