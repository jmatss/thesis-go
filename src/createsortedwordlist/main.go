package main

import (
	"createsortedwordlist/lib"
	"fmt"
	"time"
)

const (
	MaxAmountOfConcurrentThreads = 8
	AmountOfBlocks               = 4
	Filename                     = "d:\\listdir\\list"
	BufferSize                   = 1000000
)

func main() {
	start := 0
	end := 0xffffff
	startTime := time.Now()
	blocks := lib.CreateBlocks(start, end, AmountOfBlocks, MaxAmountOfConcurrentThreads, BufferSize, Filename)
	/*
		for _, block := range blocks {
			for _, hashDigest := range (*block).Hashes {
				fmt.Printf("%s %s\n", hex.EncodeToString([]byte(hashDigest[:]))[:20], hex.EncodeToString([]byte(hashDigest[:]))[20:])
			}
		}
	*/
	fmt.Printf("%v (happy compiler:%p)\n", time.Since(startTime), blocks)
}
