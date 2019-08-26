package main

import (
	"createsortedwordlist/lib"
	"fmt"
	"time"
)

const (
	MaxAmountOfConcurrentThreads = 8
	AmountOfBlocks               = 1
	Filename                     = "d:\\listdir\\list"
	BufferSize                   = 1000000
)

func main() {
	start := 0
	end := 0xffffff
	startTime := time.Now()
	blocks := lib.CreateBlocks(start, end, AmountOfBlocks, MaxAmountOfConcurrentThreads, BufferSize, Filename)
	fmt.Printf("%v (happy compiler:%p)\n", time.Since(startTime), blocks)
}
