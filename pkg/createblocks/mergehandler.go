package createblocks

import (
	"container/heap"
)

func MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string, minResult chan hashDigest) {
	if amountOfBlocks < concurrentThreads {
		concurrentThreads = amountOfBlocks
	}

	//pq := make(priorityQueue, concurrentThreads)
	var pq priorityQueue
	blockResults := make([]chan hashDigest, concurrentThreads)

	// init
	blockRange := amountOfBlocks / concurrentThreads // ~amount of blocks per worker
	currentStart := 0
	for i := 0; i < concurrentThreads; i++ {
		currentEnd := currentStart + blockRange
		if i == concurrentThreads-1 { // last iteration
			currentEnd = amountOfBlocks - 1
		}

		blockResults[i] = make(chan hashDigest)

		go mergeThread(currentStart, currentEnd, blockBufferSize/amountOfBlocks, filename, blockResults[i])
	}

	// load smallest hash from all threads into pq
	for i, blockResult := range blockResults {
		heap.Push(&pq, &hashDigestWithID{i, <-blockResult})
	}

	for {
		minDigestWithID := heap.Pop(&pq).(*hashDigestWithID)
		// if true: empty, i.e. this thread has gone through all its hashes on file
		// remove this thread and continue without it (could also decrease pq, but not implemented)
		if minDigestWithID.Digest == (hashDigest{}) {
			blockResults[minDigestWithID.Id] = blockResults[len(blockResults)-1]
			blockResults = blockResults[0 : len(blockResults)-1]
		}

		// send min result to "main thread" through this channel
		// will send empty hashDigest when finished
		minResult <- minDigestWithID.Digest
	}

}

func mergeThread(startBlockID, endBlockID, bufferSizePerBlock int, filename string, minResult chan hashDigest) {
	amountOfBlocks := endBlockID - startBlockID + 1
	blockReaders := make([]*reverseFileReader, amountOfBlocks)

	// init the readers by reading in the first hashes from file into buffers
	// and also putting the "smallest" hash from every block into the pq
	for i := 0; i < amountOfBlocks; i++ {
		blockReaders[i] = NewReverseFileReader(startBlockID+i, filename, bufferSizePerBlock)
		(*blockReaders[i]).Refill()
	}

	// will loop until there are no more hashes on disk from these blocks
	for {
		minIndex := 0
		minDigest := (*blockReaders[0]).Peek()

		for i := 0; i < amountOfBlocks; i++ {
			currentDigest := (*blockReaders[i]).Peek()
			if currentDigest.Less(minDigest) {
				minIndex = i
				minDigest = currentDigest
			}
		}

		minDigest, err := (*blockReaders[minIndex]).Read()
		if err != nil {
			// return an empty hashDigest as an indicator for the main thread to stop fetching hashes from this thread
			// the main thread can then see that the filesize is incorrect and in that way see that an error has occurred
			// TODO: a better way to do this error detection, so that it can be traced back to here
			minResult <- hashDigest{}
			break
		}

		minResult <- minDigest
		// if minDigest is an empty hashDigest, there are no hashes left to compare, exit
		if minDigest == (hashDigest{}) {
			break
		}
	}
}
