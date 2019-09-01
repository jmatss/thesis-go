package createsortedwordlist

import (
	"container/heap"
	"log"

	"github.com/jmatss/thesis-go/pkg/model"
)

func MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string, minResult chan model.HashDigest) {
	if amountOfBlocks < concurrentThreads {
		concurrentThreads = amountOfBlocks
	}

	var pq model.PriorityQueue
	blockResults := make([]chan model.HashDigest, concurrentThreads)

	blockRange := (amountOfBlocks / concurrentThreads) - 1 // ~amount of blocks per worker
	currentStart := 0
	for i := 0; i < concurrentThreads; i++ {
		currentEnd := currentStart + blockRange
		if i == concurrentThreads-1 { // last iteration
			currentEnd = amountOfBlocks - 1
		}

		blockResults[i] = make(chan model.HashDigest)
		go mergeThread(currentStart, currentEnd, blockBufferSize/amountOfBlocks, filename, blockResults[i])

		currentStart = currentEnd + 1
	}

	// load smallest hash from all threads into pq
	for i, blockResult := range blockResults {
		heap.Push(&pq, &model.HashDigestWithID{i, <-blockResult})
	}

	for {
		minDigestWithID := heap.Pop(&pq).(*model.HashDigestWithID)

		// send min result to "main thread" through this channel
		// will send empty hashDigest when finished
		minResult <- minDigestWithID.Digest

		// if the min is empty, all elements in the pq are empty and all "mergeThread"s are done, exit
		if minDigestWithID.Digest == (model.HashDigest{}) {
			break
		}
		// else, add next digest from same thread into pq
		heap.Push(&pq, &model.HashDigestWithID{minDigestWithID.Id, <-blockResults[minDigestWithID.Id]})
	}
}

func mergeThread(startBlockID, endBlockID, bufferSizePerBlock int, filename string, minResult chan model.HashDigest) {
	amountOfBlocks := endBlockID - startBlockID + 1
	blockReaders := make([]*model.ReverseFileReader, amountOfBlocks)

	// init the readers by reading in the first hashes from file into buffers
	// and also putting the "smallest" hash from every block into the pq
	for i := 0; i < amountOfBlocks; i++ {
		blockReaders[i] = model.NewReverseFileReader(startBlockID+i, filename, bufferSizePerBlock)
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
			minResult <- model.HashDigest{}
			break
		}

		minResult <- minDigest
		// if minDigest is an empty hashDigest, there are no hashes left to compare, exit
		if minDigest == (model.HashDigest{}) {
			break
		}
	}

	log.Printf(" mergeThread for blocks %d through %d done.", startBlockID, endBlockID)
}
