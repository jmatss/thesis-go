package createsortedwordlist

import (
	"container/heap"

	"github.com/jmatss/thesis-go/pkg/model"
)

// Takes care of all comparison logic during merging.
// Creates multiple goprocess that will do parts of the comparison and return the results to this handler.
// The handler will return the "smallest" hash of those to the main thread via a channel.
func MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string, minResult chan model.HashDigest) {
	// prevent deadlock if panic
	defer func() {
		if r := recover(); r != nil {
			minResult <- model.HashDigest{}
		}
	}()

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

		blockResults[i] = make(chan model.HashDigest, ChanSize/concurrentThreads)
		go mergeThread(currentStart, currentEnd, blockBufferSize/amountOfBlocks, filename, blockResults[i])

		// load smallest hash from thread into pq
		heap.Push(&pq, &model.HashDigestWithID{i, <-blockResults[i]})

		currentStart = currentEnd + 1
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

	for i := 0; i < len(blockResults); i++ {
		close(blockResults[i])
	}
}

// A goprocess that will do all comparisons and reads from blocks "startBlockID" through "endBlockID".
// It will return the current "smallest" one of all its blocks to its parent "MergeHandler" via a channel.
func mergeThread(startBlockID, endBlockID, bufferSizePerBlock int, filename string, minResult chan model.HashDigest) {
	// prevent deadlock if panic
	defer func() {
		if r := recover(); r != nil {
			minResult <- model.HashDigest{}
		}
	}()

	amountOfBlocks := endBlockID - startBlockID + 1
	blockReaders := make([]*model.ReverseFileReader, amountOfBlocks)
	var pq model.PriorityQueue

	// init the readers by reading in the first hashes from file into buffers
	// and also putting the "smallest" hash from every block into the pq
	// TODO: a better way to do this error detection, so that it can be traced back to here
	for i := 0; i < amountOfBlocks; i++ {
		blockReaders[i] = model.NewReverseFileReader(startBlockID+i, filename, bufferSizePerBlock)

		if err := (*blockReaders[i]).Refill(); err != nil {
			minResult <- model.HashDigest{}
			break
		}

		digest, err := (*blockReaders[i]).Read()
		if err != nil {
			minResult <- model.HashDigest{}
			break
		}
		heap.Push(&pq, &model.HashDigestWithID{i, digest})
	}

	// will loop until there are no more hashes on disk from these blocks
	for {
		min := heap.Pop(&pq).(*model.HashDigestWithID)
		minResult <- min.Digest

		if min.Digest == (model.HashDigest{}) {
			break
		}

		// The Read function will return an empty HashDigest if an error is returned,
		//  so there is no reason to check the error since we would insert an empty
		//  HashDigest into the heap anyways
		minFromSameBlock, _ := (*blockReaders[min.Id]).Read()

		heap.Push(&pq, &model.HashDigestWithID{min.Id, minFromSameBlock})
	}
}
