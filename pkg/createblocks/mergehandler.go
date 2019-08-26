package createblocks

func MergeHandler(amountOfBlocks, concurrentThreads, blockBufferSize int, filename string, pq syncPriorityQueue) {
	if amountOfBlocks < concurrentThreads {
		concurrentThreads = amountOfBlocks
	}

	blockResults := make([]chan hashDigest, concurrentThreads)

	blockRange := amountOfBlocks / concurrentThreads // ~amount of blocks per worker
	currentStart := 0
	for i := 0; i < concurrentThreads; i++ {
		currentEnd := currentStart + blockRange
		if i == concurrentThreads-1 { // last iteration
			currentEnd = amountOfBlocks - 1
		}

		// TODO: FIX
		go mergeThread(currentStart, currentEnd, blockBufferSize/amountOfBlocks, filename, blockResults)
	}

}

func mergeThread(startBlockID, endBlockID, bufferSizePerBlock int, filename string, blockResults []chan hashDigest) {

}
