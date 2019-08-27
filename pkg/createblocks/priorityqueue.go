package createblocks

import (
	"container/heap"
	"sync"
)

type priorityQueue []hashDigest

func (pq *priorityQueue) Push(digest interface{}) {
	*pq = append(*pq, digest.(hashDigest))
}
func (pq *priorityQueue) Pop() interface{} {
	length := len(*pq)
	result := (*pq)[length-1]
	*pq = (*pq)[:length-2]
	return result
}
func (pq priorityQueue) Len() int {
	return len(pq)
}
func (pq priorityQueue) Less(i, j int) bool {
	for k := CompStart; k < CompStart+CompLength; k++ {
		currentI, currentJ := pq[i][k], pq[j][k]
		if currentI < currentJ {
			return true
		} else if currentI > currentJ {
			return false
		}
	}
	return false // equal
}
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

/*
	A synchronized pq that wraps a regular pq with a lock
*/
type syncPriorityQueue struct {
	pq  *priorityQueue
	mut sync.Mutex
}

func (spq *syncPriorityQueue) Push(digest interface{}) {
	spq.mut.Lock()
	defer spq.mut.Unlock()

	heap.Push(spq.pq, digest)
}
func (spq *syncPriorityQueue) Pop() interface{} {
	spq.mut.Lock()
	defer spq.mut.Unlock()

	return heap.Pop(spq.pq)
}

/*
	As long as the Len, Less and Swap functions are called from the Push and Pop functions,
	there are no reason to lock them since they are already indirectly locked
*/
func (spq syncPriorityQueue) Len() int {
	return len(*spq.pq)
}
func (spq syncPriorityQueue) Less(i, j int) bool {
	for k := CompStart; k < CompStart+CompLength; k++ {
		currentI, currentJ := (*spq.pq)[i][k], (*spq.pq)[j][k]
		if currentI < currentJ {
			return true
		} else if currentI > currentJ {
			return false
		}
	}
	return false // equal
}
func (spq syncPriorityQueue) Swap(i, j int) {
	(*spq.pq)[i], (*spq.pq)[j] = (*spq.pq)[j], (*spq.pq)[i]
}
