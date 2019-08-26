package createblocks

import "sync"

type syncPriorityQueue struct {
	Hashes []hashDigest
	mut    sync.Mutex
}

func (pq *syncPriorityQueue) Push(digest interface{}) {
	pq.mut.Lock()
	defer pq.mut.Unlock()

	hashes := (*pq).Hashes
	hashes = append(hashes, digest.(hashDigest))
}
func (pq *syncPriorityQueue) Pop() interface{} {
	pq.mut.Lock()
	defer pq.mut.Unlock()

	hashes := (*pq).Hashes
	length := len(hashes)
	result := (hashes)[length-1]
	hashes = (hashes)[:length-2]
	return result
}
func (pq syncPriorityQueue) Len() int {
	pq.mut.Lock()
	defer pq.mut.Unlock()

	return len(pq.Hashes)
}
func (pq syncPriorityQueue) Less(i, j int) bool {
	pq.mut.Lock()
	defer pq.mut.Unlock()

	for k := CompStart; k < CompStart+CompLength; k++ {
		currentI, currentJ := pq.Hashes[i][k], pq.Hashes[j][k]
		if currentI < currentJ {
			return true
		} else if currentI > currentJ {
			return false
		}
	}
	return false // equal
}
func (pq *syncPriorityQueue) Swap(i, j int) {
	pq.mut.Lock()
	defer pq.mut.Unlock()

	hashes := (*pq).Hashes
	hashes[i], hashes[j] = hashes[j], hashes[i]
}
