package createblocks

type PriorityQueue []hashDigest

func (pq *PriorityQueue) Push(digest interface{}) {
	*pq = append(*pq, digest.(hashDigest))
}
func (pq *PriorityQueue) Pop() interface{} {
	length := len(*pq)
	result := (*pq)[length-1]
	*pq = (*pq)[:length-2]
	return result
}
func (pq PriorityQueue) Len() int {
	return len(pq)
}
func (pq PriorityQueue) Less(i, j int) bool {
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
func (pq *PriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}
