package createblocks

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
func (pq *priorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}
