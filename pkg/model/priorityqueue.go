package model

// TODO: Make priorityqueue synchronized and blocking!

type HashDigestWithID struct {
	Id     int
	Digest HashDigest
}

type PriorityQueue []*HashDigestWithID

func (pq *PriorityQueue) Push(digest interface{}) {
	*pq = append(*pq, digest.(*HashDigestWithID))
}

func (pq *PriorityQueue) Pop() interface{} {
	length := len(*pq)
	result := (*pq)[length-1]
	*pq = (*pq)[:length-1]
	return result
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	// Treat an empty HashDigest as greater than,
	// this allows non empty HashDigest to "float to the top" and be pop'ed before empty ones
	if pq[i].Digest == (HashDigest{}) {
		return false
	} else if pq[j].Digest == (HashDigest{}) {
		return true
	}

	for k := CompStart; k < CompStart+CompLength; k++ {
		currentI, currentJ := (*pq[i]).Digest[k], (*pq[j]).Digest[k]
		if currentI < currentJ {
			return true
		} else if currentI > currentJ {
			return false
		}
	}
	return false // equal
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
