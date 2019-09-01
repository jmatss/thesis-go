package createblocks

// TODO: Make priorityqueue synchronized and blocking!

type hashDigestWithID struct {
	Id     int
	Digest hashDigest
}

type priorityQueue []*hashDigestWithID

func (pq *priorityQueue) Push(digest interface{}) {
	*pq = append(*pq, digest.(*hashDigestWithID))
}

func (pq *priorityQueue) Pop() interface{} {
	length := len(*pq)
	result := (*pq)[length-1]
	*pq = (*pq)[:length-1]
	return result
}

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	// Treat an empty hashDigest as greater than,
	// this allows non empty hashDigest to "float to the top" and be pop'ed before empty ones
	if pq[i].Digest == (hashDigest{}) {
		return false
	} else if pq[j].Digest == (hashDigest{}) {
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

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
