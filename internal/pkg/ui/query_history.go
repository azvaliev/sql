package ui

type QueryHistory struct {
	queryList []string
	size      int
	writeIdx  int
	readIdx   int
}

func NewQueryHistory(size int) *QueryHistory {
	queryList := make([]string, size)
	return &QueryHistory{
		queryList: queryList,
		size:      size,
		writeIdx:  0,
		readIdx:   0,
	}
}

func (queryHistory *QueryHistory) AddEntry(entry string) {
	queryHistory.ResetPosition()
	queryHistory.queryList[queryHistory.writeIdx] = entry
	queryHistory.writeIdx += 1

	// Wrap around once we're about to exceed length
	if queryHistory.writeIdx >= queryHistory.size {
		queryHistory.writeIdx = 0
	}
}

func (queryHistory *QueryHistory) GetPrevEntry() (entry string) {
	// Going past the write idx again will loop over
	if queryHistory.readIdx == queryHistory.writeIdx {
		return ""
	}

	if !queryHistory.IsPositionSet() {
		queryHistory.readIdx = queryHistory.writeIdx
	}

	queryHistory.readIdx = queryHistory.changeIdx(queryHistory.readIdx, -1)
	result := queryHistory.queryList[queryHistory.readIdx]
	if result == "" {
		// Undo the change if we went to unitilized items, or have looped full circle
		queryHistory.readIdx = queryHistory.changeIdx(queryHistory.readIdx, +1)
	}

	return queryHistory.queryList[queryHistory.readIdx]
}

func (queryHistory *QueryHistory) GetNextEntry() (entry string) {
	if !queryHistory.IsPositionSet() {
		return ""
	}

	alreadyAtLatestEntry := queryHistory.readIdx == queryHistory.writeIdx-1
	if alreadyAtLatestEntry {
		return ""
	}

	queryHistory.readIdx = queryHistory.changeIdx(queryHistory.readIdx, +1)

	return queryHistory.queryList[queryHistory.readIdx]
}

func (queryHistory *QueryHistory) IsPositionSet() bool {
	return queryHistory.readIdx != -1
}

// When the user is done paginating
func (queryHistory *QueryHistory) ResetPosition() {
	queryHistory.readIdx = -1
}

// Change an index within the items array, moving forward or back
// Loop around as needed
func (queryHistory *QueryHistory) changeIdx(idx int, diff int) (newIdx int) {
	if diff > queryHistory.size-1 {
		return idx
	}

	updatedIdx := idx + diff

	if updatedIdx < 0 {
		updatedIdx = queryHistory.size + updatedIdx
	}
	if updatedIdx >= queryHistory.size {
		updatedIdx -= queryHistory.size
	}

	return updatedIdx
}
