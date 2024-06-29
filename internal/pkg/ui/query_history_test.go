package ui_test

import (
	"testing"

	"github.com/azvaliev/sql/internal/pkg/ui"
	"github.com/stretchr/testify/assert"
)

func TestQueryHistory(t *testing.T) {
	// Build query history data
	size := 3
	queryHistory := ui.NewQueryHistory(size)
	initialItems := []string{"item 1", "item 2", "item 3"}
	newItem := "override item 1"
	orderedFinalItems := []string{newItem, initialItems[2], initialItems[1]}

	for _, item := range initialItems {
		queryHistory.AddEntry(item)
	}

	t.Run("Test GetPrevEntry", func(t *testing.T) {
		assert := assert.New(t)

		for i := range size {
			// The first item in this array should be last in initial items, etc
			itemIdx := len(initialItems) - 1 - i

			expectedEntry := initialItems[itemIdx]
			actualEntry := queryHistory.GetPrevEntry()

			assert.Equal(expectedEntry, actualEntry, queryHistory)
		}

		emptyEntry := queryHistory.GetPrevEntry()
		assert.Empty(
			emptyEntry,
			"After looping past the end, it should be empty",
			queryHistory,
		)
	})

	t.Run("Test GetNextEntry", func(t *testing.T) {
		assert := assert.New(t)

		for i := range size - 1 {
			// We're going in reverse, so this item order should be the same, but we'll skip the first item
			itemIdx := i + 1

			expectedEntry := initialItems[itemIdx]
			actualEntry := queryHistory.GetNextEntry()

			assert.Equal(expectedEntry, actualEntry, queryHistory)
		}
	})

	t.Run("Test overflowing size", func(t *testing.T) {
		assert := assert.New(t)
		// Offset the readindex to make sure it resets
		queryHistory.GetPrevEntry()

		queryHistory.AddEntry(newItem)

		// Check the item order
		for i := range size {
			expectedItem := orderedFinalItems[i]
			actualItem := queryHistory.GetPrevEntry()

			assert.Equal(expectedItem, actualItem, queryHistory)
		}

		// this should be out of bounds
		emptyEntry := queryHistory.GetPrevEntry()
		assert.Empty(emptyEntry, "this entry should be empty, out of bounds", queryHistory)
	})

	t.Run("Manually reset position", func(t *testing.T) {
		assert := assert.New(t)

		// Add an offset
		queryHistory.ResetPosition()
		queryHistory.GetPrevEntry()
		queryHistory.GetPrevEntry()

		// Reset the position should start from the start now
		queryHistory.ResetPosition()

		for i := range size {
			expectedItem := orderedFinalItems[i]
			actualItem := queryHistory.GetPrevEntry()

			assert.Equal(expectedItem, actualItem, queryHistory)
		}
	})

}
