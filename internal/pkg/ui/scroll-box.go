package ui

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type scrollBoxItem struct {
	Item        tview.Primitive
	FixedHeight int
}

type ScrollBox struct {
	*tview.Box
	items  []*scrollBoxItem
	offset int
}

func NewScrollBox() *ScrollBox {
	scrollBox := &ScrollBox{
		Box: tview.NewBox(),
	}

	return scrollBox
}

func (scrollBox *ScrollBox) AddItem(item tview.Primitive, fixedHeight int) *ScrollBox {
	scrollBox.items = append(scrollBox.items, &scrollBoxItem{
		Item:        item,
		FixedHeight: fixedHeight,
	})

	return scrollBox
}

func (scrollBox *ScrollBox) ClearItems() *ScrollBox {
	scrollBox.items = nil
	return scrollBox
}

// Offset is relative to the bottom
func (scrollBox *ScrollBox) SetOffset(offset int) *ScrollBox {
	scrollBox.offset = offset
	return scrollBox
}

func (scrollBox *ScrollBox) Draw(screen tcell.Screen) {
	scrollBox.Box.DrawForSubclass(screen, scrollBox)

	var itemSizeSum int
	for _, item := range scrollBox.items {
		itemSizeSum += item.FixedHeight
	}

	// NOTE: Y axis is represented in tview as the number gets larger as the position is lower
	// This y is representing the topmost point of the space we have available
	x, y, width, height := scrollBox.GetInnerRect()
	currentY := y

	// If it's going to overflow, we'll start drawing above
	willOverflow := itemSizeSum > height
	if willOverflow {
		// The lowest Y in our container is the top most point (y) + the height of our container
		lowestYAvailable := y + height
		// We want to start drawing so that the last item would end up on the lowest point available
		currentY = lowestYAvailable - itemSizeSum
	}

	for _, item := range scrollBox.items {
		if item.Item != nil {
			item.Item.SetRect(x, currentY, width, item.FixedHeight)
			item.Item.Draw(screen)
		}

		currentY += item.FixedHeight
	}
}
