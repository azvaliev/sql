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
	scrollBox.setOffset(0)

	return scrollBox
}

func (scrollBox *ScrollBox) ClearItems() *ScrollBox {
	scrollBox.items = nil
	return scrollBox
}

// Offset is relative to the bottom
// Internal setter to control offset logic
func (scrollBox *ScrollBox) setOffset(offset int) *ScrollBox {
	itemSizeSum := scrollBox.getItemSizeSum()
	_, _, _, height := scrollBox.GetInnerRect()

	maxOffset := itemSizeSum - height
	minOffset := 0

	computedOffset := offset

	// Clamp computedOffset so we're not scrolling past the results
	{
		if computedOffset > maxOffset {
			computedOffset = maxOffset
		}
		if computedOffset < minOffset {
			computedOffset = minOffset
		}
	}

	scrollBox.offset = computedOffset

	return scrollBox
}

func (scrollBox *ScrollBox) getItemSizeSum() (itemSizeSum int) {
	for _, item := range scrollBox.items {
		itemSizeSum += item.FixedHeight
	}

	return itemSizeSum
}

func (scrollBox *ScrollBox) Draw(screen tcell.Screen) {
	scrollBox.Box.DrawForSubclass(screen, scrollBox)

	itemSizeSum := scrollBox.getItemSizeSum()

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

		// If we have offset, we should start drawing lower by offset amount
		currentY += scrollBox.offset
	}

	for _, item := range scrollBox.items {
		if item.Item != nil {
			item.Item.SetRect(x, currentY, width, item.FixedHeight)
			item.Item.Draw(screen)
		}

		currentY += item.FixedHeight
	}
}

func (scrollBox *ScrollBox) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
	return scrollBox.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
		switch action {
		case tview.MouseScrollDown:
			{
				scrollBox.setOffset(scrollBox.offset - 5)
				consumed = true
				break
			}
		case tview.MouseScrollUp:
			{
				scrollBox.setOffset(scrollBox.offset + 5)
				consumed = true
				break
			}
		}

		return consumed, capture
	})
}
