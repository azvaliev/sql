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
	items   []*scrollBoxItem
	yOffset int
	// Scroll all table items
	xOffset int
}

func NewScrollBox() *ScrollBox {
	scrollBox := &ScrollBox{
		Box:     tview.NewBox(),
		yOffset: 0,
		xOffset: 0,
	}

	return scrollBox
}

func (scrollBox *ScrollBox) AddItem(item tview.Primitive, fixedHeight int) *ScrollBox {
	scrollBox.items = append(scrollBox.items, &scrollBoxItem{
		Item:        item,
		FixedHeight: fixedHeight,
	})
	scrollBox.ClearOffsets()

	return scrollBox
}

func (scrollBox *ScrollBox) ClearItems() *ScrollBox {
	scrollBox.items = nil
	return scrollBox
}

func (scrollBox *ScrollBox) ClearOffsets() *ScrollBox {
	scrollBox.yOffset = 0
	scrollBox.xOffset = 0

	return scrollBox
}

const xOffsetScrollFactor = 2

func (scrollBox *ScrollBox) ScrollRight() {
	scrollBox.setXOffset(scrollBox.xOffset + xOffsetScrollFactor)
}

func (scrollBox *ScrollBox) ScrollLeft() {
	scrollBox.setXOffset(scrollBox.xOffset - xOffsetScrollFactor)
}

const yOffsetScrollFactor = 5

func (scrollBox *ScrollBox) ScrollUp() {
	scrollBox.setYOffset(scrollBox.yOffset + yOffsetScrollFactor)
}

func (scrollBox *ScrollBox) ScrollDown() {
	scrollBox.setYOffset(scrollBox.yOffset - yOffsetScrollFactor)
}

// X offset is relative to the left
// Internal setter to control offset logic
func (scrollBox *ScrollBox) setXOffset(offset int) *ScrollBox {
	minOffset := 0
	var maxOffset int
	// Get max item offset for table scrolling
	for _, item := range scrollBox.items {
		switch v := item.Item.(type) {
		case *tview.Table:
			{
				colCount := v.GetColumnCount()
				if colCount > maxOffset {
					maxOffset = colCount
				}

				break
			}
		}
	}

	computedOffset := offset

	if offset < minOffset {
		computedOffset = minOffset
	} else if offset > maxOffset {
		computedOffset = maxOffset
	}

	scrollBox.xOffset = computedOffset
	return scrollBox
}

// Offset is relative to the bottom
// Internal setter to control offset logic
func (scrollBox *ScrollBox) setYOffset(offset int) *ScrollBox {
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

	scrollBox.yOffset = computedOffset

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
		currentY += scrollBox.yOffset
	}

	for _, item := range scrollBox.items {
		if item.Item != nil {
			// Handle x offsets
			switch v := item.Item.(type) {
			case *tview.Table:
				{
					v.SetOffset(0, scrollBox.xOffset)
					break
				}
			}

			item.Item.SetRect(x, currentY, width, item.FixedHeight)
			item.Item.Draw(screen)
		}

		currentY += item.FixedHeight
	}
}

func (scrollBox *ScrollBox) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return scrollBox.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Key() {
		case tcell.KeyUp:
			{
				scrollBox.ScrollUp()
				break
			}
		case tcell.KeyDown:
			{
				scrollBox.ScrollDown()
				break
			}
		case tcell.KeyLeft:
			{
				scrollBox.ScrollLeft()
				break
			}
		case tcell.KeyRight:
			{
				scrollBox.ScrollRight()
				break
			}
		}
	})
}

func (scrollBox *ScrollBox) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
	return scrollBox.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
		switch action {
		case tview.MouseLeftDoubleClick:
		case tview.MouseLeftClick:
			{
				setFocus(scrollBox)
				break
			}
		case tview.MouseScrollDown:
			{
				scrollBox.ScrollDown()
				consumed = true
				break
			}
		case tview.MouseScrollUp:
			{
				scrollBox.ScrollUp()
				consumed = true
				break
			}
		case tview.MouseScrollRight:
			{
				scrollBox.ScrollRight()
				consumed = true
				break
			}
		case tview.MouseScrollLeft:
			{
				scrollBox.ScrollLeft()
				consumed = true
				break
			}
		}

		return consumed, capture
	})
}
