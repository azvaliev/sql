package ui

import (
	"github.com/azvaliev/sql/internal/pkg/ui/components"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	ColorPrimary    = tcell.ColorWhite
	ColorSecondary  = tcell.ColorLightGray
	ColorBackground = tcell.Color235
	ColorError      = tcell.ColorRed
)

type TextViewVariant int

const (
	TextViewPrimary TextViewVariant = iota + 1
	TextViewSecondary
	TextViewError
)

func NewTextView(variant TextViewVariant) *tview.TextView {
	textView := tview.NewTextView()
	textView.SetBackgroundColor(tcell.ColorNone)

	switch variant {
	case TextViewPrimary:
		{
			textView.SetTextColor(ColorPrimary)
			break
		}
	case TextViewSecondary:
		{
			textView.SetTextColor(ColorSecondary)
			break
		}
	case TextViewError:
		{
			textView.SetTextColor(ColorError)
			break
		}
	}

	return textView

}

func NewTextArea() *tview.TextArea {
	textArea := tview.
		NewTextArea().
		SetTextStyle(
			tcell.
				StyleDefault.
				Foreground(ColorSecondary).
				Background(ColorBackground),
		)

	textArea.SetBackgroundColor(ColorBackground)

	return textArea
}

func NewFlex() *tview.Flex {
	box := tview.NewFlex()
	box.SetBackgroundColor(ColorBackground)

	return box
}

func NewGrid() *tview.Grid {
	box := tview.NewGrid()
	box.SetBackgroundColor(ColorBackground)

	return box
}

func NewTable() *tview.Table {
	table := tview.
		NewTable().
		SetSeparator(tview.Borders.Vertical).
		SetBorders(true)

	table.SetBackgroundColor(tcell.ColorNone)

	return table
}

func NewScrollBox() *components.ScrollBox {
	scrollBox := components.NewScrollBox()
	scrollBox.SetBackgroundColor(ColorBackground)

	return scrollBox
}

func NewButton(label string) *tview.Button {
	return tview.
		NewButton(label).
		SetStyle(buttonStyle).
		SetActivatedStyle(buttonActiveStyle).
		SetDisabledStyle(buttonDisabledStyle)
}

var buttonStyle tcell.Style = tcell.
	StyleDefault.
	Background(tcell.ColorNone).
	Foreground(ColorPrimary).
	Underline(true)

var buttonDisabledStyle tcell.Style = tcell.
	StyleDefault.
	Background(tcell.ColorNone).
	Foreground(ColorSecondary).
	StrikeThrough(true)

var buttonActiveStyle tcell.Style = tcell.
	StyleDefault.
	Background(tcell.ColorNone).
	Foreground(tcell.ColorBlue).
	Underline(true)
