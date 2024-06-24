package ui

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type App struct {
	tviewApp        *tview.Application
	resultContainer *ScrollBox
	queryTextArea   *tview.TextArea
	db              *db.DBClient
}

const (
	ColorPrimary   = tcell.ColorWhite
	ColorSecondary = tcell.ColorLightGray
	ColorError     = tcell.ColorRed
)

func MustGetScreenHeight() (height int) {
	s, err := tcell.NewScreen()
	if err != nil {
		panic(fmt.Sprintf("Could not determine screen height for rendering\n%+v", err))
	}

	_, height = s.Size()
	return height
}

// Setup initial layout and application structure
func Init(db *db.DBClient) *App {
	tviewApp := tview.NewApplication().EnableMouse(true)

	queryTextArea := tview.NewTextArea().SetTextStyle(tcell.StyleDefault.Foreground(ColorSecondary))
	queryTextArea.SetTitle("Query").SetBorder(true)

	resultContainer := NewScrollBox()
	screenHeight := MustGetScreenHeight()

	box := tview.NewFlex().
		SetFullScreen(true).
		SetDirection(tview.FlexRow).
		AddItem(resultContainer, screenHeight-5, 4, false).
		AddItem(queryTextArea, 5, 1, true)

	tviewApp.SetRoot(box, true)

	app := App{
		tviewApp:        tviewApp,
		resultContainer: resultContainer,
		queryTextArea:   queryTextArea,
		db:              db,
	}

	return &app
}

// Register listeners and run live app
func (app *App) Run() (err error) {
	app.queryTextArea.SetInputCapture(app.handleInputCapture)

	return app.tviewApp.Run()
}

var newlineRegexp = regexp.MustCompile("\n")

func getTextLineCount(text string) (lines int) {
	lines = 1

	newLineChars := len(
		newlineRegexp.FindAllStringSubmatchIndex(text, -1),
	)
	lines += newLineChars

	return lines
}

func (app *App) commitQuery(query string) {
	formattedQueryText := fmt.Sprint("> ", query)

	queryTextItem := tview.
		NewTextView().
		SetText(formattedQueryText).
		SetTextColor(ColorSecondary).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	results, err := app.db.Query(query)
	var resultItem tview.Primitive
	var height int

	if err != nil {
		resultItem, height = app.createErrorView(err)
	} else if results != nil && len(results.Columns) > 0 {
		resultItem, height = app.createResultView(results)
	} else {
		resultItem, height = app.createNoResultView()
	}

	app.resultContainer.AddItem(
		queryTextItem,
		getTextLineCount(query),
	)
	app.resultContainer.AddItem(
		resultItem,
		height,
	)
}

func (app *App) createErrorView(dbErr error) (view *tview.TextView, lines int) {
	errorTextItem := tview.
		NewTextView().
		SetText(fmt.Sprint(dbErr, "\n")).
		SetTextColor(ColorError).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		}).
		SetWrap(true)

	return errorTextItem, getTextLineCount(errorTextItem.GetText(false))
}

func (app *App) createNoResultView() (view *tview.TextView, lines int) {
	noResultsTextItem := tview.
		NewTextView().
		SetText("Success: 0 results returned\n").
		SetTextColor(ColorPrimary).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	return noResultsTextItem, getTextLineCount(noResultsTextItem.GetText(false))
}

func (app *App) createResultView(result *db.QueryResult) (view *tview.Table, lines int) {
	resultTable := tview.NewTable().
		SetSeparator(tview.Borders.Vertical).
		SetBorders(true)

	for columnIdx, column := range result.Columns {
		resultTable.SetCell(
			0,
			columnIdx,
			tview.NewTableCell(column).
				SetAlign(tview.AlignLeft),
		)
	}

	for rowIdx, row := range result.Rows {
		rowIdx := rowIdx + 1
		for columnIdx, column := range result.Columns {
			cellValue := row[column]
			if cellValue == "" {
				cellValue = "NULL"
			}

			resultTable.SetCell(
				rowIdx,
				columnIdx,
				tview.NewTableCell(cellValue).SetAttributes(tcell.AttrDim),
			)

		}
	}

	height := len(result.Rows)*2 + 5

	return resultTable, int(height)
}

// Intercept text area key presses for shortcuts or committing querys
func (app *App) handleInputCapture(event *tcell.EventKey) *tcell.EventKey {
	isNotShortcut := event.Modifiers() != tcell.ModCtrl && event.Modifiers() != tcell.ModAlt

	// Handle committing the query, if applicable
	if isNotShortcut {
		query := app.queryTextArea.GetText()
		queryLen := len(strings.TrimSpace(query))
		pressedEnter := event.Key() == tcell.KeyEnter

		var lastChar rune
		if queryLen > 0 {
			lastChar = rune(query[len(query)-1])
		}

		shouldCommitQuery := pressedEnter && lastChar == ';' && queryLen > 0
		if shouldCommitQuery {
			app.commitQuery(query)
			app.queryTextArea.SetText("", false)

			return nil
		}

		return event
	}

	// Handle shortcuts
	switch event.Key() {
	case tcell.KeyUp:
		{
			app.resultContainer.ScrollUp()
			return nil
		}
	case tcell.KeyDown:
		{
			app.resultContainer.ScrollDown()
			return nil
		}
	case tcell.KeyLeft:
		{
			app.resultContainer.ScrollLeft()
			return nil
		}
	case tcell.KeyRight:
		{
			app.resultContainer.ScrollRight()
			return nil
		}
	}

	return event
}
