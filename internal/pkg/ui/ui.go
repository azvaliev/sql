package ui

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/azvaliev/sql/internal/pkg/ui/components"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rivo/uniseg"
	"golang.design/x/clipboard"
)

type App struct {
	tviewApp        *tview.Application
	resultContainer *components.ScrollBox
	queryTextArea   *tview.TextArea
	db              *db.DBClient
}

func MustGetScreenDimensions() (width, height int) {
	s, err := tcell.NewScreen()
	if err != nil {
		panic(fmt.Sprintf("Could not determine screen height for rendering\n%+v", err))
	}

	width, height = s.Size()
	return width, height
}

// Setup initial layout and application structure
func Init(db *db.DBClient) *App {
	tviewApp := tview.NewApplication().EnableMouse(true)

	queryTextArea := NewTextArea()
	queryTextArea.SetTitle("Query").SetBorder(true)

	resultContainer := NewScrollBox()
	_, screenHeight := MustGetScreenDimensions()

	box := NewFlex().
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

func getTextLineCount(textView *tview.TextView, maxWidth int) int {
	if maxWidth <= 0 {
		_, _, maxWidth, _ = textView.GetInnerRect()
	}

	// Get string width, in the same units as tview uses
	totalStringCharsWidth := float64(
		uniseg.StringWidth(textView.GetText(true)),
	)

	lines := math.Ceil(totalStringCharsWidth / float64(maxWidth))
	return int(lines)
}

func (app *App) commitQuery(query string) {
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

	queryViewWithActions, queryViewWithActionsHeight := app.createQueryViewWithActions(
		query,
		results,
		err,
	)

	app.resultContainer.AddItem(
		queryViewWithActions,
		queryViewWithActionsHeight,
	)
	app.resultContainer.AddItem(
		resultItem,
		height,
	)
}

func mustInitClipboard() {
	err := clipboard.Init()

	// TODO: handle this gracefully?
	if err != nil {
		panic(errors.Join(
			errors.New("no access to clipboard"),
			err,
		))
	}
}

func (app *App) createQueryViewWithActions(
	query string,
	queryResult *db.QueryResult,
	queryError error,
) (queryView *tview.Grid, fixedHeight int) {
	formattedQueryText := fmt.Sprint("> ", query)

	_, _, containerWidth, _ := app.resultContainer.GetInnerRect()
	queryTextItemWidth := containerWidth / 2

	queryTextItem := NewTextView(TextViewSecondary).
		SetText(formattedQueryText).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		}).
		SetWrap(true).
		SetWordWrap(true)

	textItemHeight := getTextLineCount(queryTextItem, queryTextItemWidth)

	shouldDisableCopyResultsButtons := false
	if queryError != nil {
		shouldDisableCopyResultsButtons = true
	}
	if queryResult == nil || len(queryResult.Columns) == 0 {
		shouldDisableCopyResultsButtons = true
	}

	queryCopyCSVButton := NewButton("Copy as CSV").
		SetDisabled(shouldDisableCopyResultsButtons).
		SetSelectedFunc(func() {
			mustInitClipboard()

			resultCSV := queryResult.ToCSV()
			clipboard.Write(clipboard.FmtText, resultCSV)
		})

	queryCopyJSONButton := NewButton("Copy as JSON").
		SetDisabled(shouldDisableCopyResultsButtons).
		SetSelectedFunc(func() {
			mustInitClipboard()

			resultJSON := queryResult.ToJSON()
			clipboard.Write(clipboard.FmtText, resultJSON)
		})

	queryView = NewGrid().
		SetRows(3).
		SetColumns(
			queryTextItemWidth,
			0,
			len(queryCopyCSVButton.GetLabel()),
			len(queryCopyJSONButton.GetLabel()),
		).
		SetGap(0, 2)

	queryView.AddItem(
		queryTextItem,
		0,
		0,
		1,
		1,
		0,
		0,
		false,
	)
	queryView.AddItem(
		queryCopyCSVButton,
		0,
		2,
		1,
		1,
		0,
		0,
		true,
	)
	queryView.AddItem(
		queryCopyJSONButton,
		0,
		3,
		1,
		1,
		0,
		0,
		true,
	)

	return queryView, textItemHeight
}

func (app *App) createErrorView(dbErr error) (view *tview.TextView, lines int) {
	errorTextItem := NewTextView(TextViewError).
		SetText(fmt.Sprint(dbErr, "\n")).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		}).
		SetWrap(true)

	_, _, containerWidth, _ := app.resultContainer.GetInnerRect()
	textLines := getTextLineCount(errorTextItem, containerWidth)
	linesWithSpacing := textLines + 2

	return errorTextItem, linesWithSpacing
}

func (app *App) createNoResultView() (view *tview.TextView, lines int) {
	noResultsTextItem := NewTextView(TextViewPrimary).
		SetText("Success: 0 results returned\n").
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	_, _, containerWidth, _ := app.resultContainer.GetInnerRect()
	textLines := getTextLineCount(noResultsTextItem, containerWidth)
	linesWithSpacing := textLines + 2

	return noResultsTextItem, linesWithSpacing
}

func createResultCell(value string) *tview.TableCell {
	cell := tview.
		NewTableCell(value).
		SetAttributes(tcell.AttrDim)

	cell.
		SetClickedFunc(func() bool {
			mustInitClipboard()
			clipboard.Write(clipboard.FmtText, []byte(value))

			return true
		})

	return cell
}

func (app *App) createResultView(result *db.QueryResult) (view *tview.Table, lines int) {
	resultTable := NewTable()

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

			resultTable.SetCell(
				rowIdx,
				columnIdx,
				createResultCell(cellValue.ToString()),
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
