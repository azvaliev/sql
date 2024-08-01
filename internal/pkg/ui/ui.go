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
	queryHistory    *QueryHistory
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
		queryHistory:    NewQueryHistory(100),
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

	currentText := textView.GetText(true)

	// Get newline count
	newlineCount := len(strings.Split(currentText, "\n")) - 1

	// Get string width, in the same units as tview uses
	totalStringCharsWidth := float64(
		uniseg.StringWidth(currentText),
	)

	// counting the raw characters will account for implicit line breaks, overflowing the available space
	implicitLines := math.Ceil(totalStringCharsWidth / float64(maxWidth))

	return int(implicitLines) + newlineCount
}

func (app *App) commitQuery(query string) {
	defer app.queryHistory.AddEntry(query)
	results, err := app.db.Query(query)
	var resultItem tview.Primitive
	var height int

	var queryAction AvailableActions
	if err != nil {
		resultItem, height = app.createErrorView(err)
		queryAction = QueryNoResultsErrorAction
	} else if results != nil && len(results.Columns) > 0 {
		resultItem, height = app.createResultView(results)
		queryAction = QueryWithResultsActions
	} else {
		resultItem, height = app.createNoResultView()
		queryAction = QueryNoResultsErrorAction
	}

	queryViewWithActions, queryViewWithActionsHeight := app.createQueryViewWithActions(
		query,
		queryAction,
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

type AvailableActions int

const (
	QueryWithResultsActions AvailableActions = iota + 1
	QueryNoResultsErrorAction
)

func (app *App) createQueryViewWithActions(
	query string,
	queryAction AvailableActions,
	queryResult *db.QueryResult,
	queryError error,
) (queryView *tview.Grid, fixedHeight int) {
	queryView = NewGrid().
		SetGap(0, 2)

	_, _, containerWidth, _ := app.resultContainer.GetInnerRect()
	queryTextItemWidth := containerWidth / 2
	gridHeight := 1

	// Create query text item
	{
		formattedQueryText := fmt.Sprint("> ", query)

		queryTextItem := NewTextView(TextViewSecondary).
			SetText(formattedQueryText).
			SetChangedFunc(func() {
				app.tviewApp.Draw()
			}).
			SetWrap(true).
			SetWordWrap(true)

		gridHeight = getTextLineCount(queryTextItem, queryTextItemWidth)

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
	}

	// Query text item + gap
	columns := []int{queryTextItemWidth, 0}
	buttonColumnStartIdx := len(columns)

	// Add all the buttons to the grid
	actionButtons := createQueryActionButtons(queryResult, queryError, queryAction)
	for buttonIdx, button := range actionButtons {
		columnIdx := buttonColumnStartIdx + buttonIdx

		columns = append(columns, len(button.GetLabel()))
		queryView.AddItem(
			button,
			0,
			columnIdx,
			1,
			1,
			0,
			0,
			true,
		)
	}

	// Our single row in this grid must be as tall as the grid itself
	queryView.SetRows(gridHeight)
	queryView.SetColumns(columns...)
	return queryView, gridHeight
}

func createQueryActionButtons(queryResult *db.QueryResult, queryError error, queryActions AvailableActions) (buttons []*tview.Button) {
	switch queryActions {
	case QueryWithResultsActions:
		{
			queryCopyCSVButton := NewButton("Copy as CSV").
				SetSelectedFunc(func() {
					mustInitClipboard()

					resultCSV := queryResult.ToCSV()
					clipboard.Write(clipboard.FmtText, resultCSV)
				})

			queryCopyJSONButton := NewButton("Copy as JSON").
				SetSelectedFunc(func() {
					mustInitClipboard()

					resultJSON := queryResult.ToJSON()
					clipboard.Write(clipboard.FmtText, resultJSON)
				})

			return []*tview.Button{queryCopyCSVButton, queryCopyJSONButton}
		}
	case QueryNoResultsErrorAction:
		{
			queryCopyResultsButton := NewButton("Copy Output").
				SetSelectedFunc(func() {
					mustInitClipboard()

					var result string
					if queryError != nil {
						result = queryError.Error()
					} else {
						result = NoResultsMessage
					}

					clipboard.Write(clipboard.FmtText, []byte(result))
				})

			return []*tview.Button{queryCopyResultsButton}
		}
	default:
		{
			return buttons
		}
	}
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

const NoResultsMessage string = "Success: 0 results returned\n"

func (app *App) createNoResultView() (view *tview.TextView, lines int) {
	noResultsTextItem := NewTextView(TextViewPrimary).
		SetText(NoResultsMessage).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	_, _, containerWidth, _ := app.resultContainer.GetInnerRect()
	textLines := getTextLineCount(noResultsTextItem, containerWidth)
	linesWithSpacing := textLines + 2

	return noResultsTextItem, linesWithSpacing
}

func (app *App) createResultCell(value string) *tview.TableCell {
	cell := tview.
		NewTableCell(value).
		SetAttributes(tcell.AttrDim)

	cell.
		SetClickedFunc(func() bool {
			mustInitClipboard()
			clipboard.Write(clipboard.FmtText, []byte(value))

			// Refocus back on the textarea so that copied content could be used in the next query
			app.tviewApp.SetFocus(app.queryTextArea)

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
				app.createResultCell(cellValue.ToString()),
			)
		}
	}

	height := len(result.Rows)*2 + 5

	return resultTable, int(height)
}

// Intercept text area key presses for shortcuts or committing querys
func (app *App) handleInputCapture(event *tcell.EventKey) *tcell.EventKey {
	isNotShortcut := event.Modifiers() != tcell.ModCtrl && event.Modifiers() != tcell.ModAlt

	if isNotShortcut {
		query := app.queryTextArea.GetText()
		queryLen := len(strings.TrimSpace(query))

		// user wasn't paginating before
		// or they have text typed in we want to be careful before removing
		shouldNotAllowScrollingQueryHistory := queryLen > 0 && !app.queryHistory.IsPositionSet()

		switch event.Key() {
		// Handle committing the query, if applicable
		case tcell.KeyEnter:
			{
				var lastChar rune
				if queryLen > 0 {
					lastChar = rune(query[len(query)-1])
				}

				shouldCommitQuery := lastChar == ';' && queryLen > 0
				if shouldCommitQuery {
					app.commitQuery(query)
					app.queryTextArea.SetText("", false)

					return nil
				}
				return event
			}
		case tcell.KeyUp:
			{
				if shouldNotAllowScrollingQueryHistory {
					return event
				}

				prevEntry := app.queryHistory.GetPrevEntry()
				app.queryTextArea.SetText(prevEntry, false)

				return nil
			}
		case tcell.KeyDown:
			{
				if shouldNotAllowScrollingQueryHistory {
					return event
				}

				nextEntry := app.queryHistory.GetNextEntry()
				app.queryTextArea.SetText(nextEntry, false)

				return nil
			}
		default:
			{
				app.queryHistory.ResetPosition()
				return event
			}
		}
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
