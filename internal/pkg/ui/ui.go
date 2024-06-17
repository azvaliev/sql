package ui

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type App struct {
	tviewApp        *tview.Application
	resultContainer *tview.Flex
	queryTextArea   *tview.TextArea
	db              *db.DBClient
}

const (
	QueryTextColor    = tcell.ColorLightGray
	StandardTextColor = tcell.ColorWhite
	ResultTextColor   = tcell.ColorWhite
	ErrorTextColor    = tcell.ColorRed
)

// Setup initial layout and application structure
func Init(db *db.DBClient) *App {
	tviewApp := tview.NewApplication()

	queryTextArea := tview.NewTextArea().SetTextStyle(tcell.StyleDefault.Foreground(QueryTextColor))
	queryTextArea.SetTitle("Query").SetBorder(true)

	resultContainer := tview.NewFlex().SetDirection(tview.FlexRow)
	resultContainer.SetBorder(true)

	box := tview.NewFlex().
		SetFullScreen(true).
		SetDirection(tview.FlexRow).
		AddItem(resultContainer, 0, 4, false).
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
	app.queryTextArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
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
	})

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
		SetTextColor(QueryTextColor).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	results, err := app.db.Query(query)
	var resultItem *tview.TextView
	resultItem = app.createNoResultView()

	if err != nil {
		resultItem = app.createErrorView(err)
	} else if results != nil {
		resultItem = app.createResultView(results)
	} else {
		resultItem = app.createNoResultView()
	}

	app.resultContainer.AddItem(
		queryTextItem,
		getTextLineCount(query),
		1,
		false,
	)
	app.resultContainer.AddItem(
		resultItem,
		getTextLineCount(resultItem.GetText(false))+1,
		1,
		false,
	)
}

func (app *App) createResultView(results *db.QueryResult) *tview.TextView {
	resultTextItem := tview.
		NewTextView().
		SetText(fmt.Sprint(results)).
		SetTextColor(ResultTextColor).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	return resultTextItem
}

func (app *App) createErrorView(dbErr error) *tview.TextView {
	errorTextItem := tview.
		NewTextView().
		SetText(dbErr.Error()).
		SetTextColor(ErrorTextColor).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	return errorTextItem
}

func (app *App) createNoResultView() *tview.TextView {
	noResultsTextItem := tview.
		NewTextView().
		SetText("Success: 0 results returned").
		SetTextColor(StandardTextColor).
		SetChangedFunc(func() {
			app.tviewApp.Draw()
		})

	return noResultsTextItem
}
