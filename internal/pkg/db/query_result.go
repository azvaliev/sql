package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

type NullString struct {
	sql.NullString
}

func (nullString *NullString) ToString() string {
	if !nullString.Valid {
		return "NULL"
	}

	return nullString.String
}

func (nullString *NullString) MarshalJSON() ([]byte, error) {
	if nullString.Valid {
		return json.Marshal(nullString.String)
	}

	return json.Marshal(nil)
}

type QueryResult struct {
	// Each row maps column -> value
	// Why NullString for values?
	// Making a more generic type here to store any SQL value results in some messy reflection code
	// For our purposes, we can store all data types as either string or null, since our main
	// intention is to render them as string
	Rows []map[string]*NullString
	// Column names, order preserved with how they were selected
	Columns []string
}

func (queryResult *QueryResult) ToJSON() (res []byte) {
	res, err := json.Marshal(queryResult.Rows)
	if err != nil {
		// TODO: is there a better way to handle?
		// With our data structure is this failure even possible?
		panic(errors.Join(
			errors.New("Failed to marshal query results into JSON"),
			err,
		))
	}

	return res
}

func (queryResult *QueryResult) ToCSV() (res []byte) {
	var resString strings.Builder

	// Add column header
	resString.WriteString(
		strings.Join(queryResult.Columns, ","),
	)

	// Add each row
	for _, row := range queryResult.Rows {
		resString.WriteRune('\n')
		rowValues := make([]string, len(queryResult.Columns))

		for columnIdx, columnName := range queryResult.Columns {
			cellValue := row[columnName]
			rowValues[columnIdx] = cellValue.ToString()
		}
		resString.WriteString(strings.Join(rowValues, ","))
	}

	return []byte(resString.String())
}
