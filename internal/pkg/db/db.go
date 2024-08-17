package db

import (
	"context"
	"errors"

	"github.com/azvaliev/sql/internal/pkg/db/conn"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBClient struct {
	ctx         context.Context
	connManager *conn.ConnectionManager
}

// Instantiate a DBClient from a DSN
func CreateDBClient(connManager *conn.ConnectionManager) (*DBClient, error) {
	if connManager == nil {
		return nil, errors.New("Cannot instantiate DBClient with nil connection manager")
	}

	db := DBClient{
		ctx:         context.Background(),
		connManager: connManager,
	}

	return &db, nil
}

// Cleanup database resources
// Call before this struct drops out of scope
func (db *DBClient) Destroy() {
	db.connManager.Destroy()
}

// Run a query and store the output in a displayable format
// NOTE: results and error may both be nil if a query is succesful yet doesn't return any rows
func (db *DBClient) Query(statement string) (results *QueryResult, err error) {

	conn, err := db.connManager.GetConnection()
	if err != nil {
		return nil, err
	}

	statementWithParams, err := db.transformStatement(statement)
	if err != nil {
		return nil, errors.Join(
			errors.New("Query Failed"),
			err,
		)
	}

	// Execute the statement and get the raw rows iterator
	rows, err := conn.QueryxContext(
		db.ctx,
		statementWithParams.statement,
		statementWithParams.params...,
	)
	if err != nil {
		return nil, errors.Join(
			errors.New("Query Failed"),
			err,
		)
	} else if rows == nil {
		return nil, nil
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			panic("Failed to cleanup rows")
		}
	}()

	columnParsingError := errors.New("Could not determine columns")

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Join(
			columnParsingError,
			err,
		)
	}

	// Scan all the rows into a string format, since we're just selecting to display
	rawRows := [][]NullString{}
	for rows.Next() {
		rawRow := make([]NullString, len(columns))
		rawRowPtrs := make([]any, len(columns))

		for i := range rawRow {
			rawRow[i] = NullString{}
			rawRowPtrs[i] = &rawRow[i]
		}

		if err = rows.Scan(rawRowPtrs...); err != nil {
			return nil, errors.Join(
				errors.New("failed to read rows"),
				err,
			)
		}

		rawRows = append(rawRows, rawRow)
	}

	// Transform each row into a map of column -> value
	mappedRows := make([]map[string]*NullString, len(rawRows))
	for rowIdx := range rawRows {
		rawRow := rawRows[rowIdx]
		mappedRow := make(map[string]*NullString, len(rawRow))

		for columnIdx, columnValue := range rawRow {
			columnName := columns[columnIdx]
			mappedRow[columnName] = &columnValue
		}

		mappedRows[rowIdx] = mappedRow
	}

	return &QueryResult{
		Rows:    mappedRows,
		Columns: columns,
	}, err
}
