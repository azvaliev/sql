package db

import (
	"context"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type DBClient struct {
	ctx         context.Context
	sqlDB       *sqlx.DB
	_conn       *sqlx.Conn
	connManager ConnManager
}

// Instantiate a DBClient from a DSN
func CreateDBClient(
	dsnProducer ConnManager,
) (*DBClient, error) {
	dataSourceName, err := dsnProducer.GetDSN()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to create connection string"),
			err,
		)
	}

	sqlDB, err := sqlx.Open(string(dsnProducer.GetFlavor()), dataSourceName)
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to open database"),
			err,
		)
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to establish connection to database"),
			err,
		)
	}

	// Keep connections alive for 5 mins
	sqlDB.SetConnMaxLifetime(time.Minute * 5)

	// Only should ever have a single connection
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	db := DBClient{
		ctx:         context.Background(),
		sqlDB:       sqlDB,
		connManager: dsnProducer,
	}

	return &db, nil
}

// Cleanup database resources
// Call before this struct drops out of scope
func (db *DBClient) Destroy() error {
	// This only returns an error if the connection is already closed, safe to ignore
	_ = db._conn.Close()

	return db.sqlDB.Close()
}

// Run a query and store the output in a displayable format
// NOTE: results and error may both be nil if a query is succesful yet doesn't return any rows
func (db *DBClient) Query(statement string) (results *QueryResult, err error) {
	conn, err := db.getConnection()
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

// We try to use a single connection, instantiated when DBClient is instantiated
// This will either return that existing connection, or create a new one if that got dropped
func (db *DBClient) getConnection() (*sqlx.Conn, error) {
	if db._conn != nil {
		// See if our existing connection is still alive
		err := db._conn.PingContext(db.ctx)
		if err == nil {
			return db._conn, nil
		}
		db._conn.Close()
	}

	conn, err := db.sqlDB.Connx(db.ctx)

	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to get connection to database"),
			err,
		)
	}

	if db.connManager.IsSafeMode() {
		_, err = conn.ExecContext(db.ctx, "SET SQL_SAFE_UPDATES = 1")
		if err != nil {
			return nil, err
		}
	}

	db._conn = conn
	return db._conn, nil
}
