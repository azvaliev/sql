package db

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
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

type QueryResult struct {
	// Each row maps column -> value
	Rows []map[string]string
	// Column names, order preserved with how they were selected
	Columns []string
}

// Run a query and store the output in a displayable format
// NOTE: results and error may both be nil if a query is succesful yet doesn't return any rows
func (db *DBClient) Query(statement string) (results *QueryResult, err error) {
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}

	statement, params := db.transformStatement(statement)

	// Execute the statement and get the raw rows iterator
	rows, err := conn.QueryxContext(db.ctx, statement, params...)
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
	rawRows := [][]sql.NullString{}
	for rows.Next() {
		rawRow := make([]sql.NullString, len(columns))
		rawRowPtrs := make([]any, len(columns))

		for i := range rawRow {
			rawRow[i] = sql.NullString{}
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
	mappedRows := make([]map[string]string, len(rawRows))
	for rowIdx := range rawRows {
		rawRow := rawRows[rowIdx]
		mappedRow := make(map[string]string, len(rawRow))

		for columnIdx, columnValue := range rawRow {
			columnName := columns[columnIdx]
			if columnValue.Valid {
				mappedRow[columnName] = columnValue.String
			} else {
				mappedRow[columnName] = "NULL"
			}
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

// For some special queries we will transform them under the hood for convinience
// i.e. DESCRIBE for non-MySQL
func (db *DBClient) transformStatement(statement string) (transformedStatement string, params []interface{}) {
	if db.connManager.GetFlavor() != PostgreSQL {
		return statement, nil
	}

	tableName, isDescribe := statementIsDescribe(statement)
	if isDescribe {
		if newQuery, params, ok := db.buildDescribeQuery(tableName); ok {
			return newQuery, params
		}
	}

	return statement, nil
}

var describeRegExp = regexp.MustCompile(`^DESCRIBE "?(\w+)"?$`)

func statementIsDescribe(query string) (tableName string, isDescribe bool) {
	matches := describeRegExp.FindStringSubmatch(query)
	if len(matches) != 2 {
		return "", false
	}

	return matches[1], true
}

func (db *DBClient) buildDescribeQuery(tableName string) (query string, params []interface{}, ok bool) {
	switch db.connManager.GetFlavor() {
	case PostgreSQL:
		{
			return postgresDescribeQuery, []interface{}{tableName}, true
		}
	default:
		{
			return "", nil, false
		}
	}
}

const postgresDescribeQuery string = `
WITH columns AS (
  SELECT
    c.column_name AS "Field",
    -- Include character length and numeric precision/scale for relevant data types
    CASE
        WHEN c.data_type = 'character' AND c.character_maximum_length IS NOT NULL THEN c.data_type || '(' || c.character_maximum_length || ')'
        WHEN c.data_type = 'character varying' AND c.character_maximum_length IS NOT NULL THEN c.data_type || '(' || c.character_maximum_length || ')'
        WHEN c.data_type = 'character' THEN c.data_type
        WHEN c.data_type = 'character varying' THEN c.data_type
        WHEN c.data_type = 'numeric' THEN c.data_type || '(' || c.numeric_precision || ', ' || c.numeric_scale || ')'
        ELSE c.data_type
    END AS "Type",
    CASE
        WHEN c.is_nullable = 'YES' THEN 'YES'
        ELSE 'NO'
    END AS "Null",
    CASE
        WHEN kcu.column_name IS NOT NULL AND tc.constraint_type = 'PRIMARY KEY' THEN 'PRI'
        WHEN kcu.column_name IS NOT NULL AND tc.constraint_type = 'UNIQUE' THEN 'UNI'
        WHEN i.indexname IS NOT NULL AND i.indisunique THEN 'UNI'
        WHEN i.indexname IS NOT NULL THEN 'MUL'
        ELSE ''
    END AS "Key",
    COALESCE(c.column_default, 'NULL') AS "Default"
  FROM
    information_schema.columns c
  LEFT JOIN
    information_schema.key_column_usage kcu
    ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
  LEFT JOIN
    information_schema.table_constraints tc
    ON kcu.table_name = tc.table_name AND kcu.constraint_name = tc.constraint_name
  LEFT JOIN
    (
        SELECT
            ic.relname as indexname,
            a.attname as column_name,
            i.indrelid::regclass::text as table_name,
            a.attnum,
            i.indkey as indkey,
            i.indkey[0] as first_column,
            i.indisunique
        FROM
            pg_class t,
            pg_class ic,
            pg_index i,
            pg_attribute a
        WHERE
            t.oid = i.indrelid
            AND ic.oid = i.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(i.indkey)
            AND t.relkind = 'r'
            AND ic.relkind = 'i'
            AND i.indisprimary = false
    ) i
    ON c.table_name = i.table_name AND c.column_name = i.column_name
    AND (i.column_name = c.column_name AND (i.attnum = i.first_column OR array_length(i.indkey, 1) = 1))
  WHERE
    c.table_name = $1
)
SELECT
  "Field",
  "Type",
  "Null",
  "Key",
  "Default"
FROM
  columns;
`
