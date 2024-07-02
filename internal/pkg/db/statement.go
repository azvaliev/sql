package db

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type StatementWithParams struct {
	statement string
	params    []interface{}
}

// For some special queries we will transform them under the hood for convinience
// i.e. DESCRIBE for non-MySQL
func (db *DBClient) transformStatement(statement string) (
	transformedStatement *StatementWithParams,
	err error,
) {
	if tableName, isDescribe := statementIsDescribe(statement); isDescribe {
		return db.buildDescribeQuery(tableName, statement)
	}

	if tableName, isShowIndexes := statementIsShowIndexes(statement); isShowIndexes {
		return db.buildShowIndexesQuery(tableName, statement)
	}

	if statementIsShowTables(statement) {
		return db.buildShowTablesQuery(statement)
	}

	return &StatementWithParams{statement, nil}, nil
}

var describeRegExp = regexp.MustCompile(`(?i)^DESCRIBE "?(\w+)"?;?$`)

func statementIsDescribe(statement string) (tableName string, isDescribe bool) {
	matches := describeRegExp.FindStringSubmatch(strings.TrimSpace(statement))
	if len(matches) != 2 {
		return "", false
	}
	tableName = matches[1]

	return tableName, true
}

func statementIsShowTables(statement string) bool {
	normalizedStatement := strings.ReplaceAll(
		strings.ToUpper(strings.TrimSpace(statement)),
		";",
		"",
	)

	return normalizedStatement == "SHOW TABLES"
}

var showIndexesRegExp = regexp.MustCompile(`(?i)^SHOW INDEXES FROM "?(\w+)"?;?$`)

func statementIsShowIndexes(statement string) (tableName string, isShowIndexes bool) {
	matches := showIndexesRegExp.FindStringSubmatch(strings.TrimSpace(statement))
	if len(matches) != 2 {
		return "", false
	}

	tableName = matches[1]
	return tableName, true
}

func commandNotSupportedError(command string, flavor DBFlavor) error {
	return fmt.Errorf("%s not supported for %s", command, flavor)
}

func (db *DBClient) buildShowTablesQuery(originalStatement string) (showTablesQuery *StatementWithParams, err error) {
	switch db.connManager.GetFlavor() {
	case PostgreSQL:
		{
			return &StatementWithParams{postgresShowTablesQuery, nil}, nil
		}
	case MySQL:
		{
			return &StatementWithParams{originalStatement, nil}, nil
		}
	default:
		{
			return nil, commandNotSupportedError("SHOW TABLES", db.connManager.GetFlavor())
		}
	}
}

func (db *DBClient) buildShowIndexesQuery(tableName string, originalStatement string) (showIndexesQuery *StatementWithParams, err error) {
	switch db.connManager.GetFlavor() {
	case MySQL:
		{
			return &StatementWithParams{originalStatement, nil}, nil
		}
	case PostgreSQL:
		{
			err := db.assertPostgresTableExists(tableName)
			if err != nil {
				return nil, err
			}

			return &StatementWithParams{postgresShowIndexesQuery, []interface{}{tableName}}, nil
		}
	default:
		{
			return nil, commandNotSupportedError("SHOW INDEXES", db.connManager.GetFlavor())
		}
	}
}

func (db *DBClient) buildDescribeQuery(tableName string, originalStatement string) (describeQuery *StatementWithParams, err error) {
	switch db.connManager.GetFlavor() {
	case MySQL:
		{
			return &StatementWithParams{originalStatement, nil}, nil
		}
	case PostgreSQL:
		{
			err := db.assertPostgresTableExists(tableName)
			if err != nil {
				return nil, err
			}

			return &StatementWithParams{postgresDescribeQuery, []interface{}{tableName}}, nil
		}
	default:
		{
			return nil, commandNotSupportedError("DESCRIBE", db.connManager.GetFlavor())
		}
	}
}

const postgresTableExistQuery string = `
   SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables
       WHERE  table_schema = current_schema()
       AND    table_name = $1
   );`

func (db *DBClient) assertPostgresTableExists(tableName string) (err error) {
	conn, err := db.getConnection()
	if err != nil {
		return errors.Join(
			errors.New("Failed to get connection"),
			err,
		)
	}

	var exists bool
	err = conn.GetContext(db.ctx, &exists, postgresTableExistQuery, tableName)
	if err != nil && err != sql.ErrNoRows {
		return errors.Join(
			errors.New("Unable to validate that the table exists"),
			err,
		)
	}

	if !exists {
		return fmt.Errorf("Table %s does not exist", tableName)
	}

	return nil
}

const postgresShowTablesQuery string = `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
ORDER BY table_name ASC
`

const postgresShowIndexesQuery string = `
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = $1
ORDER BY indexname ASC
`

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
