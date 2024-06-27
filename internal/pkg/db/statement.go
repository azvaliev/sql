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
	tableName, isDescribe := statementIsDescribe(statement)
	if isDescribe {
		return db.buildDescribeQuery(tableName, statement)
	}

	return &StatementWithParams{statement, nil}, nil
}

var describeRegExp = regexp.MustCompile(`^DESCRIBE "?(\w+)"?;?$`)

func statementIsDescribe(query string) (tableName string, isDescribe bool) {
	matches := describeRegExp.FindStringSubmatch(strings.TrimSpace(query))
	if len(matches) != 2 {
		return "", false
	}
	tableName = matches[1]

	return tableName, true
}

func (db *DBClient) buildDescribeQuery(tableName string, originalStatement string) (describeQuery *StatementWithParams, err error) {
	switch db.connManager.GetFlavor() {
	case MySQL:
		{
			return &StatementWithParams{originalStatement, nil}, nil
		}
	case PostgreSQL:
		{
			tableExists, err := db.assertPostgresTableExists(tableName)
			if err != nil {
				return nil, err
			}
			if !tableExists {
				return nil, fmt.Errorf("Table %s does not exist", tableName)
			}
			return &StatementWithParams{postgresDescribeQuery, []interface{}{tableName}}, nil
		}
	default:
		{
			return nil, fmt.Errorf("DESCRIBE not supported for %s", db.connManager.GetFlavor())
		}
	}
}

const postgresTableExistQuery string = `
   SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables
       WHERE  table_schema = 'public'
       AND    table_name = $1
   );`

func (db *DBClient) assertPostgresTableExists(tableName string) (exists bool, err error) {
	conn, err := db.getConnection()
	if err != nil {
		return false, errors.Join(
			errors.New("Failed to get connection"),
			err,
		)
	}

	err = conn.GetContext(db.ctx, &exists, postgresTableExistQuery, tableName)
	if err != nil && err != sql.ErrNoRows {
		return false, errors.Join(
			errors.New("Unable to validate that the table exists"),
			err,
		)
	}

	return exists, nil
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
