package db_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

type dataDisplayTypeTestCase struct {
	ColumnName    string
	Datatype      string
	ProvidedValue string
	ExpectedValue string
	// format string if we want to run a fn or something on the column
	// Ex: "AVG(%s)"
	SelectFmt string
}

type dataDisplayTestOptions struct {
	Cases       []dataDisplayTypeTestCase
	ConnOptions db.DBConnOptions
	Versions    []string
}

var dataDisplayTestSuite = []dataDisplayTestOptions{
	{
		Cases: mysqlDataDisplayTestCases,
		ConnOptions: db.DBConnOptions{
			Flavor:       db.MySQL,
			Host:         "localhost",
			DatabaseName: "test",
			User:         "user",
			Password:     "password",
			Port:         3306,
			SafeMode:     true,
		},
		Versions: TESTED_MYSQL_VERSIONS[:],
	},
	{
		Cases: postgresDataDisplayTestCases,
		ConnOptions: db.DBConnOptions{
			Flavor:       db.PostgreSQL,
			Host:         "localhost",
			DatabaseName: "test",
			User:         "user",
			Password:     "password",
			Port:         5432,
		},
		Versions: TESTED_POSTGRES_VERSIONS[:],
	},
}

func TestDBDataDisplay(t *testing.T) {
	for _, testSuite := range dataDisplayTestSuite {

		// Check display of all datatypes
		for _, dbVersion := range testSuite.Versions {
			ctx := context.Background()
			initTestDBOptions := InitTestDBOptions{dbVersion, &testSuite.ConnOptions}

			container, err := initTestDB(&initTestDBOptions, ctx)
			assert.NoError(t, err)

			defer testDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&testSuite.ConnOptions)
			assert.NoError(t, err)

			for idx, tt := range testSuite.Cases {
				t.Run(fmt.Sprintf("%s %s - %s display", testSuite.ConnOptions.Flavor, dbVersion, tt.Datatype), func(t *testing.T) {
					tt := tt
					idx := idx

					assert := assert.New(t)

					tableName := fmt.Sprintf("test_%s_%d", tt.ColumnName, idx)
					tableCreationStatement := fmt.Sprintf(`
					CREATE TABLE %s (
						%s %s
					);
				`, tableName, tt.ColumnName, tt.Datatype)
					_, err := dbClient.Query(tableCreationStatement)

					testCtx := []string{tableCreationStatement}
					assert.NoError(err, testCtx)

					dataInsertionStatement := fmt.Sprintf(`
					INSERT INTO %s VALUES (%s);
				`, tableName, tt.ProvidedValue)
					_, err = dbClient.Query(dataInsertionStatement)

					testCtx = append(testCtx, dataInsertionStatement)
					assert.NoError(err, testCtx)

					columnSelectExpression := tt.ColumnName
					if tt.SelectFmt != "" {
						columnSelectExpression = fmt.Sprintf("%s as %s", fmt.Sprintf(tt.SelectFmt, columnSelectExpression), tt.ColumnName)
					}

					dataSelectStatement := fmt.Sprintf("SELECT %s FROM %s;", columnSelectExpression, tableName)
					result, err := dbClient.Query(dataSelectStatement)

					testCtx = append(testCtx, dataSelectStatement)
					assert.NoError(err, testCtx)

					assert.Len(result.Rows, 1, testCtx)
					data := result.Rows[0]

					assert.Len(data, 1, testCtx)
					assert.Equal(tt.ExpectedValue, data[tt.ColumnName].ToString(), testCtx)

				})
			}
		}
	}
}

var postgresDataDisplayTestCases = []dataDisplayTypeTestCase{
	{
		ColumnName:    "uuidcolumn",
		Datatype:      "UUID",
		ProvidedValue: `'cd0722a2-d55c-4e15-9a20-961967718cc4'`,
		ExpectedValue: "cd0722a2-d55c-4e15-9a20-961967718cc4",
	},
	{
		ColumnName:    "varcharcolumn",
		Datatype:      "VARCHAR",
		ProvidedValue: `'test string'`,
		ExpectedValue: "test string",
	},
	{
		ColumnName:    "integercolumn",
		Datatype:      "INTEGER",
		ProvidedValue: `123`,
		ExpectedValue: "123",
	},
	{
		ColumnName:    "booleancolumn",
		Datatype:      "BOOLEAN",
		ProvidedValue: `true`,
		ExpectedValue: "true",
	},
	{
		ColumnName:    "datecolumn",
		Datatype:      "DATE",
		ProvidedValue: `'2023-01-01'`,
		ExpectedValue: "2023-01-01T00:00:00Z",
	},
	{
		ColumnName:    "timestampcolumn",
		Datatype:      "TIMESTAMP",
		ProvidedValue: `'2023-01-01 12:34:56'`,
		ExpectedValue: "2023-01-01T12:34:56Z",
	},
	{
		ColumnName:    "timestamptzcolumn",
		Datatype:      "TIMESTAMPTZ",
		ProvidedValue: `'2023-01-01 12:34:56 +00:00'`,
		ExpectedValue: "2023-01-01T12:34:56Z",
		SelectFmt:     "%s AT TIME ZONE 'UTC'",
	},
	{
		ColumnName:    "floatcolumn",
		Datatype:      "FLOAT",
		ProvidedValue: `123.45`,
		ExpectedValue: "123.45",
	},
	{
		ColumnName:    "jsonbcolumn",
		Datatype:      "JSONB",
		ProvidedValue: `'{"key": "value"}'`,
		ExpectedValue: `{"key": "value"}`,
	},
	{
		ColumnName:    "integerarraycolumn",
		Datatype:      "INTEGER[]",
		ProvidedValue: `'{1, 2, 3}'`,
		ExpectedValue: "{1,2,3}",
	},
	{
		ColumnName:    "numericcolumn",
		Datatype:      "NUMERIC",
		ProvidedValue: `123456789.123456789`,
		ExpectedValue: "123456789.123456789",
	},
	{
		ColumnName:    "intervalcolumn",
		Datatype:      "INTERVAL",
		ProvidedValue: `'1 year 2 months 3 days'`,
		ExpectedValue: "1 year 2 mons 3 days",
	},
	{
		ColumnName:    "nullcolumn",
		Datatype:      "TEXT",
		ProvidedValue: "null",
		ExpectedValue: "NULL",
	},
}

var mysqlDataDisplayTestCases = []dataDisplayTypeTestCase{
	{
		ColumnName:    "tinyIntUnsignedColumn",
		Datatype:      "TINYINT(3) UNSIGNED",
		ProvidedValue: "127",
		ExpectedValue: "127",
	},
	{
		ColumnName:    "smallIntColumn",
		Datatype:      "SMALLINT",
		ProvidedValue: "32767",
		ExpectedValue: "32767",
	},
	{
		ColumnName:    "mediumIntUnsignedColumn",
		Datatype:      "MEDIUMINT UNSIGNED",
		ProvidedValue: "8388607",
		ExpectedValue: "8388607",
	},
	{
		ColumnName:    "intColumn",
		Datatype:      "INT",
		ProvidedValue: "2147483647",
		ExpectedValue: "2147483647",
	},
	{
		ColumnName:    "bigIntColumn",
		Datatype:      "BIGINT",
		ProvidedValue: "9223372036854775807",
		ExpectedValue: "9223372036854775807",
	},
	{
		ColumnName:    "floatColumn",
		Datatype:      "FLOAT",
		ProvidedValue: "123.45",
		ExpectedValue: "123.45",
	},
	{
		ColumnName:    "decimalColumn",
		Datatype:      "DECIMAL(10,2)",
		ProvidedValue: "123456.78",
		ExpectedValue: "123456.78",
	},
	{
		ColumnName:    "dateColumn",
		Datatype:      "DATE",
		ProvidedValue: `"2023-06-01"`,
		ExpectedValue: `2023-06-01`,
	},
	{
		ColumnName:    "timestampColumn",
		Datatype:      "TIMESTAMP",
		ProvidedValue: `"2023-06-01 12:30:45"`,
		ExpectedValue: `2023-06-01 12:30:45`,
	},
	{
		ColumnName:    "timeColumn",
		Datatype:      "TIME",
		ProvidedValue: `"12:30:45"`,
		ExpectedValue: `12:30:45`,
	},
	{
		ColumnName:    "yearColumn",
		Datatype:      "YEAR",
		ProvidedValue: `"2023"`,
		ExpectedValue: `2023`,
	},
	{
		ColumnName:    "charColumn",
		Datatype:      "CHAR(5)",
		ProvidedValue: `"Hello"`,
		ExpectedValue: `Hello`,
	},
	{
		ColumnName:    "varcharColumn",
		Datatype:      "VARCHAR(50)",
		ProvidedValue: `"Hello, World!"`,
		ExpectedValue: `Hello, World!`,
	},
	{
		ColumnName:    "varbinaryColumn",
		Datatype:      "VARBINARY(50)",
		ProvidedValue: "0x000048656c6c6f",
		ExpectedValue: "\x00\x00Hello",
	},
	{
		ColumnName:    "blobColumn",
		Datatype:      "BLOB",
		ProvidedValue: `"Blob data up to 65535 bytes"`,
		ExpectedValue: "Blob data up to 65535 bytes",
	},
	{
		ColumnName:    "textColumn",
		Datatype:      "TEXT",
		ProvidedValue: `"This is a text string that can be very long"`,
		ExpectedValue: "This is a text string that can be very long",
	},
	{
		ColumnName:    "geometryColumn",
		Datatype:      "GEOMETRY",
		ProvidedValue: "ST_GeomFromText('POINT(1 1)')",
		ExpectedValue: "POINT(1 1)",
		SelectFmt:     "ST_AsText(%s)",
	},
	{
		ColumnName:    "pointColumn",
		Datatype:      "POINT",
		ProvidedValue: "ST_GeomFromText('POINT(1 1)')",
		ExpectedValue: "POINT(1 1)",
		SelectFmt:     "ST_AsText(%s)",
	},
	{
		ColumnName:    "lineStringColumn",
		Datatype:      "LINESTRING",
		ProvidedValue: "ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')",
		ExpectedValue: "LINESTRING(0 0,1 1,2 2)",
		SelectFmt:     "ST_AsText(%s)",
	},
	{
		ColumnName:    "polygonColumn",
		Datatype:      "POLYGON",
		ProvidedValue: "ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 0))')",
		ExpectedValue: "POLYGON((0 0,1 1,1 0,0 0))",
		SelectFmt:     "ST_AsText(%s)",
	},
	{
		ColumnName:    "geometryCollectionColumn",
		Datatype:      "GEOMETRYCOLLECTION",
		ProvidedValue: "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0,1 1))')",
		ExpectedValue: "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))",
		SelectFmt:     "ST_AsText(%s)",
	},
	{
		ColumnName:    "jsonColumn",
		Datatype:      "JSON",
		ProvidedValue: `"{\"key\": \"value\"}"`,
		ExpectedValue: "{\"key\": \"value\"}",
	},
	{
		ColumnName:    "nullColumn",
		Datatype:      "TEXT",
		ProvidedValue: "NULL",
		ExpectedValue: "NULL",
	},
}
