package db_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/azvaliev/sql/internal/pkg/db/conn"
	"github.com/stretchr/testify/assert"
)

// Test transformed DB queries

var showTablesTestSuite = []struct {
	ConnOptions          conn.DSNOptions
	ShowTablesColumnName string
	DBVersions           []string
}{
	{
		conn.DSNOptions{
			Flavor:       conn.PostgreSQL,
			Host:         "localhost",
			DatabaseName: "test",
			User:         "user",
			Password:     "password",
			Port:         5432,
		},
		"table_name",
		TESTED_POSTGRES_VERSIONS[:],
	},
	{
		conn.DSNOptions{
			Flavor:       conn.MySQL,
			Host:         "localhost",
			DatabaseName: "test",
			User:         "user",
			Password:     "password",
			Port:         3306,
		},
		"Tables_in_test",
		TESTED_MYSQL_VERSIONS[:],
	},
}

func TestDBShowTables(t *testing.T) {
	for _, testSuite := range showTablesTestSuite {
		for _, dbVersion := range testSuite.DBVersions {
			t.Run(fmt.Sprintf("%s %s - SHOW TABLES", testSuite.ConnOptions.Flavor, dbVersion), func(t *testing.T) {
				dbVersion := dbVersion
				assert := assert.New(t)

				dbClient, cleanup := mustInitTestDBWithClient(
					&InitTestDBOptions{dbVersion, &testSuite.ConnOptions},
					assert,
				)
				defer cleanup()

				/// Create a few test tables

				expectedTableNames := []string{"table_one", "table_two", "table_three"}
				for _, tableName := range expectedTableNames {
					query := fmt.Sprintf("CREATE TABLE %s (id int)", tableName)
					_, err := dbClient.Query(query)
					assert.NoError(err, "expected to create table succesfully", query)
				}

				/// Retrieve test table names via SHOW TABLES and validate

				actualTableNamesResult, err := dbClient.Query("SHOW TABLES")
				assert.NoError(err, "should get table names succesfully")

				assert.Len(actualTableNamesResult.Columns, 1, "expected 1 column")
				assert.Equal(
					testSuite.ShowTablesColumnName,
					actualTableNamesResult.Columns[0],
					"expected table name column from SHOW TABLES",
				)

				var actualTableNames []string
				for _, row := range actualTableNamesResult.Rows {
					tableName := row[testSuite.ShowTablesColumnName]
					assert.NotEmpty(tableName, "table name should be defined", row)

					actualTableNames = append(actualTableNames, tableName.ToString())
				}

				slices.Sort(expectedTableNames)
				slices.Sort(actualTableNames)
				assert.Equal(expectedTableNames, actualTableNames)
			})
		}
	}
}

func TestDBShowIndexesPostgres(t *testing.T) {
	connOptions := conn.DSNOptions{
		Flavor:       conn.PostgreSQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "user",
		Password:     "password",
		Port:         5432,
	}

	for _, postgresVersion := range TESTED_POSTGRES_VERSIONS {
		t.Run(fmt.Sprintf("Postgres %s - Show INDEXES", postgresVersion), func(t *testing.T) {
			assert := assert.New(t)

			dbClient, cleanup := mustInitTestDBWithClient(
				&InitTestDBOptions{
					postgresVersion,
					&connOptions,
				},
				assert,
			)
			defer cleanup()

			// Setup test data
			tableName := "foo"

			pkeyIndexName := fmt.Sprintf(`%s_pkey`, tableName)
			createPkeyStatement := fmt.Sprintf(`CREATE UNIQUE INDEX %s ON public.%s (a)`, pkeyIndexName, tableName)

			index1Name := fmt.Sprintf(`%s_idx_uni_b_c`, tableName)
			createIndex1Statement := fmt.Sprintf(`CREATE UNIQUE INDEX %s ON public.%s (b, c)`, index1Name, tableName)

			index2Name := fmt.Sprintf(`%s_idx_d`, tableName)
			createIndex2Statement := fmt.Sprintf(`CREATE INDEX %s ON public.%s (d)`, index2Name, tableName)

			// Create test table + indices in DB
			{
				createTableStatement := fmt.Sprintf(`CREATE TABLE %s (
				a VARCHAR(255) PRIMARY KEY,
				b INT NOT NULL,
				c CHAR(36) NOT NULL,
				D MONEY
				)`, tableName)
				for _, stmnt := range []string{createTableStatement, createIndex1Statement, createIndex2Statement} {
					_, err := dbClient.Query(stmnt)
					assert.NoError(err, "expected statement to execute succesfully", stmnt)
				}
			}

			// Validate SHOW INDEXES
			showIndexesQuery := fmt.Sprintf("SHOW INDEXES FROM %s", tableName)
			showIndexesResult, err := dbClient.Query(showIndexesQuery)
			assert.NoError(err, "expected to show indexes succesfully", showIndexesQuery)

			assert.Len(showIndexesResult.Rows, 3, "show have 2 created indexes, 1 pkey", showIndexesResult)

			expectedResults := []struct{ indexname, indexdef string }{
				{
					indexname: index2Name,
					indexdef:  createIndex2Statement,
				},
				{
					indexname: index1Name,
					indexdef:  createIndex1Statement,
				},
				{
					indexname: pkeyIndexName,
					indexdef:  createPkeyStatement,
				},
			}

			for idx, expectedResult := range expectedResults {
				actualResult := showIndexesResult.Rows[idx]

				assert.Equal(
					expectedResult.indexname,
					actualResult["indexname"].ToString(),
					"index names should match",
					expectedResult,
					actualResult,
				)
				assert.Equal(
					expectedResult.indexdef,
					strings.ReplaceAll(actualResult["indexdef"].ToString(), "USING btree ", ""),
					"index definitions should match",
					expectedResult,
					actualResult,
				)
			}
		})
	}
}

func TestDBShowIndexesMySQL(t *testing.T) {
	connOptions := conn.DSNOptions{
		Flavor:       conn.MySQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "user",
		Password:     "password",
		Port:         3306,
	}

	for _, mySQLVersion := range TESTED_MYSQL_VERSIONS {
		t.Run(fmt.Sprintf("MySQL %s - SHOW INDEXES", mySQLVersion), func(t *testing.T) {
			assert := assert.New(t)

			dbClient, cleanup := mustInitTestDBWithClient(
				&InitTestDBOptions{mySQLVersion, &connOptions},
				assert,
			)
			defer cleanup()

			// Setup test data
			tableName := "foo"

			index1Name := fmt.Sprintf(`%s_idx_uni_b_c`, tableName)
			createIndex1Statement := fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s (b, c)`, index1Name, tableName)

			index2Name := fmt.Sprintf(`%s_idx_d`, tableName)
			createIndex2Statement := fmt.Sprintf(`CREATE INDEX %s ON %s (d)`, index2Name, tableName)

			// Create test data in the DB
			{
				createTableStatement := fmt.Sprintf(`CREATE TABLE %s (
				a VARCHAR(255) PRIMARY KEY,
				b INT NOT NULL,
				c CHAR(36) NOT NULL,
				D INT
				)`, tableName)
				for _, stmnt := range []string{createTableStatement, createIndex1Statement, createIndex2Statement} {
					_, err := dbClient.Query(stmnt)
					assert.NoError(err, "expected statement to execute succesfully", stmnt)
				}
			}

			showTablesResult, err := dbClient.Query(
				fmt.Sprintf("SHOW INDEXES FROM %s", tableName),
			)
			assert.NoError(err, "expected SHOW INDEXES to succeed")

			assert.Equal(
				[]string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"},
				showTablesResult.Columns,
				"Should have all standards SHOW INDEXES columns",
			)
			assert.Len(showTablesResult.Rows, 4)
		})
	}
}
