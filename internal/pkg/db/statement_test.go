package db_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

// Test transformed DB queries

var showTablesTestSuite = []struct {
	ConnOptions          db.DBConnOptions
	ShowTablesColumnName string
	DBVersions           []string
}{
	{
		db.DBConnOptions{
			Flavor:       db.PostgreSQL,
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
		db.DBConnOptions{
			Flavor:       db.MySQL,
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

				/// Initialize test DB / dependencies
				ctx := context.Background()
				testDbOptions := InitTestDBOptions{dbVersion, &testSuite.ConnOptions}
				container, err := initTestDB(&testDbOptions, ctx)
				assert.NoError(err)

				defer createTestDBCleanup(ctx, container)

				dbClient, err := db.CreateDBClient(&testSuite.ConnOptions)
				assert.NoError(err)

				/// Create a few test tables

				expectedTableNames := []string{"table_one", "table_two", "table_three"}
				for _, tableName := range expectedTableNames {
					query := fmt.Sprintf("CREATE TABLE %s (id int)", tableName)
					_, err = dbClient.Query(query)
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

//
