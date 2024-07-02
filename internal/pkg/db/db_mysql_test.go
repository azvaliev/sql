package db_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

func TestDBMySQLConnOptions(t *testing.T) {
	connOptions := db.DBConnOptions{
		Flavor:       db.MySQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "user",
		Password:     "password",
		Port:         3306,
		SafeMode:     true,
	}

	for _, mySQLVersion := range TESTED_MYSQL_VERSIONS {
		t.Run(fmt.Sprintf("MySQL %s Connection", mySQLVersion), func(t *testing.T) {
			mySQLVersion := mySQLVersion
			assert := assert.New(t)

			ctx := context.Background()
			container, err := initMySQLTestDB(
				&InitTestDBOptions{mySQLVersion, &connOptions},
				ctx,
			)
			assert.NoError(err)

			defer testDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&connOptions)
			assert.NoError(err)

			// Using version function
			{
				result, err := dbClient.Query("SELECT VERSION()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				version := result.Rows[0]["VERSION()"].ToString()

				assert.Regexp(
					regexp.MustCompile(
						fmt.Sprint(mySQLVersion, `\.\d+`),
					),
					version,
				)
			}

			// Check database name setting
			{
				result, err := dbClient.Query("SELECT DATABASE()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				actualDatabaseName := result.Rows[0]["DATABASE()"].ToString()

				assert.Equal(connOptions.DatabaseName, actualDatabaseName)
			}

			// Check safe updates setting
			{
				result, err := dbClient.Query("SELECT @@SQL_SAFE_UPDATES AS SAFE_UPDATES_ENABLED")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				actualSafeMode := result.Rows[0]["SAFE_UPDATES_ENABLED"].ToString()

				assert.Equal(fmt.Sprint(1), actualSafeMode)
			}
		})
	}
}

func TestDBMySQLDescribe(t *testing.T) {
	connOptions := db.DBConnOptions{
		Flavor:       db.MySQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "buser",
		Password:     "password",
		Port:         3306,
		SafeMode:     true,
	}

	for _, mySQLVersion := range TESTED_MYSQL_VERSIONS {
		t.Run(fmt.Sprintf("MySQL %s - DESCRIBE", mySQLVersion), func(t *testing.T) {
			mySQLVersion := mySQLVersion
			assert := assert.New(t)

			ctx := context.Background()
			container, err := initMySQLTestDB(&InitTestDBOptions{mySQLVersion, &connOptions}, ctx)
			assert.NoError(err)

			defer testDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&connOptions)
			assert.NoError(err)

			// Create a table we can describe later
			const tableName string = "test"
			_, err = dbClient.Query(fmt.Sprintf(`
		CREATE TABLE %s(
			id int NOT NULL PRIMARY KEY auto_increment,
			external_id CHAR(32),
			UNIQUE (external_id),
			created_at DATETIME NOT NULL DEFAULT NOW(),
			INDEX (created_at)
		)
	`, tableName))
			assert.NoError(err)

			describeResult, err := dbClient.Query(fmt.Sprintf("DESCRIBE %s", tableName))
			assert.NoError(err)

			// Check if column names match order and values
			expectedColumnNames := []string{"id", "external_id", "created_at"}
			actualColumnNames := make([]string, len(expectedColumnNames))
			for i, describeColumnResult := range describeResult.Rows {
				actualColumnNames[i] = describeColumnResult["Field"].ToString()
			}

			assert.Equal(expectedColumnNames, actualColumnNames)

			// Validate describe output
			for _, row := range describeResult.Rows {
				assert.Len(row, 6)

				switch row["Field"].ToString() {
				case "id":
					{
						assert.Equal("int", row["Type"].ToString())
						assert.Equal("NO", row["Null"].ToString())
						assert.Equal("PRI", row["Key"].ToString())
						assert.Equal("NULL", row["Default"].ToString())
						assert.Equal("auto_increment", row["Extra"].ToString())
						break
					}
				case "external_id":
					{
						assert.Equal("char(32)", row["Type"].ToString())
						assert.Equal("YES", row["Null"].ToString())
						assert.Equal("UNI", row["Key"].ToString())
						assert.Equal("NULL", row["Default"].ToString())
						assert.Empty(row["Extra"].ToString())
						break
					}
				case "created_at":
					{
						assert.Equal("datetime", row["Type"].ToString())
						assert.Equal("NO", row["Null"].ToString())
						assert.Equal("MUL", row["Key"].ToString())
						assert.Equal("CURRENT_TIMESTAMP", row["Default"].ToString())
						assert.Equal("DEFAULT_GENERATED", row["Extra"].ToString())
						break
					}
				default:
					{
						assert.Fail(fmt.Sprint("Unexpected column", row["Field"].ToString()))
						break
					}
				}
			}
		})
	}
}
