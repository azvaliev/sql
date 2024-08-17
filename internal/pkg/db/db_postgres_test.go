package db_test

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/azvaliev/sql/internal/pkg/db/conn"
	"github.com/stretchr/testify/assert"
)

func TestDBPostgresConnOptions(t *testing.T) {
	connOptions := conn.DSNOptions{
		Flavor:       conn.PostgreSQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "user",
		Password:     "password",
		Port:         5432,
	}

	for _, postgresVersion := range TESTED_POSTGRES_VERSIONS {
		t.Run(fmt.Sprintf("PostgreSQL %s Connection", postgresVersion), func(t *testing.T) {
			postgresVersion := postgresVersion
			assert := assert.New(t)

			dbClient, cleanup := mustInitTestDBWithClient(
				&InitTestDBOptions{postgresVersion, &connOptions},
				assert,
			)
			defer cleanup()

			// Using version function
			{
				result, err := dbClient.Query("SELECT VERSION()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				version := result.Rows[0]["version"].ToString()

				assert.Regexp(
					regexp.MustCompile(
						fmt.Sprint("PostgreSQL ", postgresVersion, `\.\d+`),
					),
					version,
				)
			}

			// Check database name setting
			{
				result, err := dbClient.Query("SELECT current_database()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				actualDatabaseName := result.Rows[0]["current_database"].ToString()

				assert.Equal(connOptions.DatabaseName, actualDatabaseName)
			}
		})
	}
}

func TestDBPostgresDescribe(t *testing.T) {
	connOptions := conn.DSNOptions{
		Flavor:       conn.PostgreSQL,
		Host:         "localhost",
		DatabaseName: "test",
		User:         "user",
		Password:     "password",
		Port:         5432,
	}

	for _, postgresVersion := range TESTED_POSTGRES_VERSIONS {
		t.Run(fmt.Sprintf("Postgres %s - DESCRIBE", postgresVersion), func(t *testing.T) {
			postgresVersion := postgresVersion
			assert := assert.New(t)

			dbClient, cleanup := mustInitTestDBWithClient(
				&InitTestDBOptions{postgresVersion, &connOptions},
				assert,
			)
			defer cleanup()

			// Create a table we can describe later
			const tableName string = "test"
			_, err := dbClient.Query(fmt.Sprintf(`
		CREATE TABLE %s(
			id SERIAL NOT NULL PRIMARY KEY,
			external_id CHAR(32),
			UNIQUE (external_id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, tableName))
			assert.NoError(err)

			_, err = dbClient.Query(fmt.Sprintf(`
				CREATE INDEX idx_created_at
				ON %s(created_at)
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
				assert.Len(row, 5)

				switch row["Field"].ToString() {
				case "id":
					{
						assert.Equal("integer", row["Type"].ToString())
						assert.Equal("NO", row["Null"].ToString())
						assert.Equal("PRI", row["Key"].ToString())
						assert.Equal("nextval('test_id_seq'::regclass)", row["Default"].ToString())
						break
					}
				case "external_id":
					{
						assert.Equal("character(32)", row["Type"].ToString())
						assert.Equal("YES", row["Null"].ToString())
						assert.Equal("UNI", row["Key"].ToString())
						assert.Equal("NULL", row["Default"].ToString())
						break
					}
				case "created_at":
					{
						assert.Equal("timestamp with time zone", row["Type"].ToString())
						assert.Equal("NO", row["Null"].ToString())
						assert.Equal("MUL", row["Key"].ToString())
						assert.Equal("now()", row["Default"].ToString())
						break
					}
				default:
					{
						assert.Fail(fmt.Sprint("Unexpected column", row["Field"]))
						break
					}
				}
			}
		})
	}
}
