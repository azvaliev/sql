package db_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

func TestDBPostgresConnOptions(t *testing.T) {
	connOptions := db.DBConnOptions{
		Flavor:       db.PostgreSQL,
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

			ctx := context.Background()
			container, err := initPostgresTestDB(
				&InitTestDBOptions{postgresVersion, &connOptions},
				ctx,
			)
			assert.NoError(err)

			defer createTestDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&connOptions)
			assert.NoError(err)

			// Using version function
			{
				result, err := dbClient.Query("SELECT VERSION()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				version := result.Rows[0]["version"]

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

				actualDatabaseName := result.Rows[0]["current_database"]

				assert.Equal(connOptions.DatabaseName, actualDatabaseName)
			}
		})
	}
}
