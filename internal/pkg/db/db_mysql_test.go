package db_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"testing"
	"time"

	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

type InitTestDBOptions struct {
	Version     string
	ConnOptions *db.DBConnOptions
}

// active supported + active LTS versions
var TESTED_MYSQL_VERSIONS = [...]string{"8.0", "8.2", "8.3", "8.4"}

func initMySQLTestDB(opts InitTestDBOptions, ctx context.Context) (*mysql.MySQLContainer, error) {
	containerProps := []testcontainers.ContainerCustomizer{
		testcontainers.WithImage(
			fmt.Sprint("mysql:", opts.Version),
		),
		testcontainers.WithWaitStrategy(
			wait.
				ForLog("ready for connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second),
			wait.ForExposedPort(),
		),
	}
	connOptions := opts.ConnOptions

	if connOptions.DatabaseName != "" {
		containerProps = append(containerProps, mysql.WithDatabase(connOptions.DatabaseName))
	}
	if connOptions.User != "" {
		containerProps = append(containerProps, mysql.WithUsername(connOptions.User))
	}
	if connOptions.Password != "" {
		containerProps = append(containerProps, mysql.WithPassword(connOptions.Password))
	}

	container, err := mysql.RunContainer(ctx, containerProps...)
	if err != nil {
		return container, errors.Join(
			errors.New("failed to start MySQL container"),
			err,
		)
	}

	port, err := container.MappedPort(ctx, "3306/tcp")

	if err != nil {
		container.Terminate(ctx)
		return container, errors.Join(
			errors.New("Failed to get mapped port for 3306"),
		)
	}

	opts.ConnOptions.Port = uint16(port.Int())

	return container, nil
}

func createMySQLTestDBCleanup(ctx context.Context, container *mysql.MySQLContainer) {
	if err := container.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}
}

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
				InitTestDBOptions{mySQLVersion, &connOptions},
				ctx,
			)
			assert.NoError(err)

			defer createMySQLTestDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&connOptions)
			assert.NoError(err)

			// Using version function
			{
				result, err := dbClient.Query("SELECT VERSION()")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				version := result.Rows[0]["VERSION()"]

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

				actualDatabaseName := result.Rows[0]["DATABASE()"]

				assert.Equal(connOptions.DatabaseName, actualDatabaseName)
			}

			// Check safe updates setting
			{
				result, err := dbClient.Query("SELECT @@SQL_SAFE_UPDATES AS SAFE_UPDATES_ENABLED")
				assert.NoError(err)
				assert.Len(result.Rows, 1)

				actualSafeMode := result.Rows[0]["SAFE_UPDATES_ENABLED"]

				assert.Equal(fmt.Sprint(1), actualSafeMode)
			}
		})
	}
}

func TestDBMySQLDataDisplay(t *testing.T) {
	// Check display of all datatypes
	{
		cases := []struct {
			ColumnName    string
			Datatype      string
			ProvidedValue string
			ExpectedValue string
			// format string if we want to run a fn or something on the column
			// Ex: "AVG(%s)"
			SelectFmt string
		}{
			{
				ColumnName:    "tinyIntColumn",
				Datatype:      "TINYINT",
				ProvidedValue: "127",
				ExpectedValue: "127",
			},
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
				ColumnName:    "smallIntUnsignedColumn",
				Datatype:      "SMALLINT UNSIGNED",
				ProvidedValue: "65535",
				ExpectedValue: "65535",
			},
			{
				ColumnName:    "mediumIntColumn",
				Datatype:      "MEDIUMINT",
				ProvidedValue: "8388607",
				ExpectedValue: "8388607",
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
				ColumnName:    "intUnsignedColumn",
				Datatype:      "INT UNSIGNED",
				ProvidedValue: "4294967295",
				ExpectedValue: "4294967295",
			},
			{
				ColumnName:    "bigIntColumn",
				Datatype:      "BIGINT",
				ProvidedValue: "9223372036854775807",
				ExpectedValue: "9223372036854775807",
			},
			{
				ColumnName:    "bigIntUnsignedColumn",
				Datatype:      "BIGINT UNSIGNED",
				ProvidedValue: "18446744073709551615",
				ExpectedValue: "18446744073709551615",
			},
			{
				ColumnName:    "floatColumn",
				Datatype:      "FLOAT",
				ProvidedValue: "123.45",
				ExpectedValue: "123.45",
			},
			{
				ColumnName:    "doubleColumn",
				Datatype:      "DOUBLE",
				ProvidedValue: "123456.789",
				ExpectedValue: "123456.789",
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
				ColumnName:    "dateTimeColumn",
				Datatype:      "DATETIME",
				ProvidedValue: `"2023-06-01 12:30:45"`,
				ExpectedValue: `2023-06-01 12:30:45`,
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
				ColumnName:    "binaryColumn",
				Datatype:      "BINARY(5)",
				ProvidedValue: `0x0000000000`,
				ExpectedValue: "\x00\x00\x00\x00\x00",
			},
			{
				ColumnName:    "varbinaryColumn",
				Datatype:      "VARBINARY(50)",
				ProvidedValue: "0x000048656c6c6f",
				ExpectedValue: "\x00\x00Hello",
			},
			{
				ColumnName:    "tinyBlobColumn",
				Datatype:      "TINYBLOB",
				ProvidedValue: `"Short blob data"`,
				ExpectedValue: "Short blob data",
			},
			{
				ColumnName:    "tinyTextColumn",
				Datatype:      "TINYTEXT",
				ProvidedValue: `"Short text"`,
				ExpectedValue: "Short text",
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
				ColumnName:    "mediumBlobColumn",
				Datatype:      "MEDIUMBLOB",
				ProvidedValue: `"Medium blob data up to 16777215 bytes"`,
				ExpectedValue: "Medium blob data up to 16777215 bytes",
			},
			{
				ColumnName:    "mediumTextColumn",
				Datatype:      "MEDIUMTEXT",
				ProvidedValue: `"This is a medium-sized text string"`,
				ExpectedValue: "This is a medium-sized text string",
			},
			{
				ColumnName:    "longBlobColumn",
				Datatype:      "LONGBLOB",
				ProvidedValue: `"Long blob data up to 4294967295 bytes"`,
				ExpectedValue: "Long blob data up to 4294967295 bytes",
			},
			{
				ColumnName:    "longTextColumn",
				Datatype:      "LONGTEXT",
				ProvidedValue: `"This is a long text string that can be very, very long"`,
				ExpectedValue: "This is a long text string that can be very, very long",
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
				ColumnName:    "multiPointColumn",
				Datatype:      "MULTIPOINT",
				ProvidedValue: "ST_GeomFromText('MULTIPOINT(0 0,1 1)')",
				ExpectedValue: "MULTIPOINT((0 0),(1 1))",
				SelectFmt:     "ST_AsText(%s)",
			},
			{
				ColumnName:    "multiLineStringColumn",
				Datatype:      "MULTILINESTRING",
				ProvidedValue: "ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))')",
				ExpectedValue: "MULTILINESTRING((0 0,1 1),(2 2,3 3))",
				SelectFmt:     "ST_AsText(%s)",
			},
			{
				ColumnName:    "multiPolygonColumn",
				Datatype:      "MULTIPOLYGON",
				ProvidedValue: "ST_GeomFromText('MULTIPOLYGON(((0 0,1 1,1 0,0 0)),((2 2,3 3,3 2,2 2)))')",
				ExpectedValue: "MULTIPOLYGON(((0 0,1 1,1 0,0 0)),((2 2,3 3,3 2,2 2)))",
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
			ctx := context.Background()
			container, err := initMySQLTestDB(InitTestDBOptions{mySQLVersion, &connOptions}, ctx)
			assert.NoError(t, err)

			defer createMySQLTestDBCleanup(ctx, container)

			dbClient, err := db.CreateDBClient(&connOptions)
			assert.NoError(t, err)

			for idx, tt := range cases {
				t.Run(fmt.Sprintf("MySQL %s - %s display", mySQLVersion, tt.Datatype), func(t *testing.T) {
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

					dataSelectStatement := fmt.Sprintf("SELECT %s FROM %s", columnSelectExpression, tableName)
					result, err := dbClient.Query(dataSelectStatement)

					testCtx = append(testCtx, dataSelectStatement)
					assert.NoError(err, testCtx)

					assert.Len(result.Rows, 1, testCtx)
					data := result.Rows[0]

					assert.Len(data, 1, testCtx)
					assert.Equal(tt.ExpectedValue, data[tt.ColumnName], testCtx)

				})
			}
		}
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
			container, err := initMySQLTestDB(InitTestDBOptions{mySQLVersion, &connOptions}, ctx)
			assert.NoError(err)

			defer createMySQLTestDBCleanup(ctx, container)

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
				actualColumnNames[i] = describeColumnResult["Field"]
			}

			assert.Equal(expectedColumnNames, actualColumnNames)

			// Validate describe output
			for _, row := range describeResult.Rows {
				assert.Len(row, 6)

				switch row["Field"] {
				case "id":
					{
						assert.Equal("int", row["Type"])
						assert.Equal("NO", row["Null"])
						assert.Equal("PRI", row["Key"])
						assert.Equal("NULL", row["Default"])
						assert.Equal("auto_increment", row["Extra"])
						break
					}
				case "external_id":
					{
						assert.Equal("char(32)", row["Type"])
						assert.Equal("YES", row["Null"])
						assert.Equal("UNI", row["Key"])
						assert.Equal("NULL", row["Default"])
						assert.Empty(row["Extra"])
						break
					}
				case "created_at":
					{
						assert.Equal("datetime", row["Type"])
						assert.Equal("NO", row["Null"])
						assert.Equal("MUL", row["Key"])
						assert.Equal("CURRENT_TIMESTAMP", row["Default"])
						assert.Equal("DEFAULT_GENERATED", row["Extra"])
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
