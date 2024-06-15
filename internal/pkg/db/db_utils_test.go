package db_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type InitTestDBOptions struct {
	Version     string
	ConnOptions *db.DBConnOptions
}

type TestDBContainer interface {
	Terminate(context.Context) error
}

// active supported + active LTS versions
var TESTED_MYSQL_VERSIONS = [...]string{"8.0", "8.2", "8.3", "8.4"}

// last 3 major versions
var TESTED_POSTGRES_VERSIONS = [...]string{"15", "16"}

// Create a test database container
// Make sure to call `defer createTestDBCleanup(container)` to clean this up
func initTestDB(opts *InitTestDBOptions, ctx context.Context) (TestDBContainer, error) {
	if opts == nil {
		return nil, errors.New("options must be provided")
	}

	switch opts.ConnOptions.Flavor {
	case db.MySQL:
		{
			return initMySQLTestDB(opts, ctx)
		}
	case db.PostgreSQL:
		{
			return initPostgresTestDB(opts, ctx)
		}
	default:
		{
			return nil, errors.New(fmt.Sprint("Invalid DB flavor: ", opts.ConnOptions.Flavor))
		}
	}
}

func initMySQLTestDB(opts *InitTestDBOptions, ctx context.Context) (*mysql.MySQLContainer, error) {
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

	opts.ConnOptions.Port = uint(port.Int())

	return container, nil
}

func initPostgresTestDB(opts *InitTestDBOptions, ctx context.Context) (*postgres.PostgresContainer, error) {
	containerProps := []testcontainers.ContainerCustomizer{
		testcontainers.WithImage(
			fmt.Sprint("postgres:", opts.Version),
		),
		testcontainers.WithWaitStrategy(
			wait.
				ForLog("database system is ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(20*time.Second),
			wait.ForExposedPort(),
		),
	}
	connOptions := opts.ConnOptions

	if connOptions.DatabaseName != "" {
		containerProps = append(containerProps, postgres.WithDatabase(connOptions.DatabaseName))
	}
	if connOptions.User != "" {
		containerProps = append(containerProps, postgres.WithUsername(connOptions.User))
	}
	if connOptions.Password != "" {
		containerProps = append(containerProps, postgres.WithPassword(connOptions.Password))
	}

	container, err := postgres.RunContainer(ctx, containerProps...)
	if err != nil {
		return container, errors.Join(
			errors.New("failed to start MySQL container"),
			err,
		)
	}

	port, err := container.MappedPort(ctx, "5432/tcp")

	if err != nil {
		container.Terminate(ctx)
		return container, errors.Join(
			errors.New("Failed to get mapped port for 5432"),
		)
	}

	opts.ConnOptions.Port = uint(port.Int())

	return container, nil
}

func createTestDBCleanup(ctx context.Context, container TestDBContainer) {
	if err := container.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}
}
