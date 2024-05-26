package db

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// What type of SQL database is connected
type DBFlavor string

const (
	MySQL      DBFlavor = "mysql"
	PostgreSQL DBFlavor = "pgx"
)

type DB struct {
	ctx         context.Context
	db          *sql.DB
	conn        *sql.Conn
	connOptions *DBConnOptions
}

func CreateDB(
	connOptions DBConnOptions,
) (*DB, error) {
	dataSourceName, err := connOptions.ToString()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to create connection string"),
			err,
		)
	}

	sqlDB, err := sql.Open(connOptions.Host, dataSourceName)
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to open database"),
			err,
		)
	}

	ctx := context.Background()
	conn, err := sqlDB.Conn(ctx)

	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to establish connection to database"),
			err,
		)
	}

	err = conn.PingContext(ctx)
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to ping database"),
			err,
		)
	}

	db := DB{
		ctx:         ctx,
		db:          sqlDB,
		conn:        conn,
		connOptions: &connOptions,
	}

	return &db, nil
}
