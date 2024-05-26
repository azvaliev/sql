package db

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DB struct {
	ctx         context.Context
	db          *sql.DB
	conn        *sql.Conn
	dsnProducer *DSNProducer
}

func CreateDB(
	connOptions DSNProducer,
) (*DB, error) {
	dataSourceName, err := connOptions.ToDSN()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to create connection string"),
			err,
		)
	}

	sqlDB, err := sql.Open(string(connOptions.GetFlavor()), dataSourceName)
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
		dsnProducer: &connOptions,
	}

	return &db, nil
}
