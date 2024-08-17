package conn

import (
	"context"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

type ConnectionManager struct {
	sqlDB      *sqlx.DB
	conn       *sqlx.Conn
	dsnManager DSNManager
	ctx        context.Context
}

func CreateConnectionManager(
	dsnManager DSNManager,
	ctx context.Context,
) (*ConnectionManager, error) {
	sqlDB, err := createDB(dsnManager)
	if err != nil {
		return nil, err
	}

	return &ConnectionManager{
		sqlDB:      sqlDB,
		conn:       nil,
		dsnManager: dsnManager,
		ctx:        ctx,
	}, nil
}

func createDB(dsnManager DSNManager) (*sqlx.DB, error) {
	dataSourceName, err := dsnManager.GetDSN()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to create connection string"),
			err,
		)
	}

	sqlDB, err := sqlx.Open(string(dsnManager.GetFlavor()), dataSourceName)
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to open database"),
			err,
		)
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to establish connection to database"),
			err,
		)
	}

	// Keep connections alive for 5 mins
	sqlDB.SetConnMaxLifetime(time.Minute * 5)

	// Only should ever have a single connection
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	return sqlDB, nil
}

func (connManager *ConnectionManager) Destroy() {
	// Cleanup database resources
	// Call before this struct drops out of scope
	// This only returns an error if the connection is already closed, safe to ignore
	_ = connManager.conn.Close()
	_ = connManager.sqlDB.Close()

	connManager.sqlDB = nil
	connManager.conn = nil
}

func (connManager *ConnectionManager) GetFlavor() DBFlavor {
	return connManager.dsnManager.GetFlavor()
}

func (connManager *ConnectionManager) UseDatabase(databaseName string) error {
	connManager.dsnManager.SetDatabase(databaseName)

	newDB, err := createDB(connManager.dsnManager)
	if err != nil {
		return errors.Join(
			errors.New("Failed to switch database"),
			err,
		)
	}

	// Once we have succesfully connected to new database, cleanup the old instance
	connManager.Destroy()
	connManager.sqlDB = newDB

	return nil
}

// We try to use a single connection, instantiated when DBClient is instantiated
// This will either return that existing connection, or create a new one if that got dropped
func (connManager *ConnectionManager) GetConnection() (*sqlx.Conn, error) {
	if connManager.conn != nil {
		// See if our existing connection is still alive
		err := connManager.conn.PingContext(connManager.ctx)
		if err == nil {
			return connManager.conn, nil
		}
		connManager.conn.Close()
	}

	conn, err := connManager.sqlDB.Connx(connManager.ctx)

	if err != nil {
		return nil, errors.Join(
			errors.New("Failed to get connection to database"),
			err,
		)
	}

	if connManager.dsnManager.IsSafeMode() {
		_, err = conn.ExecContext(connManager.ctx, "SET SQL_SAFE_UPDATES = 1")
		if err != nil {
			return nil, err
		}
	}

	connManager.conn = conn
	return connManager.conn, nil
}
