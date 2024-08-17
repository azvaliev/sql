package main

import (
	"context"
	"fmt"
	"os"

	"github.com/azvaliev/sql/cmd"
	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/azvaliev/sql/internal/pkg/db/conn"
	"github.com/azvaliev/sql/internal/pkg/ui"
)

func main() {
	connOptions := cmd.ParseArgs()
	connManager, err := conn.CreateConnectionManager(
		&connOptions,
		context.Background(),
	)
	dbClient, err := db.CreateDBClient(connManager)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	app := ui.Init(dbClient)
	if err = app.Run(); err != nil {
		panic(err)
	}
}
