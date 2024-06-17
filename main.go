package main

import (
	"fmt"
	"os"

	"github.com/azvaliev/redline/internal/pkg/cli"
	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/azvaliev/redline/internal/pkg/ui"
)

func main() {
	connOptions := cli.ParseArgs()
	dbClient, err := db.CreateDBClient(&connOptions)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	app := ui.Init(dbClient)
	if err = app.Run(); err != nil {
		panic(err)
	}
}
