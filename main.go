package main

import (
	"fmt"

	"github.com/azvaliev/redline/internal/pkg/cli"
)

func main() {
	connOptions := cli.ParseArgs()

	fmt.Printf("\nconn options\n%+v\n\n", connOptions)
}
