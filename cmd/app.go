package cmd

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/azvaliev/sql/internal/pkg/db/conn"
)

const (
	mySQLUsage             = "Specify for MySQL database"
	postgreSQLUsage        = "Specify for PostgreSQL database"
	hostUsage              = "Database host - ex: localhost , remote.example.com"
	databaseNameUsage      = "Database name to connect to"
	userUsage              = "User name for logging into the database"
	passwordUsage          = "Password for logging into the database"
	portUsage              = "Port, defaults based on MySQL/PostgreSQL default port"
	safeModeUsage          = "MySQL option to prevent unintended delete/updates.\n See https://dev.mysql.com/doc/refman/8.4/en/mysql-tips.html#safe-updates for more details"
	additionalOptionsUsage = "Provide additional options as flags. Example: -additional-options=foo=bar,bar=baz"
)

func ParseArgs() conn.DSNOptions {
	parsedArgs := conn.DSNOptions{}

	// Register all the flags
	{
		setPostgreSQLDB := func(string) error {
			parsedArgs.Flavor = conn.PostgreSQL
			return nil
		}
		setMySQLDB := func(string) error {
			parsedArgs.Flavor = conn.MySQL
			return nil
		}

		flag.BoolFunc("mysql", mySQLUsage, setMySQLDB)
		flag.BoolFunc("psql", postgreSQLUsage, setPostgreSQLDB)
		flag.BoolFunc("postgres", postgreSQLUsage, setPostgreSQLDB)

		flag.StringVar(&parsedArgs.Host, "h", "", hostUsage)
		flag.StringVar(&parsedArgs.Host, "host", "", hostUsage)

		flag.StringVar(&parsedArgs.DatabaseName, "d", "", databaseNameUsage)
		flag.StringVar(&parsedArgs.DatabaseName, "database", "", databaseNameUsage)

		flag.StringVar(&parsedArgs.User, "u", "", userUsage)
		flag.StringVar(&parsedArgs.User, "user", "", userUsage)

		flag.StringVar(&parsedArgs.Password, "p", "", passwordUsage)
		flag.StringVar(&parsedArgs.Password, "password", "", passwordUsage)

		flag.UintVar(&parsedArgs.Port, "P", 0, portUsage)
		flag.UintVar(&parsedArgs.Port, "port", 0, portUsage)

		flag.BoolVar(&parsedArgs.SafeMode, "s", false, safeModeUsage)
		flag.BoolVar(&parsedArgs.SafeMode, "safe", false, safeModeUsage)

		flag.Func("additional-options", additionalOptionsUsage, func(rawOpts string) error {
			splitOpts := strings.Split(rawOpts, ",")
			if parsedArgs.AdditionalOptions == nil {
				parsedArgs.AdditionalOptions = make(map[string]string, len(splitOpts))
			}

			for _, splitOpt := range splitOpts {
				optParts := strings.Split(splitOpt, "=")
				key := optParts[0]

				// Options without a value we will leave the value as "", for conn_opts to interpret
				var value string
				if len(optParts) > 1 {
					value = optParts[1]
				}

				parsedArgs.AdditionalOptions[key] = value
			}

			return nil
		})
	}

	flag.Parse()

	err := parsedArgs.Validate()
	if err != nil {
		fmt.Printf("Unable to proceed with specified arguments: \n%s\n\n", err.Error())
		flag.Usage()
		os.Exit(2)
	}

	return parsedArgs
}
