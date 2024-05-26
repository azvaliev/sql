package db

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type DBConnOptions struct {
	Flavor       DBFlavor
	Host         string
	DatabaseName string
	User         string
	Password     string
	Port         uint16
	// Only works in MySQL
	SafeMode          bool
	AdditionalOptions map[string]string
}

func (connOptions *DBConnOptions) additionalOptionsToQueryParts() *[]string {
	if connOptions.AdditionalOptions == nil || len(connOptions.AdditionalOptions) == 0 {
		return nil
	}

	queryParts := []string{}

	for key, value := range connOptions.AdditionalOptions {
		if value == "" {
			var trueValue = "true"

			switch connOptions.Flavor {
			case MySQL:
				{
					trueValue = "true"
					break
				}
			case PostgreSQL:
				{
					trueValue = "1"
				}
			}

			queryParts = append(queryParts, fmt.Sprint(key, "=", url.QueryEscape(trueValue)))
		} else {
			queryParts = append(queryParts, fmt.Sprint(key, "=", url.QueryEscape(value)))
		}

	}

	return &queryParts
}

func (connOptions *DBConnOptions) additionalOptionsToString() string {
	queryParts := connOptions.additionalOptionsToQueryParts()
	if queryParts == nil {
		return ""
	}

	return "?" + strings.Join(*queryParts, "&")
}

func (connOptions *DBConnOptions) getNetwork() string {
	if connOptions.Host == "" {
		return ""
	}

	var firstHostChar = string(connOptions.Host[0])
	var hostIsUnixSocket bool = firstHostChar == "@" || firstHostChar == "/"

	if hostIsUnixSocket {
		return "unix"
	} else {
		return "tcp"
	}
}

func (connOptions *DBConnOptions) ToString() (string, error) {
	switch connOptions.Flavor {
	case MySQL:
		{
			config := mysql.NewConfig()
			network := connOptions.getNetwork()

			var addr strings.Builder

			addr.WriteString(connOptions.Host)
			if connOptions.Port != 0 && network == "tcp" {
				addr.WriteString(fmt.Sprint(":", connOptions.Port))
			}

			config.Addr = addr.String()
			config.Net = network
			config.DBName = connOptions.DatabaseName
			config.User = connOptions.User
			config.Passwd = connOptions.Password

			dsn := config.FormatDSN()
			additionalOptions := connOptions.additionalOptionsToString()

			return fmt.Sprint(dsn, additionalOptions), nil
		}
	case PostgreSQL:
		{
			options := map[string]string{}

			options["host"] = connOptions.Host
			if connOptions.Port != 0 {
				options["port"] = fmt.Sprint(connOptions.Port)
			}
			options["dbname"] = connOptions.DatabaseName
			options["user"] = connOptions.User
			options["password"] = connOptions.Password

			outputParts := []string{}
			for key, val := range options {
				if val != "" {
					outputParts = append(outputParts, fmt.Sprint(key, "=", val))
				}
			}

			additionalOptions := connOptions.additionalOptionsToQueryParts()
			if additionalOptions != nil {
				outputParts = append(outputParts, *additionalOptions...)
			}

			return strings.Join(outputParts, " "), nil
		}
	default:
		{
			return "", errors.New(fmt.Sprintf("Unknown database type %s", connOptions.Flavor))
		}
	}
}
