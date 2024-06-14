package db_test

import (
	"strings"
	"testing"

	"github.com/azvaliev/redline/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

func TestDBConnOptionsMySQL(t *testing.T) {
	var tests = []struct {
		Name               string
		ConnOptions        *db.DBConnOptions
		ExpectedConnString string
		ExpectedQuery      []string
	}{
		{
			Name: "All Options",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.MySQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "password",
				Port:         3306,
				AdditionalOptions: map[string]string{
					"tls":             "preferred",
					"clientFoundRows": "",
					"parseTime":       "false",
				},
			},
			ExpectedConnString: "root:password@tcp(localhost:3306)/test",
			ExpectedQuery:      []string{"clientFoundRows=true", "parseTime=false", "tls=preferred"},
		},
		{
			Name: "One Additional Option",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.MySQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "password",
				Port:         3306,
				AdditionalOptions: map[string]string{
					"tls": "preferred",
				},
			},
			ExpectedConnString: "root:password@tcp(localhost:3306)/test",
			ExpectedQuery:      []string{"tls=preferred"},
		},
		{
			Name: "No Options",
			ConnOptions: &db.DBConnOptions{
				Flavor: db.MySQL,
			},
			ExpectedConnString: "/",
		},
		{
			Name: "No Additional Options",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.MySQL,
				Host:         "localhost",
				DatabaseName: "bar",
				User:         "john",
				Password:     "doe",
				Port:         3306,
			},
			ExpectedConnString: "john:doe@tcp(localhost:3306)/bar",
		},
		{
			Name: "No Port Specified",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.MySQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "password",
			},
			ExpectedConnString: "root:password@tcp(localhost)/test",
		},
		{
			Name: "No Host Specified",
			ConnOptions: &db.DBConnOptions{
				Flavor:   db.MySQL,
				Port:     3306,
				User:     "root",
				Password: "password",
			},
			ExpectedConnString: "root:password@/",
		},
		{
			Name: "Infer Unix",
			ConnOptions: &db.DBConnOptions{
				Flavor: db.MySQL,
				Port:   3306,
				User:   "root",
				Host:   "/tmp/mysql.sock",
			},
			ExpectedConnString: "root@unix(/tmp/mysql.sock)/",
		}, {
			Name: "Infer Unix from Abstract Namespace",
			ConnOptions: &db.DBConnOptions{
				Flavor: db.MySQL,
				Port:   3306,
				User:   "root",
				Host:   "@/var/run/usbmuxd",
			},
			ExpectedConnString: "root@unix(@/var/run/usbmuxd)/",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test := test

			assert := assert.New(t)
			t.Parallel()

			fullConnString, err := test.ConnOptions.GetDSN()
			assert.Nil(err)

			// Seperate the base connection string from query options
			fullConnStringParts := strings.Split(fullConnString, "?")
			connString := fullConnStringParts[0]

			var connQueryString string
			if len(fullConnStringParts) > 1 {
				connQueryString = fullConnStringParts[1]
			}

			assert.Equal(test.ExpectedConnString, connString)

			if test.ExpectedQuery != nil {
				assert.ElementsMatch(
					test.ExpectedQuery,
					strings.Split(connQueryString, "&"),
				)
			} else {
				assert.Len(fullConnStringParts, 1)
				assert.Empty(connQueryString)
			}
		})
	}
}

func TestDBConnOptionsPostgreSQL(t *testing.T) {
	var tests = []struct {
		Name                    string
		ConnOptions             *db.DBConnOptions
		ExpectedConnStringParts []string
	}{
		{
			Name: "All Options",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.PostgreSQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "password",
				Port:         5432,
				AdditionalOptions: map[string]string{
					"sslmode":    "verify-ca",
					"requiressl": "",
				},
			},
			ExpectedConnStringParts: []string{
				"host=localhost",
				"dbname=test",
				"user=root",
				"password=password",
				"port=5432",
				"sslmode=verify-ca",
				"requiressl=1",
			},
		},
		{
			Name: "No Additional Options",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.PostgreSQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "password",
				Port:         5432,
			},
			ExpectedConnStringParts: []string{
				"host=localhost",
				"dbname=test",
				"user=root",
				"password=password",
				"port=5432",
			},
		},
		{
			Name: "Some Empty Options",
			ConnOptions: &db.DBConnOptions{
				Flavor:       db.PostgreSQL,
				Host:         "localhost",
				DatabaseName: "test",
				User:         "root",
				Password:     "",
			},
			ExpectedConnStringParts: []string{
				"host=localhost",
				"dbname=test",
				"user=root",
			},
		},
		{
			Name: "No Options",
			ConnOptions: &db.DBConnOptions{
				Flavor: db.PostgreSQL,
			},
			ExpectedConnStringParts: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test := test
			assert := assert.New(t)
			t.Parallel()

			fullConnString, err := test.ConnOptions.GetDSN()
			assert.Nil(err)

			actualConnStringParts := []string{}
			if fullConnString != "" {
				actualConnStringParts = strings.Split(fullConnString, " ")
			}

			assert.ElementsMatch(
				test.ExpectedConnStringParts,
				actualConnStringParts,
			)
		})
	}
}

func TestDBConnOptionsInvalidFlavor(t *testing.T) {
	assert := assert.New(t)

	connOptions := db.DBConnOptions{
		Flavor:   "invalid",
		Host:     "localhost",
		User:     "root",
		Password: "password",
		Port:     1234,
	}
	connOptionsString, err := connOptions.GetDSN()

	assert.Empty(connOptionsString)
	assert.Error(err)
}
