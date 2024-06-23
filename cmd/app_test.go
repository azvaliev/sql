package cmd_test

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/azvaliev/sql/cmd"
	"github.com/azvaliev/sql/internal/pkg/db"
	"github.com/stretchr/testify/assert"
)

func TestParseArgsNoFlavor(t *testing.T) {
	// Idea from: https://go.dev/talks/2014/testing.slide#1
	// Essentially spawning a new process, because lack of DB Flavor will os.exit
	if os.Getenv("BE_CRASHER") == "1" {
		cmd.ParseArgs()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestParseArgsNoFlavor")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && e.ExitCode() == 2 {
		return
	}

	t.Fatalf("process ran with err %v, want exit status 2", err)
}

var testCases = []struct {
	Name               string
	Args               []string
	ExpectedParsedArgs db.DBConnOptions
}{
	{
		Name: "MySQL Flavor w/ defaults",
		Args: []string{"-mysql"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.MySQL,
		},
	},
	{
		Name: "-postgres with defaults",
		Args: []string{"-postgres"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.PostgreSQL,
		},
	},
	{
		Name: "-psql with defaults",
		Args: []string{"-psql"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.PostgreSQL,
		},
	},
	{
		Name: "MySQL with filled out options",
		Args: []string{"-mysql", "-h", "localhost", "-P", "3306", "-u", "user", "-p=password", "--safe"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor:   db.MySQL,
			Host:     "localhost",
			Port:     3306,
			User:     "user",
			Password: "password",
			SafeMode: true,
		},
	},
	{
		Name: "PostgreSQL with filled out options",
		Args: []string{"-psql", "--host=remote.example.com", "--port=5432", "--user=postgres"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.PostgreSQL,
			Host:   "remote.example.com",
			Port:   5432,
			User:   "postgres",
		},
	},
	{
		Name: "MySQL with additional options",
		Args: []string{"-mysql", "--additional-options=hello=world,bar=baz"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.MySQL,
			AdditionalOptions: map[string]string{
				"hello": "world",
				"bar":   "baz",
			},
		},
	},
	{
		Name: "PostgreSQL with additional options",
		Args: []string{"-postgres", "--additional-options=testing=foo,test2=bar"},
		ExpectedParsedArgs: db.DBConnOptions{
			Flavor: db.PostgreSQL,
			AdditionalOptions: map[string]string{
				"testing": "foo",
				"test2":   "bar",
			},
		},
	},
}

func TestParseArgs(t *testing.T) {
	originalArgs := os.Args
	programName := originalArgs[0]

	resetFlagsArgs := func() {
		os.Args = originalArgs

		// Reset flag registration
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}
	setArgs := func(args []string) {
		os.Args = append([]string{programName}, args...)
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			setArgs(testCase.Args)
			defer resetFlagsArgs()

			actualParsedArgs := cmd.ParseArgs()
			assert.Equal(t, testCase.ExpectedParsedArgs, actualParsedArgs, "expected parsed args to match", strings.Join(testCase.Args, " "))
		})
	}
}
