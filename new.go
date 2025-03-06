package main

import (
	"errors"
	"fmt"
	"github.com/urfave/cli/v2"
	"os"
	"path"
	"regexp"
	"strings"
	"text/template"
	"time"
)

var templateContent = `
-- +migrate Up

-- +migrate Down
`
var tpl = template.Must(template.New("new_migration").Parse(templateContent))

func NewCommand(c *cli.App) {
	command := cli.Command{
		Name:        "new",
		Description: "Create a new migration file",
		Usage:       "cassandra-migrate new <name>",
		Flags:       append([]cli.Flag{
			// Add command-specific flags here
		}, CommonFlags...),
		Action: func(c *cli.Context) error {
			args := c.Args()
			name := args.Get(0)
			if name == "" {
				return errors.New("missing migration name")
			}
			conf, err := GetConfig()
			if err != nil {
				return err
			}

			if _, err := os.Stat(conf.MigrationDir); os.IsNotExist(err) {
				return err
			}

			filePath := path.Join(conf.MigrationDir, generateFileName(name))
			f, err := os.Create(filePath)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()

			if err := tpl.Execute(f, nil); err != nil {
				return err
			}

			println(fmt.Sprintf("Created migration %s", filePath))
			return nil
		},
	}
	c.Commands = append(c.Commands, &command)
}

func CreateMigration(name string) error {
	conf, err := GetConfig()
	if err != nil {
		return err
	}

	if _, err := os.Stat(conf.MigrationDir); os.IsNotExist(err) {
		return err
	}

	filePath := path.Join(conf.MigrationDir, generateFileName(name))
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if err := tpl.Execute(f, nil); err != nil {
		return err
	}

	println(fmt.Sprintf("Created migration %s", filePath))
	return nil
}

var specialCharactersRegex = regexp.MustCompile("[^A-Za-z0-9]+")

func generateFileName(filename string) string {
	name := specialCharactersRegex.ReplaceAllString(strings.TrimSpace(filename), "-")
	return fmt.Sprintf("%s-%s.cql", time.Now().Format("20060102150405"), name)
}
