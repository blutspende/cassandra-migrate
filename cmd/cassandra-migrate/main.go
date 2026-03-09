package main

import (
	"errors"
	"fmt"
	migrate "github.com/blutspende/cassandra-migrate"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

var Version = "0.0.1"

type cliOptions struct {
	ConfigFile        string
	Environment       string
	IgnoreExistErrors bool
}

func main() {
	app := newApp()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func newApp() *cli.App {
	opts := migrate.DefaultOptions()
	cliOpts := &cliOptions{
		ConfigFile:        opts.ConfigFile,
		Environment:       opts.Environment,
		IgnoreExistErrors: opts.IgnoreExistErrors,
	}

	return &cli.App{
		Name:                 "cassandra-migrate",
		Usage:                "Cassandra migration tool",
		EnableBashCompletion: true,
		Version:              Version,
		Commands: []*cli.Command{
			{
				Name:        "up",
				Description: "Migrate to the most recent version",
				Usage:       "cassandra-migrate up",
				Flags:       commonFlags(cliOpts),
				Action: func(c *cli.Context) error {
					conf, err := migrate.GetConfigFrom(cliOpts.ConfigFile, cliOpts.Environment, cliOpts.IgnoreExistErrors)
					if err != nil {
						return err
					}
					result, err := migrate.ApplyUp(conf)
					fmt.Println(fmt.Sprintf("Applied %d of %d migrations", result.AppliedCount, result.PendingCount))
					return err
				},
			},
			{
				Name:        "down",
				Description: "Undo the most recent migration",
				Usage:       "cassandra-migrate down",
				Flags:       commonFlags(cliOpts),
				Action: func(c *cli.Context) error {
					conf, err := migrate.GetConfigFrom(cliOpts.ConfigFile, cliOpts.Environment, cliOpts.IgnoreExistErrors)
					if err != nil {
						return err
					}
					result, err := migrate.ApplyDown(conf)
					if err != nil {
						return err
					}
					if !result.Applied {
						fmt.Println("No migrations to apply")
						return nil
					}
					fmt.Println("Applied down migration", result.MigrationID)
					return nil
				},
			},
			{
				Name:        "new",
				Description: "Create a new migration file",
				Usage:       "cassandra-migrate new <name>",
				Flags:       commonFlags(cliOpts),
				Action: func(c *cli.Context) error {
					name := c.Args().Get(0)
					if name == "" {
						return errors.New("missing migration name")
					}
					conf, err := migrate.GetConfigFrom(cliOpts.ConfigFile, cliOpts.Environment, cliOpts.IgnoreExistErrors)
					if err != nil {
						return err
					}
					filePath, err := migrate.CreateMigration(conf, name)
					if err != nil {
						return err
					}
					fmt.Println(fmt.Sprintf("Created migration %s", filePath))
					return nil
				},
			},
		},
	}
}

func commonFlags(opts *cliOptions) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "config",
			Usage:       fmt.Sprintf("name of the config file (default: %s)", migrate.DefaultConfigFile),
			Required:    false,
			Value:       migrate.DefaultConfigFile,
			Destination: &opts.ConfigFile,
		},
		&cli.StringFlag{
			Name:        "env",
			Usage:       fmt.Sprintf("environment to use (default: %s)", migrate.DefaultConfigEnvironment),
			Required:    false,
			Value:       migrate.DefaultConfigEnvironment,
			Destination: &opts.Environment,
		},
		&cli.BoolFlag{
			Name:        "ignore",
			Usage:       "ignore already exists and does not exist errors in migrations",
			Required:    false,
			Value:       false,
			Destination: &opts.IgnoreExistErrors,
			Aliases:     []string{"i"},
		},
	}
}
