package main

import (
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

var Version = "0.0.1"

func main() {
	app := &cli.App{
		Name:                 "cassandra-migrate",
		Usage:                "Cassandra migration tool",
		EnableBashCompletion: true,
		Version:              Version,
	}
	UpCommand(app)
	DownCommand(app)
	NewCommand(app)
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
