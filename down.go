package main

import (
	"bytes"
	"fmt"
	"github.com/blutspende/cassandra-migrate/sqlparse"
	"github.com/gocql/gocql"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func DownCommand(c *cli.App) {
	command := cli.Command{
		Name:        "down",
		Description: "Undo the most recent migration",
		Usage:       "cassandra-migrate down",
		Flags:       append([]cli.Flag{}, CommonFlags...),
		Action: func(c *cli.Context) error {
			conf, err := GetConfig()
			if err != nil {
				return err
			}
			migrationFiles, err := filepath.Glob(filepath.Join(conf.MigrationDir, "*.cql"))
			if err != nil {
				return err
			}
			port, err := strconv.Atoi(conf.Connection.Port)
			if err != nil {
				return err
			}
			session, err := GetConnection(conf.Connection.Hosts, port, "", conf.Connection.Username, conf.Connection.Password)
			if err != nil {
				return err
			}
			defer session.Close()
			id, err := GetLatestMigrationID(conf.Keyspace, session)
			if err == gocql.ErrNotFound {
				fmt.Println("No migrations to apply")
				return nil
			}
			if err != nil {
				return err
			}
			var filename string
			for i := range migrationFiles {
				if filepath.Base(migrationFiles[i]) == id {
					filename = migrationFiles[i]
					break
				}
			}
			content, err := os.ReadFile(filename)
			if err != nil {
				return err
			}
			migration, err := sqlparse.ParseMigration(bytes.NewReader(content))
			if err != nil {
				return err
			}
			for _, statement := range migration.DownStatements {
				err = session.Query(statement).Exec()
				if err != nil {
					if conf.IgnoreExistErrors && IsExistError(err) {
						fmt.Println("WRN: error occurred while executing statement: ", statement, " error: ", err.Error(), " (ignored due to 'ignore' flag)")
						continue
					}
					fmt.Println("ERR: error occurred while executing statement: ", statement, " error: ", err)
					return err
				}
			}
			err = DeleteMigration(conf.Keyspace, id, session)
			if err != nil {
				return err
			}
			fmt.Println("Applied down migration", id)

			return nil
		},
	}
	c.Commands = append(c.Commands, &command)
}

func GetLatestMigrationID(keyspace string, session *gocql.Session) (string, error) {
	migrations, err := GetExistingMigrations(keyspace, session)
	if err != nil {
		return "", err
	}
	if len(migrations) == 0 {
		return "", gocql.ErrNotFound
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].AppliedAt.After(migrations[j].AppliedAt)
	})

	return migrations[0].ID, nil
}

func DeleteMigration(keyspace string, id string, session *gocql.Session) error {
	query := fmt.Sprintf(`DELETE FROM "migrations".%s_migrations WHERE id = ?`, keyspace)
	return session.Query(query, id).Exec()
}
