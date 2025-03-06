package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blutspende/cassandra-migrate/sqlparse"
	"github.com/gocql/gocql"
	"github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"strconv"
)

func UpCommand(c *cli.App) {
	command := cli.Command{
		Name:        "up",
		Description: "Migrate to the most recent version",
		Usage:       "cassandra-migrate up",
		Flags:       append([]cli.Flag{
			// Add command-specific flags here
		}, CommonFlags...),
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
			err = session.Query(fmt.Sprintf(createMigrationsKeyspaceQueryTemplate, conf.Replication.Factor)).Exec()
			if err != nil {
				return err
			}
			err = session.Query(fmt.Sprintf(createMigrationsTableQueryTemplate, conf.Keyspace)).Exec()
			if err != nil {
				return err
			}
			existingMigrationIDs, err := GetExistingMigrationIDs(conf.Keyspace, session)
			if err != nil {
				return err
			}
			if len(existingMigrationIDs) > len(migrationFiles) {
				migrationFilenamesMap := make(map[string]any)
				for _, file := range migrationFiles {
					migrationFilenamesMap[filepath.Base(file)] = nil
				}
				for id := range existingMigrationIDs {
					if _, ok := migrationFilenamesMap[id]; !ok {
						return errors.New("unknown migration in database: " + id)
					}
				}
			}
			appliedMigrationIDs := make([]string, 0)
			var execErr error
			newMigrationFiles := make([]string, 0)

			for _, file := range migrationFiles {
				if _, ok := existingMigrationIDs[filepath.Base(file)]; ok {
					continue
				}
				newMigrationFiles = append(newMigrationFiles, file)
			}
			for _, file := range newMigrationFiles {
				content, err := os.ReadFile(file)
				if err != nil {
					return err
				}
				migration, err := sqlparse.ParseMigration(bytes.NewReader(content))
				if err != nil {
					return err
				}
				for _, statement := range migration.UpStatements {
					err := session.Query(ReplaceVarsInStatement(statement, conf)).Exec()
					if err != nil {
						if conf.IgnoreExistErrors && IsExistError(err) {
							fmt.Println("WRN: error occurred while executing statement: ", statement, " error: ", err.Error(), " (ignored due to 'ignore' flag)")
							continue
						}
						fmt.Println("ERR: error occurred while executing statement: ", statement, " error: ", err.Error())
						execErr = err
						break
					}
				}
				if execErr != nil {
					break
				}
				appliedMigrationIDs = append(appliedMigrationIDs, filepath.Base(file))

			}
			batch := session.NewBatch(gocql.LoggedBatch)
			for _, id := range appliedMigrationIDs {
				batch.Query(fmt.Sprintf(insertMigrationQueryTemplate, conf.Keyspace), id)
			}
			if err := session.ExecuteBatch(batch); err != nil {
				return err
			}
			fmt.Println(fmt.Sprintf("Applied %d of %d migrations", len(appliedMigrationIDs), len(newMigrationFiles)))
			return execErr
		},
	}
	c.Commands = append(c.Commands, &command)
}

const (
	createMigrationsKeyspaceQueryTemplate = `CREATE KEYSPACE IF NOT EXISTS "migrations" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': %s};`
	createMigrationsTableQueryTemplate    = `CREATE TABLE IF NOT EXISTS migrations."%s_migrations"  (id TEXT, applied_at TIMESTAMP, PRIMARY KEY(id));`
	insertMigrationQueryTemplate          = `INSERT INTO "migrations"."%s_migrations" (id, applied_at) VALUES (?, toTimestamp(now()));`
)

func GetExistingMigrationIDs(keyspace string, session *gocql.Session) (map[string]any, error) {
	existingMigrations, err := GetExistingMigrations(keyspace, session)
	if err != nil {
		return nil, err
	}
	existingMigrationIDs := make(map[string]any)
	for _, migration := range existingMigrations {
		existingMigrationIDs[migration.ID] = nil
	}

	return existingMigrationIDs, nil
}
