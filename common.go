package main

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/urfave/cli/v2"
	"reflect"
	"strings"
	"time"
)

var CommonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "config",
		Usage:       fmt.Sprintf("name of the config file (default: %s)", DefaultConfigFile),
		Required:    false,
		Value:       DefaultConfigFile,
		Destination: &ConfigFile,
	},
	&cli.StringFlag{
		Name:        "env",
		Usage:       fmt.Sprintf("environment to use (default: %s)", DefaultConfigEnvironment),
		Required:    false,
		Value:       DefaultConfigEnvironment,
		Destination: &ConfigEnvironment,
	},
	&cli.BoolFlag{
		Name:        "ignore",
		Usage:       "ignore already exists and does not exist errors in migrations",
		Required:    false,
		Value:       false,
		Destination: &IgnoreExistErrors,
		Aliases:     []string{"i"},
	},
}

func GetConnection(hosts []string, port int, keyspace, username, password string) (*gocql.Session, error) {
	if username == DefaultConfigUsername && password == DefaultConfigPassword {
		println("warning, using default credentials")
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}

	return cluster.CreateSession()
}

func IsExistError(err error) bool {
	if reflect.TypeOf(err).String() == "gocql.errorFrame" {
		code := reflect.ValueOf(err).FieldByName("code").Int()
		return code == 0x2400
	}
	var cqlErr *gocql.RequestErrAlreadyExists
	return errors.As(err, &cqlErr)
}

type Migration struct {
	ID        string
	AppliedAt time.Time
}

func GetExistingMigrations(keyspace string, session *gocql.Session) ([]Migration, error) {
	query := fmt.Sprintf(`SELECT id, applied_at FROM migrations."%s_migrations";`, keyspace)
	appliedMigrations := make([]Migration, 0)
	iter := session.Query(query).Iter()
	for {
		var migration Migration
		if !iter.Scan(&migration.ID, &migration.AppliedAt) {
			break
		}
		appliedMigrations = append(appliedMigrations, migration)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return appliedMigrations, nil
}

func ReplaceVarsInStatement(statement string, conf Config) string {
	newStatement := statement
	replicationStrategy := conf.Replication.Strategy
	if !strings.HasPrefix(replicationStrategy, "'") && !strings.HasSuffix(replicationStrategy, "'") {
		replicationStrategy = fmt.Sprintf("'%s'", replicationStrategy)
	}
	newStatement = strings.ReplaceAll(newStatement, replicationStrategyVar, replicationStrategy)
	newStatement = strings.ReplaceAll(newStatement, replicationFactorVar, fmt.Sprintf("%s", conf.Replication.Factor))
	return newStatement
}

const (
	replicationStrategyVar = "${ReplicationStrategy}"
	replicationFactorVar   = "${ReplicationFactor}"
)
