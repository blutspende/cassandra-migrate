package migrate

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"time"
)

// GetConnection creates a Cassandra session using password authentication.
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

// IsExistError reports whether the given error is a Cassandra "already exists" error.
func IsExistError(err error) bool {
	if reflect.TypeOf(err).String() == "gocql.errorFrame" {
		code := reflect.ValueOf(err).FieldByName("code").Int()
		return code == 0x2400
	}
	var cqlErr *gocql.RequestErrAlreadyExists
	return errors.As(err, &cqlErr)
}

// Migration represents one applied migration row from the tracking table.
type Migration struct {
	ID        string
	AppliedAt time.Time
}

// GetExistingMigrations returns all applied migrations for a keyspace.
func GetExistingMigrations(keyspace string, session *gocql.Session) ([]Migration, error) {
	query := fmt.Sprintf(`SELECT id, applied_at FROM "%s_migrations";`, keyspace)
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
