package migrate

import (
	"bytes"
	"fmt"
	"github.com/blutspende/cassandra-migrate/sqlparse"
	"github.com/gocql/gocql"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// DownResult summarizes a single ApplyDown execution.
type DownResult struct {
	Applied     bool
	MigrationID string
}

// ApplyDown executes the Down statements for the latest applied migration.
func ApplyDown(conf Config) (DownResult, error) {
	migrationFiles, err := filepath.Glob(filepath.Join(conf.MigrationDir, "*.cql"))
	if err != nil {
		return DownResult{}, err
	}
	port, err := strconv.Atoi(conf.Connection.Port)
	if err != nil {
		return DownResult{}, err
	}
	session, err := GetConnection(conf.Connection.Hosts, port, conf.Keyspace, conf.Connection.Username, conf.Connection.Password)
	if err != nil {
		return DownResult{}, err
	}
	defer session.Close()
	id, err := GetLatestMigrationID(conf.Keyspace, session)
	if err == gocql.ErrNotFound {
		return DownResult{Applied: false}, nil
	}
	if err != nil {
		return DownResult{}, err
	}
	var filename string
	for i := range migrationFiles {
		if filepath.Base(migrationFiles[i]) == id {
			filename = migrationFiles[i]
			break
		}
	}
	if filename == "" {
		return DownResult{}, fmt.Errorf("migration file %s not found in %s", id, conf.MigrationDir)
	}
	content, err := os.ReadFile(filename)
	if err != nil {
		return DownResult{}, err
	}
	migration, err := sqlparse.ParseMigration(bytes.NewReader(content))
	if err != nil {
		return DownResult{}, err
	}
	for _, statement := range migration.DownStatements {
		err = session.Query(statement).Exec()
		if err != nil {
			if conf.IgnoreExistErrors && IsExistError(err) {
				continue
			}
			return DownResult{}, fmt.Errorf("failed to execute down statement in %s: %w", filepath.Base(filename), err)
		}
	}
	err = DeleteMigration(conf.Keyspace, id, session)
	if err != nil {
		return DownResult{}, err
	}

	return DownResult{Applied: true, MigrationID: id}, nil
}

// GetLatestMigrationID returns the newest applied migration ID by applied_at.
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

// DeleteMigration removes a migration ID from the tracking table.
func DeleteMigration(keyspace string, id string, session *gocql.Session) error {
	query := fmt.Sprintf(`DELETE FROM "%s_migrations" WHERE id = ?`, keyspace)
	return session.Query(query, id).Exec()
}
