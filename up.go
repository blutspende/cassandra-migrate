package migrate

import (
	"bytes"
	"errors"
	"fmt"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/blutspende/cassandra-migrate/sqlparse"
	"os"
	"path/filepath"
	"strconv"
)

// UpResult summarizes a single ApplyUp execution.
type UpResult struct {
	AppliedCount        int
	PendingCount        int
	AppliedMigrationIDs []string
}

// ApplyUp executes all pending migration Up statements and records applied IDs.
func ApplyUp(conf Config) (UpResult, error) {
	migrationFiles, err := filepath.Glob(filepath.Join(conf.MigrationDir, "*.cql"))
	if err != nil {
		return UpResult{}, err
	}
	port, err := strconv.Atoi(conf.Connection.Port)
	if err != nil {
		return UpResult{}, err
	}
	session, err := GetConnection(conf.Connection.Hosts, port, conf.Keyspace, conf.Connection.Username, conf.Connection.Password)
	if err != nil {
		return UpResult{}, err
	}
	defer session.Close()
	err = session.Query(fmt.Sprintf(createMigrationsTableQueryTemplate, conf.Keyspace)).Exec()
	if err != nil {
		return UpResult{}, err
	}
	existingMigrationIDs, err := GetExistingMigrationIDs(conf.Keyspace, session)
	if err != nil {
		return UpResult{}, err
	}
	if len(existingMigrationIDs) > len(migrationFiles) {
		migrationFilenamesMap := make(map[string]any)
		for _, file := range migrationFiles {
			migrationFilenamesMap[filepath.Base(file)] = nil
		}
		for id := range existingMigrationIDs {
			if _, ok := migrationFilenamesMap[id]; !ok {
				return UpResult{}, errors.New("unknown migration in database: " + id)
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
			return UpResult{}, err
		}
		migration, err := sqlparse.ParseMigration(bytes.NewReader(content))
		if err != nil {
			return UpResult{}, err
		}
		err = applyAndRecordMigration(
			conf.Keyspace,
			file,
			migration.UpStatements,
			conf.IgnoreExistErrors,
			func(statement string, args ...any) error {
				return session.Query(statement, args...).Exec()
			},
		)
		if err != nil {
			execErr = err
			break
		}
		appliedMigrationIDs = append(appliedMigrationIDs, filepath.Base(file))
	}

	result := UpResult{
		AppliedCount:        len(appliedMigrationIDs),
		PendingCount:        len(newMigrationFiles),
		AppliedMigrationIDs: appliedMigrationIDs,
	}
	return result, execErr
}

const (
	createMigrationsTableQueryTemplate = `CREATE TABLE IF NOT EXISTS "%s_migrations" (id TEXT, applied_at TIMESTAMP, PRIMARY KEY(id));`
	insertMigrationQueryTemplate       = `INSERT INTO "%s_migrations" (id, applied_at) VALUES (?, toTimestamp(now()));`
)

type queryExecutor func(statement string, args ...any) error

func applyAndRecordMigration(keyspace, file string, statements []string, ignoreExistErrors bool, execQuery queryExecutor) error {
	migrationID := filepath.Base(file)
	for _, statement := range statements {
		err := execQuery(statement)
		if err != nil {
			if ignoreExistErrors && IsExistError(err) {
				continue
			}
			return fmt.Errorf("failed to execute statement in %s: %w", migrationID, err)
		}
	}

	if err := execQuery(fmt.Sprintf(insertMigrationQueryTemplate, keyspace), migrationID); err != nil {
		return fmt.Errorf("failed to record migration %s: %w", migrationID, err)
	}

	return nil
}

// GetExistingMigrationIDs returns applied migration IDs as a set.
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
