package migrate

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryCall struct {
	statement string
	args      []any
}

type requestErrorStub struct {
	code    int
	message string
}

func (e requestErrorStub) Code() int {
	return e.code
}

func (e requestErrorStub) Message() string {
	return e.message
}

func (e requestErrorStub) Error() string {
	return e.message
}

func TestApplyUp_InvalidGlobPattern(t *testing.T) {
	conf := Config{
		MigrationDir: "[",
		Connection: Connection{
			Port: DefaultConfigPort,
		},
	}

	_, err := ApplyUp(conf)
	require.Error(t, err)
	assert.True(t, errors.Is(err, filepath.ErrBadPattern))
}

func TestApplyUp_InvalidPort(t *testing.T) {
	conf := Config{
		MigrationDir: t.TempDir(),
		Connection: Connection{
			Port: "invalid",
		},
	}

	_, err := ApplyUp(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid syntax")
}

func TestCreateMigrationsTableQueryTemplate(t *testing.T) {
	query := fmt.Sprintf(createMigrationsTableQueryTemplate, "bloodlab")

	assert.Equal(t, `CREATE TABLE IF NOT EXISTS "bloodlab_migrations" (id TEXT, applied_at TIMESTAMP, PRIMARY KEY(id));`, query)
}

func TestInsertMigrationQueryTemplate(t *testing.T) {
	query := fmt.Sprintf(insertMigrationQueryTemplate, "bloodlab")

	assert.Equal(t, `INSERT INTO "bloodlab_migrations" (id, applied_at) VALUES (?, toTimestamp(now()));`, query)
}

func TestApplyAndRecordMigration_RecordsImmediatelyAfterFileStatements(t *testing.T) {
	calls := make([]queryCall, 0)
	err := applyAndRecordMigration(
		"bloodlab",
		filepath.Join("migrations", "20260422123000-create-users.cql"),
		[]string{
			"CREATE TABLE users (id uuid PRIMARY KEY);",
			"CREATE INDEX users_id_idx ON users (id);",
		},
		false,
		func(statement string, args ...any) error {
			calls = append(calls, queryCall{statement: statement, args: args})
			return nil
		},
	)
	require.NoError(t, err)

	require.Len(t, calls, 3)
	assert.Equal(t, "CREATE TABLE users (id uuid PRIMARY KEY);", calls[0].statement)
	assert.Empty(t, calls[0].args)
	assert.Equal(t, "CREATE INDEX users_id_idx ON users (id);", calls[1].statement)
	assert.Empty(t, calls[1].args)
	assert.Equal(t, fmt.Sprintf(insertMigrationQueryTemplate, "bloodlab"), calls[2].statement)
	assert.Equal(t, []any{"20260422123000-create-users.cql"}, calls[2].args)
}

func TestApplyAndRecordMigration_DoesNotRecordWhenStatementFails(t *testing.T) {
	expectedErr := errors.New("statement failed")
	calls := make([]queryCall, 0)
	err := applyAndRecordMigration(
		"bloodlab",
		filepath.Join("migrations", "20260422123000-create-users.cql"),
		[]string{
			"CREATE TABLE users (id uuid PRIMARY KEY);",
			"CREATE INDEX users_id_idx ON users (id);",
		},
		false,
		func(statement string, args ...any) error {
			calls = append(calls, queryCall{statement: statement, args: args})
			if statement == "CREATE INDEX users_id_idx ON users (id);" {
				return expectedErr
			}
			return nil
		},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, "failed to execute statement in 20260422123000-create-users.cql: statement failed", err.Error())

	require.Len(t, calls, 2)
	assert.Equal(t, "CREATE TABLE users (id uuid PRIMARY KEY);", calls[0].statement)
	assert.Equal(t, "CREATE INDEX users_id_idx ON users (id);", calls[1].statement)
}

func TestApplyAndRecordMigration_IgnoresAlreadyExistsAndStillRecords(t *testing.T) {
	calls := make([]queryCall, 0)
	err := applyAndRecordMigration(
		"bloodlab",
		filepath.Join("migrations", "20260422123000-create-users.cql"),
		[]string{
			"CREATE TABLE users (id uuid PRIMARY KEY);",
		},
		true,
		func(statement string, args ...any) error {
			calls = append(calls, queryCall{statement: statement, args: args})
			if statement == "CREATE TABLE users (id uuid PRIMARY KEY);" {
				return requestErrorStub{
					code:    gocql.ErrCodeAlreadyExists,
					message: "table already exists",
				}
			}
			return nil
		},
	)
	require.NoError(t, err)

	require.Len(t, calls, 2)
	assert.Equal(t, "CREATE TABLE users (id uuid PRIMARY KEY);", calls[0].statement)
	assert.Equal(t, fmt.Sprintf(insertMigrationQueryTemplate, "bloodlab"), calls[1].statement)
	assert.Equal(t, []any{"20260422123000-create-users.cql"}, calls[1].args)
}
