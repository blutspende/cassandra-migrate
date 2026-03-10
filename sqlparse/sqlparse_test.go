package sqlparse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndsWithSemicolon(t *testing.T) {
	tests := []struct {
		line   string
		result bool
	}{
		{line: "END;", result: true},
		{line: "END; -- comment", result: true},
		{line: "END   ; -- comment", result: true},
		{line: "END -- comment", result: false},
		{line: "END -- comment ;", result: false},
		{line: `END " ; " -- comment`, result: false},
	}

	for _, test := range tests {
		assert.Equal(t, test.result, endsWithSemicolon(test.line), test.line)
	}
}

func TestParseMigration_SplitsStatements(t *testing.T) {
	migration, err := ParseMigration(strings.NewReader(`-- +migrate Up
CREATE TABLE keyspace.post (
  id int PRIMARY KEY,
  title text
);
INSERT INTO keyspace.post (id, title) VALUES (1, 'Title');

-- +migrate Down
DELETE FROM keyspace.post WHERE id = 1;
DROP TABLE keyspace.post;
`))
	require.NoError(t, err)

	assert.Len(t, migration.UpStatements, 2)
	assert.Len(t, migration.DownStatements, 2)
}

func TestParseMigration_SplitsStatementsByLineSeparator(t *testing.T) {
	LineSeparator = "GO"
	defer func() { LineSeparator = "" }()

	migration, err := ParseMigration(strings.NewReader(`-- +migrate Up
CREATE TABLE keyspace.post (
  id int PRIMARY KEY,
  title text
)
GO
INSERT INTO keyspace.post (id, title) VALUES (1, 'Title')
GO

-- +migrate Down
DELETE FROM keyspace.post WHERE id = 1
GO
DROP TABLE keyspace.post
GO
`))
	require.NoError(t, err)

	assert.Len(t, migration.UpStatements, 2)
	assert.Len(t, migration.DownStatements, 2)
}

func TestParseMigration_RejectsUnsupportedCommand(t *testing.T) {
	_, err := ParseMigration(strings.NewReader(`-- +migrate Up
CREATE TABLE keyspace.post (id int PRIMARY KEY);
-- +migrate StatementBegin
SELECT now();
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported migration command "StatementBegin"`)
	assert.Contains(t, err.Error(), "https://github.com/blutspende/cassandra-migrate")
}

func TestParseMigration_RejectsMissingTerminator(t *testing.T) {
	_, err := ParseMigration(strings.NewReader(`-- +migrate Up
CREATE TABLE keyspace.post (
  id int PRIMARY KEY,
  title text
)

-- +migrate Down
DROP TABLE keyspace.post;
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be ended by a semicolon")
	assert.Contains(t, err.Error(), "https://github.com/blutspende/cassandra-migrate")
}

func TestParseMigration_RejectsMissingAnnotations(t *testing.T) {
	_, err := ParseMigration(strings.NewReader(`CREATE TABLE keyspace.post (id int PRIMARY KEY);`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Up/Down annotations found")
	assert.Contains(t, err.Error(), "https://github.com/blutspende/cassandra-migrate")
}

func TestParseMigration_AllowsVariablesInCQL(t *testing.T) {
	migration, err := ParseMigration(strings.NewReader(`-- +migrate Up
CREATE TABLE IF NOT EXISTS keyspace.users (
    user_id UUID PRIMARY KEY,
    first_name text,
    last_name text
) WITH CLUSTERING ORDER BY (last_name ASC)
AND replication = {'class': ${Strategy}, 'replication_factor': ${Factor}};

-- +migrate Down
DROP TABLE IF EXISTS keyspace.users;
`))
	require.NoError(t, err)

	assert.Len(t, migration.UpStatements, 1)
	assert.Len(t, migration.DownStatements, 1)
}
