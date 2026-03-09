package migrate

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
