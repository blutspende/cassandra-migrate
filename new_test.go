package migrate

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMigration_CreatesTimestampedFileWithTemplate(t *testing.T) {
	tempDir := t.TempDir()
	conf := Config{
		MigrationDir: tempDir,
	}

	filePath, err := CreateMigration(conf, "create users table")
	require.NoError(t, err)

	assert.Regexp(t, regexp.MustCompile(`^\d{14}-create-users-table\.cql$`), filepath.Base(filePath))

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, templateContent, string(content))
}

func TestCreateMigration_MissingName(t *testing.T) {
	tempDir := t.TempDir()
	conf := Config{
		MigrationDir: tempDir,
	}

	_, err := CreateMigration(conf, "   ")
	require.Error(t, err)
	assert.Equal(t, "missing migration name", err.Error())
}

func TestCreateMigration_MissingDirectory(t *testing.T) {
	conf := Config{
		MigrationDir: filepath.Join(t.TempDir(), "missing"),
	}

	_, err := CreateMigration(conf, "test")
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestGenerateFileName(t *testing.T) {
	at := time.Date(2026, time.March, 6, 13, 5, 42, 0, time.UTC)
	got := GenerateFileName(" create_user@table ", at)
	assert.Equal(t, "20260306130542-create-user-table.cql", got)
}
