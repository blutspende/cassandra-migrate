package migrate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigFrom_AppliesDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("TEST_KEYSPACE", "bloodlab")
	t.Setenv("TEST_HOST_1", "127.0.0.1")
	t.Setenv("TEST_USER", "team_user")
	t.Setenv("TEST_PASS", "team_pass")

	configFile := writeConfigFile(t, `
development:
  keyspace: ${TEST_KEYSPACE}
  migration_dir: ""
  connection:
    hosts:
      - "  ${TEST_HOST_1} "
      - ""
      - "   "
    port: ""
    username: ${TEST_USER}
    password: ${TEST_PASS}
`)

	conf, err := GetConfigFrom(configFile, "development", true)
	require.NoError(t, err)

	assert.Equal(t, "bloodlab", conf.Keyspace)
	assert.Equal(t, []string{"127.0.0.1"}, conf.Connection.Hosts)
	assert.Equal(t, DefaultConfigPort, conf.Connection.Port)
	assert.Equal(t, "team_user", conf.Connection.Username)
	assert.Equal(t, "team_pass", conf.Connection.Password)
	assert.Equal(t, DefaultConfigMigrationDir, conf.MigrationDir)
	assert.True(t, conf.IgnoreExistErrors)
}

func TestGetConfigFrom_MissingEnvironment(t *testing.T) {
	configFile := writeConfigFile(t, `
development:
  keyspace: test
  connection:
    hosts:
      - 127.0.0.1
`)

	_, err := GetConfigFrom(configFile, "staging", false)
	require.Error(t, err)
	assert.Equal(t, "no environment: staging", err.Error())
}

func TestGetConfigFrom_MissingHosts(t *testing.T) {
	configFile := writeConfigFile(t, `
development:
  keyspace: test
  connection:
    hosts:
      - ""
`)

	_, err := GetConfigFrom(configFile, "development", false)
	require.Error(t, err)
	assert.Equal(t, "at least one host is required", err.Error())
}

func TestGetConfigFrom_InvalidKeyspace(t *testing.T) {
	configFile := writeConfigFile(t, `
development:
  keyspace: my_keyspace
  connection:
    hosts:
      - 127.0.0.1
`)

	_, err := GetConfigFrom(configFile, "development", false)
	require.Error(t, err)
	assert.Equal(t, "keyspace contains special characters", err.Error())
}

func TestGetConfig_DefaultOptionsFileMissing(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	tempDir := t.TempDir()
	require.NoError(t, os.Chdir(tempDir))
	t.Cleanup(func() {
		_ = os.Chdir(wd)
	})

	_, err = GetConfig()
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "cassandraconfig.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(content), 0o644))
	return configFile
}
