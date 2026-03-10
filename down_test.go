package migrate

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsNewerMigration_PrefersLaterAppliedAt(t *testing.T) {
	older := Migration{
		ID:        "20260309103000-create-users.cql",
		AppliedAt: time.Date(2026, time.March, 9, 10, 30, 0, 0, time.UTC),
	}
	newer := Migration{
		ID:        "20260309100000-create-orders.cql",
		AppliedAt: older.AppliedAt.Add(time.Minute),
	}

	assert.True(t, IsNewerMigration(newer, older))
	assert.False(t, IsNewerMigration(older, newer))
}

func TestLatestMigrationOrdering_SortsTiedTimestampsByDescendingID(t *testing.T) {
	appliedAt := time.Date(2026, time.March, 9, 10, 30, 0, 0, time.UTC)
	migrations := []Migration{
		{ID: "20260309110000-add-column.cql", AppliedAt: appliedAt.Add(-time.Minute)}, // Older timestamp
		{ID: "20260309102959-create-table.cql", AppliedAt: appliedAt},                 // Same timestamp, lowest ID
		{ID: "20260309103000-add-index.cql", AppliedAt: appliedAt},                    // Same timestamp, middle ID
		{ID: "20260309103001-create-users.cql", AppliedAt: appliedAt},                 // Same timestamp, highest ID
	}

	sort.Slice(migrations, func(i, j int) bool {
		return IsNewerMigration(migrations[i], migrations[j])
	})

	assert.Equal(t, "20260309103001-create-users.cql", migrations[0].ID)
	assert.Equal(t, "20260309103000-add-index.cql", migrations[1].ID)
	assert.Equal(t, "20260309102959-create-table.cql", migrations[2].ID)
	assert.Equal(t, "20260309110000-add-column.cql", migrations[3].ID)
}

func TestApplyDown_InvalidGlobPattern(t *testing.T) {
	conf := Config{
		MigrationDir: "[",
		Connection: Connection{
			Port: DefaultConfigPort,
		},
	}

	_, err := ApplyDown(conf)
	require.Error(t, err)
	assert.True(t, errors.Is(err, filepath.ErrBadPattern))
}

func TestApplyDown_InvalidPort(t *testing.T) {
	conf := Config{
		MigrationDir: t.TempDir(),
		Connection: Connection{
			Port: "invalid",
		},
	}

	_, err := ApplyDown(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid syntax")
}
