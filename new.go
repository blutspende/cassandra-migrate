package migrate

import (
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"text/template"
	"time"
)

var templateContent = `
-- +migrate Up

-- +migrate Down
`
var tpl = template.Must(template.New("new_migration").Parse(templateContent))

// CreateMigration creates a timestamped migration file in conf.MigrationDir.
func CreateMigration(conf Config, name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("missing migration name")
	}

	if _, err := os.Stat(conf.MigrationDir); os.IsNotExist(err) {
		return "", err
	}

	filePath := path.Join(conf.MigrationDir, generateFileName(name))
	f, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	if err := tpl.Execute(f, nil); err != nil {
		return "", err
	}

	return filePath, nil
}

var specialCharactersRegex = regexp.MustCompile("[^A-Za-z0-9]+")

func generateFileName(filename string) string {
	return GenerateFileName(filename, time.Now())
}

// GenerateFileName returns a sanitized migration filename for a given timestamp.
func GenerateFileName(filename string, at time.Time) string {
	name := specialCharactersRegex.ReplaceAllString(strings.TrimSpace(filename), "-")
	return fmt.Sprintf("%s-%s.cql", at.Format("20060102150405"), name)
}
