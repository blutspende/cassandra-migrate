# cassandra-migrate

Internal Go library and CLI for Cassandra schema migrations, using a `sql-migrate`-style migration file format.

## What This Repository Provides

- Importable Go library package: `github.com/blutspende/cassandra-migrate` (package name: `migrate`)
- CLI binary source: `./cmd/cassandra-migrate`
- CQL migration parser in `./sqlparse`

## Library-First Usage

### Install in Another Internal Project

```bash
go get github.com/blutspende/cassandra-migrate@<tag>
```

### Example: Apply Migrations from Go Code

```go
package main

import (
	"log"

	migrate "github.com/blutspende/cassandra-migrate"
)

func main() {
	conf, err := migrate.GetConfigFrom("cassandraconfig.yaml", "development", false)
	if err != nil {
		log.Fatal(err)
	}

	result, err := migrate.ApplyUp(conf)
	if err != nil {
		log.Fatalf("apply up failed after %d migrations: %v", result.AppliedCount, err)
	}
	log.Printf("applied %d/%d", result.AppliedCount, result.PendingCount)
}
```

### Example: Roll Back One Migration

```go
result, err := migrate.ApplyDown(conf)
if err != nil {
	log.Fatal(err)
}
if !result.Applied {
	log.Println("no migrations to apply")
}
```

### Example: Create Migration File Programmatically

```go
path, err := migrate.CreateMigration(conf, "create users table")
if err != nil {
	log.Fatal(err)
}
log.Println("created:", path)
```

## Public API Surface

- `DefaultOptions() Options`
- `GetConfig() (Config, error)`
- `GetConfigFrom(configFile, environment string, ignoreExistErrors bool) (Config, error)`
- `ReadConfigFile(configFile string) (map[string]Config, error)`
- `CreateMigration(conf Config, name string) (string, error)`
- `GenerateFileName(filename string, at time.Time) string`
- `ApplyUp(conf Config) (UpResult, error)`
- `ApplyDown(conf Config) (DownResult, error)`

## CLI Usage

Install:

```bash
go install github.com/blutspende/cassandra-migrate/cmd/cassandra-migrate@<tag>
```

If you are working from a local checkout of this repository:

```bash
go install ./cmd/cassandra-migrate
```

Run:

```bash
cassandra-migrate --help
```

Commands:

- `cassandra-migrate new <name>`
- `cassandra-migrate up`
- `cassandra-migrate down`

Shared flags:

- `--config` (default: `cassandraconfig.yaml`)
- `--env` (default: `development`)
- `--ignore`, `-i` (ignore already-exists type errors during statement execution)

Example:

```bash
cassandra-migrate up --config cassandraconfig.yaml --env development
```

## Configuration

Default config file name: `cassandraconfig.yaml`  
Default environment: `development`

Example:

```yaml
development:
  keyspace: myapp
  migration_dir: migrations
  connection:
    hosts:
      - 127.0.0.1
    port: "9042"
    username: "${CASSANDRA_USERNAME}"
    password: "${CASSANDRA_PASSWORD}"
```

Fields:

- `keyspace` (required, alphanumeric only)
- `migration_dir` (default: `migrations`)
- `connection.hosts` (required, at least one non-empty host)
- `connection.port` (default: `9042`)
- `connection.username` (default: `cassandra`)
- `connection.password` (default: `cassandra`)

All config string values are passed through `os.ExpandEnv`, so `${VAR}` placeholders are supported.

## Migration File Format

Generated template:

```sql
-- +migrate Up

-- +migrate Down
```

Example:

```sql
-- +migrate Up
CREATE TABLE IF NOT EXISTS myapp.users (
  user_id UUID PRIMARY KEY,
  email text
);

-- +migrate Down
DROP TABLE IF EXISTS myapp.users;
```

Supported directives:

- `-- +migrate Up`
- `-- +migrate Down`

## Runtime Behavior

- Migration files are read from `migration_dir` with `*.cql` pattern in lexicographic order.
- The configured `keyspace` must already exist before running migrations.
- The CLI connects to the configured `keyspace` and executes migrations there.
- Applied migrations are tracked in the `"<keyspace>_migrations"` table inside that keyspace.
- `ApplyDown` rolls back the latest applied migration by `applied_at`.
- If database migration IDs exist that are missing locally, `ApplyUp` fails.
