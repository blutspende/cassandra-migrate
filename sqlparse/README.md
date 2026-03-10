# CQL migration parser

This package contains the migration parser used by `github.com/blutspende/cassandra-migrate`.
It is adapted from the MIT-licensed parser lineage used by `github.com/rubenv/sql-migrate`
and `goose`, but trimmed for Cassandra use.

Supported migration commands:

- `-- +migrate Up`
- `-- +migrate Down`

Statements are split on semicolons by default. If `LineSeparator` is set, a line
whose contents exactly match that separator is also treated as a statement boundary.

## License

This library is distributed under the [MIT](LICENSE) license.
