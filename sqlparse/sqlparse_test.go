package sqlparse

import (
	"strings"
	"testing"

	//revive:disable-next-line:dot-imports
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type SqlParseSuite struct{}

var _ = Suite(&SqlParseSuite{})

func (*SqlParseSuite) TestSemicolons(c *C) {
	type testData struct {
		line   string
		result bool
	}

	tests := []testData{
		{
			line:   "END;",
			result: true,
		},
		{
			line:   "END; -- comment",
			result: true,
		},
		{
			line:   "END   ; -- comment",
			result: true,
		},
		{
			line:   "END -- comment",
			result: false,
		},
		{
			line:   "END -- comment ;",
			result: false,
		},
		{
			line:   "END \" ; \" -- comment",
			result: false,
		},
	}

	for _, test := range tests {
		r := endsWithSemicolon(test.line)
		c.Assert(r, Equals, test.result)
	}
}

func (*SqlParseSuite) TestSplitStatements(c *C) {
	type testData struct {
		sql       string
		upCount   int
		downCount int
	}

	tests := []testData{
		{
			sql:       functxt,
			upCount:   2,
			downCount: 2,
		},
		{
			sql:       multitxt,
			upCount:   2,
			downCount: 2,
		},
	}

	for _, test := range tests {
		migration, err := ParseMigration(strings.NewReader(test.sql))
		c.Assert(err, IsNil)
		c.Assert(migration.UpStatements, HasLen, test.upCount)
		c.Assert(migration.DownStatements, HasLen, test.downCount)
	}
}

func (*SqlParseSuite) TestIntentionallyBadStatements(c *C) {
	for _, test := range intentionallyBad {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, NotNil)
	}
}

func (*SqlParseSuite) TestJustComment(c *C) {
	for _, test := range justAComment {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, NotNil)
	}
}

func (*SqlParseSuite) TestCustomTerminator(c *C) {
	LineSeparator = "GO"
	defer func() { LineSeparator = "" }()

	type testData struct {
		sql       string
		upCount   int
		downCount int
	}

	tests := []testData{
		{
			sql:       functxtSplitByGO,
			upCount:   2,
			downCount: 2,
		},
		{
			sql:       multitxtSplitByGO,
			upCount:   2,
			downCount: 2,
		},
	}

	for _, test := range tests {
		migration, err := ParseMigration(strings.NewReader(test.sql))
		c.Assert(err, IsNil)
		c.Assert(migration.UpStatements, HasLen, test.upCount)
		c.Assert(migration.DownStatements, HasLen, test.downCount)
	}
}

func (*SqlParseSuite) TestTerminatedCQLStatement(c *C) {
	for _, test := range validCQLStatements {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, IsNil)
	}
}

func (*SqlParseSuite) TestComplexCQLStatement(c *C) {
	for _, test := range cqlStatementsWithVars {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, IsNil)
	}
}

func (*SqlParseSuite) TestUnterminatedCQLStatement(c *C) {
	for _, test := range invalidCQLStatement {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, NotNil)
	}
}

func (*SqlParseSuite) TestCQLStatementWithVars(c *C) {
	for _, test := range cqlStatementsWithVars {
		_, err := ParseMigration(strings.NewReader(test))
		c.Assert(err, IsNil)
	}
}

var functxt = `-- +migrate Up
CREATE TABLE IF NOT EXISTS histories (
  id                BIGSERIAL  PRIMARY KEY,
  current_value     varchar(2000) NOT NULL,
  created_at      timestamp with time zone  NOT NULL
);

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION histories_partition_creation( DATE, DATE )
returns void AS $$
DECLARE
  create_query text;
BEGIN
  FOR create_query IN SELECT
      'CREATE TABLE IF NOT EXISTS histories_'
      || TO_CHAR( d, 'YYYY_MM' )
      || ' ( CHECK( created_at >= timestamp '''
      || TO_CHAR( d, 'YYYY-MM-DD 00:00:00' )
      || ''' AND created_at < timestamp '''
      || TO_CHAR( d + INTERVAL '1 month', 'YYYY-MM-DD 00:00:00' )
      || ''' ) ) inherits ( histories );'
    FROM generate_series( $1, $2, '1 month' ) AS d
  LOOP
    EXECUTE create_query;
  END LOOP;  -- LOOP END
END;         -- FUNCTION END
$$
language plpgsql;
-- +migrate StatementEnd

-- +migrate Down
drop function histories_partition_creation(DATE, DATE);
drop TABLE histories;
`

// test multiple up/down transitions in a single script
var multitxt = `-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE post;

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE fancier_post;
`

// raise error when statements are not explicitly ended
var intentionallyBad = []string{
	// first statement missing terminator
	`-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
)

-- +migrate Down
DROP TABLE post;

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE fancier_post;
`,

	// second half of first statement missing terminator
	`-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);

SELECT 'No ending semicolon'

-- +migrate Down
DROP TABLE post;

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE fancier_post;
`,

	// second statement missing terminator
	`-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE post

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE fancier_post;
`,

	// trailing text after explicit StatementEnd
	`-- +migrate Up
-- +migrate StatementBegin
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);
-- +migrate StatementBegin
SELECT 'no semicolon'

-- +migrate Down
DROP TABLE post;

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
);

-- +migrate Down
DROP TABLE fancier_post;
`,
}

// Same as functxt above but split by GO lines
var functxtSplitByGO = `-- +migrate Up
CREATE TABLE IF NOT EXISTS histories (
  id                BIGSERIAL  PRIMARY KEY,
  current_value     varchar(2000) NOT NULL,
  created_at      timestamp with time zone  NOT NULL
)
GO

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION histories_partition_creation( DATE, DATE )
returns void AS $$
DECLARE
  create_query text;
BEGIN
  FOR create_query IN SELECT
      'CREATE TABLE IF NOT EXISTS histories_'
      || TO_CHAR( d, 'YYYY_MM' )
      || ' ( CHECK( created_at >= timestamp '''
      || TO_CHAR( d, 'YYYY-MM-DD 00:00:00' )
      || ''' AND created_at < timestamp '''
      || TO_CHAR( d + INTERVAL '1 month', 'YYYY-MM-DD 00:00:00' )
      || ''' ) ) inherits ( histories );'
    FROM generate_series( $1, $2, '1 month' ) AS d
  LOOP
    EXECUTE create_query;
  END LOOP;  -- LOOP END
END;         -- FUNCTION END
$$
GO
/* while GO wouldn't be used in a statement like this, I'm including it for the test */
language plpgsql
-- +migrate StatementEnd

-- +migrate Down
drop function histories_partition_creation(DATE, DATE)
GO
drop TABLE histories
GO
`

// test multiple up/down transitions in a single script, split by GO lines
var multitxtSplitByGO = `-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
)
GO

-- +migrate Down
DROP TABLE post
GO

-- +migrate Up
CREATE TABLE fancier_post (
    id int NOT NULL,
    title text,
    body text,
    created_on timestamp without time zone,
    PRIMARY KEY(id)
)
GO

-- +migrate Down
DROP TABLE fancier_post
GO
`

// test a comment without sql instruction
var justAComment = []string{
	`-- +migrate Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
)

-- +migrate Down
-- no migration here
`,
}

var validCQLStatements = []string{
	`-- +migrate Up
 CREATE TABLE keyspace.post (
  id int PRIMARY KEY,
  title text,
  body text
 ) WITH CLUSTERING ORDER BY (title ASC);
 -- +migrate Down
 DROP TABLE keyspace.post;
 `,
	`-- +migrate Up
 INSERT INTO keyspace.post (id, title, body) VALUES (1, 'Title', 'Body');
 -- +migrate Down
 DELETE FROM keyspace.post WHERE id = 1;
 `,
}

var invalidCQLStatement = []string{
	`-- +migrate Up
 CREATE TABLE keyspace.post (
  id int PRIMARY KEY,
  title text,
  body text
 ) WITH CLUSTERING ORDER BY (title ASC)
 -- +migrate Down
 DROP TABLE keyspace.post;
 `,
	`-- +migrate Up
 INSERT INTO keyspace.post (id, title, body) VALUES (1, 'Title', 'Body')
 -- +migrate Down
 DELETE FROM keyspace.post WHERE id = 1;
 `,
}

var cqlStatementsWithVars = []string{
	`-- +migrate Up
CREATE TABLE IF NOT EXISTS keyspace.users (
    user_id UUID PRIMARY KEY,
    first_name text,
    last_name text,
    email text,
    created_at timestamp,
    last_login timestamp,
    profile_pic blob,
    address frozen<address>,
    preferences map<text, text>,
    friends set<UUID>,
    posts list<UUID>
) WITH CLUSTERING ORDER BY (last_name ASC, first_name ASC)
AND replication = {'class': ${Strategy}, 'replication_factor': ${Factor}}
AND compaction = {'class': 'LeveledCompactionStrategy'}
AND compression = {'sstable_compression': 'LZ4Compressor'}
AND default_time_to_live = 86400
AND gc_grace_seconds = 3600;

-- +migrate Down
DROP TABLE IF EXISTS keyspace.users;
DROP TYPE IF EXISTS keyspace.address;`,
	`-- +migrate Up
CREATE TABLE bucket_test (
  bucket text,
  due_at timestamp,
  event_id uuid,
  PRIMARY KEY ((bucket), due_at, event_id)
) WITH CLUSTERING ORDER BY (due_at ASC);`,
}
