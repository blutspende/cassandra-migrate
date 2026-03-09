package sqlparse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	sqlCmdPrefix = "-- +migrate "
)

type ParsedMigration struct {
	UpStatements   []string
	DownStatements []string
}

// LineSeparator can be used to split migrations by an exact line match. This line
// will be removed from the output. If left blank, it is not considered. It is defaulted
// to blank so you will have to set it manually.
// Use case: in MSSQL, it is convenient to separate commands by GO statements like in
// SQL Query Analyzer.
var LineSeparator = ""

func errNoTerminator() error {
	if len(LineSeparator) == 0 {
		return fmt.Errorf(`ERROR: The last statement must be ended by a semicolon.
			See https://github.com/blutspende/cassandra-migrate for details.`)
	}

	return fmt.Errorf(`ERROR: The last statement must be ended by a semicolon or a line whose contents are %q.
			See https://github.com/blutspende/cassandra-migrate for details.`, LineSeparator)
}

// Checks the line to see if the line has a statement-ending semicolon
// or if the line contains a double-dash comment.
func endsWithSemicolon(line string) bool {
	prev := ""
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		word := scanner.Text()
		if strings.HasPrefix(word, "--") {
			break
		}
		prev = word
	}

	return strings.HasSuffix(prev, ";")
}

type migrationDirection int

const (
	directionNone migrationDirection = iota
	directionUp
	directionDown
)

type migrateCommand struct {
	Command string
}

func parseCommand(line string) (*migrateCommand, error) {
	cmd := &migrateCommand{}

	if !strings.HasPrefix(line, sqlCmdPrefix) {
		return nil, fmt.Errorf("ERROR: not a migration command")
	}

	fields := strings.Fields(line[len(sqlCmdPrefix):])
	if len(fields) == 0 {
		return nil, fmt.Errorf(`ERROR: incomplete migration command`)
	}

	cmd.Command = fields[0]

	return cmd, nil
}

// Split the given sql script into individual statements.
//
// The base case is to simply split on semicolons, as these
// naturally terminate a statement.
func ParseMigration(r io.ReadSeeker) (*ParsedMigration, error) {
	p := &ParsedMigration{}

	_, err := r.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	currentDirection := directionNone

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		// ignore comment except beginning with '-- +'
		if strings.HasPrefix(line, "-- ") && !strings.HasPrefix(line, "-- +") {
			continue
		}

		// handle any migrate-specific commands
		if strings.HasPrefix(line, sqlCmdPrefix) {
			cmd, err := parseCommand(line)
			if err != nil {
				return nil, err
			}

			switch cmd.Command {
			case "Up":
				if len(strings.TrimSpace(buf.String())) > 0 {
					return nil, errNoTerminator()
				}
				currentDirection = directionUp

			case "Down":
				if len(strings.TrimSpace(buf.String())) > 0 {
					return nil, errNoTerminator()
				}
				currentDirection = directionDown

			default:
				return nil, fmt.Errorf(`ERROR: unsupported migration command %q.
			Only '-- +migrate Up' and '-- +migrate Down' are supported.
			See https://github.com/blutspende/cassandra-migrate for details.`, cmd.Command)
			}

			continue
		}

		if currentDirection == directionNone {
			continue
		}

		isLineSeparator := len(LineSeparator) > 0 && line == LineSeparator

		if !isLineSeparator {
			if _, err := buf.WriteString(line + "\n"); err != nil {
				return nil, err
			}
		}

		if endsWithSemicolon(line) || isLineSeparator {
			switch currentDirection {
			case directionUp:
				p.UpStatements = append(p.UpStatements, buf.String())

			case directionDown:
				p.DownStatements = append(p.DownStatements, buf.String())

			default:
				panic("impossible state")
			}

			buf.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if currentDirection == directionNone {
		return nil, fmt.Errorf(`ERROR: no Up/Down annotations found, so no statements were executed.
			See https://github.com/blutspende/cassandra-migrate for details.`)
	}

	// allow comment without sql instruction. Example:
	// -- +migrate Down
	// -- nothing to downgrade!
	if len(strings.TrimSpace(buf.String())) > 0 && !strings.HasPrefix(buf.String(), "-- +") {
		return nil, errNoTerminator()
	}

	return p, nil
}
