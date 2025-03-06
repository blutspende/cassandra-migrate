package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReplaceVarsInStatement(t *testing.T) {
	statement := "CREATE TABLE IF NOT EXISTS test.test (id INT PRIMARY KEY, name TEXT) WITH REPLICATION = {'class': ${ReplicationStrategy}, 'replication_factor': ${ReplicationFactor};"
	conf := Config{
		Replication: Replication{
			Strategy: "SimpleStrategy",
			Factor:   "4",
		},
	}
	statement = ReplaceVarsInStatement(statement, conf)
	assert.Equal(t, statement, "CREATE TABLE IF NOT EXISTS test.test (id INT PRIMARY KEY, name TEXT) WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 4;")
}
