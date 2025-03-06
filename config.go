package main

import (
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

var (
	ConfigFile        string
	ConfigEnvironment string
	IgnoreExistErrors bool
)

const (
	DefaultConfigFile          = "cassandraconfig.yaml"
	DefaultConfigEnvironment   = "development"
	DefaultConfigPort          = "9042"
	DefaultConfigUsername      = "cassandra"
	DefaultConfigPassword      = "cassandra"
	DefaultConfigMigrationDir  = "migrations"
	DefaultReplicationFactor   = "1"
	DefaultReplicationStrategy = "SimpleStrategy"
)

type Config struct {
	Keyspace          string      `yaml:"keyspace"`
	MigrationDir      string      `yaml:"migration_dir"`
	Connection        Connection  `yaml:"connection"`
	Replication       Replication `yaml:"replication"`
	IgnoreExistErrors bool        `yaml:"-"`
}

type Connection struct {
	Hosts    []string `yaml:"hosts"`
	Port     string   `yaml:"port"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type Replication struct {
	Factor   string `yaml:"factor"`
	Strategy string `yaml:"strategy"`
}

func ReadConfig() (map[string]Config, error) {
	file, err := os.ReadFile(ConfigFile)
	if err != nil {
		return nil, err
	}

	config := make(map[string]Config)
	err = yaml.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func GetConfig() (Config, error) {
	config, err := ReadConfig()
	if err != nil {
		return Config{}, err
	}

	conf, ok := config[ConfigEnvironment]
	if !ok {
		return Config{}, errors.New("no environment: " + ConfigEnvironment)
	}
	nonEmptyHosts := make([]string, 0)
	for _, host := range conf.Connection.Hosts {
		trimmedHost := strings.TrimSpace(os.ExpandEnv(host))
		if trimmedHost != "" {
			nonEmptyHosts = append(nonEmptyHosts, trimmedHost)
		}
	}
	conf.Connection.Hosts = nonEmptyHosts
	if len(conf.Connection.Hosts) == 0 {
		return Config{}, errors.New("at least one host is required")
	}
	if conf.Keyspace == "" {
		return Config{}, errors.New("keyspace is required")
	}
	conf.Keyspace = os.ExpandEnv(conf.Keyspace)
	if specialCharactersRegex.Match([]byte(conf.Keyspace)) {
		return Config{}, errors.New("keyspace contains special characters")
	}
	conf.Connection.Port = os.ExpandEnv(conf.Connection.Port)
	if conf.Connection.Port == "" {
		conf.Connection.Port = DefaultConfigPort
	}
	conf.Connection.Username = os.ExpandEnv(conf.Connection.Username)
	if conf.Connection.Username == "" {
		conf.Connection.Username = DefaultConfigUsername
	}
	conf.Connection.Password = os.ExpandEnv(conf.Connection.Password)
	if conf.Connection.Password == "" {
		conf.Connection.Password = DefaultConfigPassword
	}
	conf.MigrationDir = os.ExpandEnv(conf.MigrationDir)
	if conf.MigrationDir == "" {
		conf.MigrationDir = DefaultConfigMigrationDir
	}
	conf.Replication.Factor = os.ExpandEnv(conf.Replication.Factor)
	if conf.Replication.Factor == "" {
		conf.Replication.Factor = DefaultReplicationFactor
	}
	conf.Replication.Strategy = os.ExpandEnv(conf.Replication.Strategy)
	if conf.Replication.Strategy == "" {
		conf.Replication.Strategy = DefaultReplicationStrategy
	}
	conf.IgnoreExistErrors = IgnoreExistErrors

	return conf, nil
}
