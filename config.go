package migrate

import (
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

const (
	DefaultConfigFile         = "cassandraconfig.yaml"
	DefaultConfigEnvironment  = "development"
	DefaultConfigPort         = "9042"
	DefaultConfigUsername     = "cassandra"
	DefaultConfigPassword     = "cassandra"
	DefaultConfigMigrationDir = "migrations"
)

// Config represents validated runtime migration settings for one environment.
type Config struct {
	Keyspace          string     `yaml:"keyspace"`
	MigrationDir      string     `yaml:"migration_dir"`
	Connection        Connection `yaml:"connection"`
	IgnoreExistErrors bool       `yaml:"-"`
}

// Connection describes Cassandra connectivity settings.
type Connection struct {
	Hosts    []string `yaml:"hosts"`
	Port     string   `yaml:"port"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

// Options represents loader options for retrieving a Config from YAML.
type Options struct {
	ConfigFile        string
	Environment       string
	IgnoreExistErrors bool
}

// DefaultOptions returns default config loader values.
func DefaultOptions() Options {
	return Options{
		ConfigFile:        DefaultConfigFile,
		Environment:       DefaultConfigEnvironment,
		IgnoreExistErrors: false,
	}
}

// ReadConfigFile reads and unmarshals the full environment map from a config file.
func ReadConfigFile(configFile string) (map[string]Config, error) {
	file, err := os.ReadFile(configFile)
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

// GetDefaultConfig loads configuration using default options.
func GetDefaultConfig() (Config, error) {
	opts := DefaultOptions()
	return GetConfigFrom(opts.ConfigFile, opts.Environment, opts.IgnoreExistErrors)
}

// GetConfigFrom loads and validates one environment from the config file.
func GetConfigFrom(configFile, configEnvironment string, ignoreExistErrors bool) (Config, error) {
	config, err := ReadConfigFile(configFile)
	if err != nil {
		return Config{}, err
	}

	conf, ok := config[configEnvironment]
	if !ok {
		return Config{}, errors.New("no environment: " + configEnvironment)
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
	conf.IgnoreExistErrors = ignoreExistErrors

	return conf, nil
}
