package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTP     HTTPConfig     `yaml:"http"`
	GRPC     GRPCConfig     `yaml:"grpc"`
	Database DatabaseConfig `yaml:"database"`
	Logger   LoggerConfig   `yaml:"logger"`
}

type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

type GRPCConfig struct {
	Addr string `yaml:"addr"`
}

type DatabaseConfig struct {
	URL             string `yaml:"url"`
	MaxConns        int32  `yaml:"max_conns"`
	MinConns        int32  `yaml:"min_conns"`
	HealthCheckSecs int    `yaml:"health_check_seconds"`
}

type LoggerConfig struct {
	Level string `yaml:"level"`
}

func Default() Config {
	return Config{
		HTTP: HTTPConfig{Addr: ":8080"},
		GRPC: GRPCConfig{Addr: ":9090"},
		Database: DatabaseConfig{
			URL:             "postgres://postgres:postgres@localhost:5432/nino?sslmode=disable",
			MaxConns:        10,
			MinConns:        2,
			HealthCheckSecs: 30,
		},
		Logger: LoggerConfig{Level: "info"},
	}
}

// Load reads a YAML file and applies environment overrides. An absent file is
// allowed so container deployments can configure the service entirely through
// environment variables.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		if err == nil && len(data) > 0 {
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return Config{}, fmt.Errorf("parse config %q: %w", path, err)
			}
		}
	}
	applyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.HTTP.Addr == "" {
		return errors.New("http.addr must not be empty")
	}
	if c.GRPC.Addr == "" {
		return errors.New("grpc.addr must not be empty")
	}
	if c.Database.URL == "" {
		return errors.New("database.url must not be empty")
	}
	if c.Database.MaxConns < 1 {
		return errors.New("database.max_conns must be at least 1")
	}
	if c.Database.MinConns < 0 || c.Database.MinConns > c.Database.MaxConns {
		return errors.New("database.min_conns must be between 0 and max_conns")
	}
	return nil
}

func applyEnv(c *Config) {
	if value := firstEnv("NINO_HTTP_ADDR", "HTTP_ADDR"); value != "" {
		c.HTTP.Addr = value
	}
	if value := firstEnv("NINO_GRPC_ADDR", "GRPC_ADDR"); value != "" {
		c.GRPC.Addr = value
	}
	if value := firstEnv("NINO_DATABASE_URL", "NINO_DB_URL", "DATABASE_URL"); value != "" {
		c.Database.URL = value
	}
	if value := firstEnv("NINO_LOG_LEVEL", "LOG_LEVEL"); value != "" {
		c.Logger.Level = value
	}
	if value := firstEnv("NINO_DB_MAX_CONNS", "NINO_DATABASE_MAX_CONNS", "DB_MAX_CONNS"); value != "" {
		if n, err := strconv.ParseInt(value, 10, 32); err == nil {
			c.Database.MaxConns = int32(n)
		}
	}
	if value := firstEnv("NINO_DB_MIN_CONNS", "NINO_DATABASE_MIN_CONNS", "DB_MIN_CONNS"); value != "" {
		if n, err := strconv.ParseInt(value, 10, 32); err == nil {
			c.Database.MinConns = int32(n)
		}
	}
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
