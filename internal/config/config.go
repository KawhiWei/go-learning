package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTP     HTTPConfig     `yaml:"http"`
	GRPC     GRPCConfig     `yaml:"grpc"`
	Database DatabaseConfig `yaml:"database"`
	Kafka    KafkaConfig    `yaml:"kafka"`
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

// KafkaConfig 描述 API Producer 与 work Consumer 共用的连接配置，以及
// Worker 的消费组、并发、重试和关闭参数。
type KafkaConfig struct {
	Enabled             bool     `yaml:"enabled"`
	Brokers             []string `yaml:"brokers"`
	GroupID             string   `yaml:"group_id"`
	Topics              []string `yaml:"topics"`
	ClientID            string   `yaml:"client_id"`
	RetryIntervalSecs   int      `yaml:"retry_interval_seconds"`
	PublishTimeoutSecs  int      `yaml:"publish_timeout_seconds"`
	WorkerConcurrency   int      `yaml:"worker_concurrency"`
	PollMaxRecords      int      `yaml:"poll_max_records"`
	ShutdownTimeoutSecs int      `yaml:"shutdown_timeout_seconds"`
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
		Kafka: KafkaConfig{
			Enabled:             false,
			GroupID:             "nino-data-work",
			ClientID:            "nino-data",
			RetryIntervalSecs:   5,
			PublishTimeoutSecs:  10,
			WorkerConcurrency:   8,
			PollMaxRecords:      100,
			ShutdownTimeoutSecs: 30,
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
	if c.Kafka.Enabled {
		if len(normalizeList(c.Kafka.Brokers)) == 0 {
			return errors.New("kafka.brokers must not be empty when kafka is enabled")
		}
		if strings.TrimSpace(c.Kafka.GroupID) == "" {
			return errors.New("kafka.group_id must not be empty when kafka is enabled")
		}
		if len(normalizeList(c.Kafka.Topics)) == 0 {
			return errors.New("kafka.topics must not be empty when kafka is enabled")
		}
		if c.Kafka.RetryIntervalSecs < 1 {
			return errors.New("kafka.retry_interval_seconds must be at least 1 when kafka is enabled")
		}
		if strings.TrimSpace(c.Kafka.ClientID) == "" {
			return errors.New("kafka.client_id must not be empty when kafka is enabled")
		}
		if c.Kafka.PublishTimeoutSecs < 1 {
			return errors.New("kafka.publish_timeout_seconds must be at least 1 when kafka is enabled")
		}
		if c.Kafka.WorkerConcurrency < 1 {
			return errors.New("kafka.worker_concurrency must be at least 1 when kafka is enabled")
		}
		if c.Kafka.PollMaxRecords < 1 {
			return errors.New("kafka.poll_max_records must be at least 1 when kafka is enabled")
		}
		if c.Kafka.ShutdownTimeoutSecs < 1 {
			return errors.New("kafka.shutdown_timeout_seconds must be at least 1 when kafka is enabled")
		}
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
	if value := firstEnv("NINO_KAFKA_ENABLED", "KAFKA_ENABLED"); value != "" {
		if enabled, err := strconv.ParseBool(value); err == nil {
			c.Kafka.Enabled = enabled
		}
	}
	if value := firstEnv("NINO_KAFKA_BROKERS", "KAFKA_BROKERS"); value != "" {
		c.Kafka.Brokers = splitList(value)
	}
	if value := firstEnv("NINO_KAFKA_GROUP_ID", "KAFKA_GROUP_ID"); value != "" {
		c.Kafka.GroupID = strings.TrimSpace(value)
	}
	if value := firstEnv("NINO_KAFKA_TOPICS", "KAFKA_TOPICS"); value != "" {
		c.Kafka.Topics = splitList(value)
	}
	if value := firstEnv("NINO_KAFKA_RETRY_INTERVAL_SECS", "KAFKA_RETRY_INTERVAL_SECS"); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			c.Kafka.RetryIntervalSecs = n
		}
	}
	if value := firstEnv("NINO_KAFKA_CLIENT_ID", "KAFKA_CLIENT_ID"); value != "" {
		c.Kafka.ClientID = strings.TrimSpace(value)
	}
	applyPositiveIntEnv(&c.Kafka.PublishTimeoutSecs, "NINO_KAFKA_PUBLISH_TIMEOUT_SECS")
	applyPositiveIntEnv(&c.Kafka.WorkerConcurrency, "NINO_KAFKA_WORKER_CONCURRENCY")
	applyPositiveIntEnv(&c.Kafka.PollMaxRecords, "NINO_KAFKA_POLL_MAX_RECORDS")
	applyPositiveIntEnv(&c.Kafka.ShutdownTimeoutSecs, "NINO_KAFKA_SHUTDOWN_TIMEOUT_SECS")
	c.Kafka.Brokers = normalizeList(c.Kafka.Brokers)
	c.Kafka.Topics = normalizeList(c.Kafka.Topics)
}

func applyPositiveIntEnv(target *int, name string) {
	if value := os.Getenv(name); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			*target = n
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

func splitList(value string) []string {
	return normalizeList(strings.Split(value, ","))
}

func normalizeList(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
