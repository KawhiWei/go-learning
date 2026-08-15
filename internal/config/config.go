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
	// HTTP 是 Hertz HTTP 服务的监听配置。
	HTTP HTTPConfig `yaml:"http"`
	// GRPC 是 Kitex RPC 服务的监听配置。
	GRPC GRPCConfig `yaml:"grpc"`
	// Database 是 PostgreSQL 连接池配置，供每个进程独立创建连接池。
	Database DatabaseConfig `yaml:"database"`
	// Kafka 是 Producer 与 Consumer 可复用的 Kafka 基础设施配置。
	Kafka KafkaConfig `yaml:"kafka"`
	// Logger 控制进程日志输出级别。
	Logger LoggerConfig `yaml:"logger"`
}

type HTTPConfig struct {
	// Addr 是 Hertz 监听地址，例如 ":8080"。
	Addr string `yaml:"addr"`
}

type GRPCConfig struct {
	// Addr 是 Kitex 监听地址，例如 ":9090"。
	Addr string `yaml:"addr"`
}

type DatabaseConfig struct {
	// URL 是 pgx 使用的 PostgreSQL 连接串。
	URL string `yaml:"url"`
	// MaxConns 是单个进程允许数据库连接池创建的最大连接数。
	MaxConns int32 `yaml:"max_conns"`
	// MinConns 是连接池保持的最小空闲连接数，必须不大于 MaxConns。
	MinConns int32 `yaml:"min_conns"`
	// HealthCheckSecs 是连接池检查空闲连接健康状态的间隔，单位为秒。
	HealthCheckSecs int `yaml:"health_check_seconds"`
}

// KafkaConfig 描述 Producer 与 Consumer 共用的连接配置，以及
// Consumer 的消费组、并发、重试和关闭参数。
type KafkaConfig struct {
	// Enabled 控制进程是否允许创建 Kafka 客户端。
	Enabled bool `yaml:"enabled"`
	// Brokers 是 Kafka broker 地址列表，例如 kafka:9092。
	Brokers []string `yaml:"brokers"`
	// GroupID 是 Consumer 所属消费组；Producer 不使用该字段。
	GroupID string `yaml:"group_id"`
	// Topics 是 Producer 可发布且 Consumer 应订阅的 Topic 白名单。
	Topics []string `yaml:"topics"`
	// ClientID 是 franz-go 客户端标识，便于 broker 日志和监控区分实例。
	ClientID string `yaml:"client_id"`
	// RetryIntervalSecs 是 Consumer 处理或提交失败后重试的等待时间，单位为秒。
	RetryIntervalSecs int `yaml:"retry_interval_seconds"`
	// PublishTimeoutSecs 是 Producer 等待 broker 确认一条消息的最长时间，单位为秒。
	PublishTimeoutSecs int `yaml:"publish_timeout_seconds"`
	// ConsumerConcurrency 是一个 poll 批次中可并发处理的最大 partition 数。
	ConsumerConcurrency int `yaml:"consumer_concurrency"`
	// PollMaxRecords 是 Consumer 单次 PollRecords 最多获取的消息数。
	PollMaxRecords int `yaml:"poll_max_records"`
	// ShutdownTimeoutSecs 是收到退出信号后排空在途任务和提交 offset 的最长时间，单位为秒。
	ShutdownTimeoutSecs int `yaml:"shutdown_timeout_seconds"`
}

type LoggerConfig struct {
	// Level 是结构化日志的最小输出级别，例如 debug、info、warn 或 error。
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
			GroupID:             "nino-data-consumer",
			ClientID:            "nino-data",
			RetryIntervalSecs:   5,
			PublishTimeoutSecs:  10,
			ConsumerConcurrency: 8,
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
		if c.Kafka.ConsumerConcurrency < 1 {
			return errors.New("kafka.consumer_concurrency must be at least 1 when kafka is enabled")
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
	applyPositiveIntEnv(&c.Kafka.ConsumerConcurrency, "NINO_KAFKA_CONSUMER_CONCURRENCY")
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
