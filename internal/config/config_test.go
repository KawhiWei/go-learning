package config

import "testing"

func TestKafkaValidation(t *testing.T) {
	cfg := Default()
	cfg.Kafka.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("enabled Kafka without brokers and topics should fail validation")
	}

	cfg.Kafka.Brokers = []string{" localhost:9092 "}
	cfg.Kafka.Topics = []string{"events"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid Kafka config rejected: %v", err)
	}
}

func TestKafkaEnvironmentOverrides(t *testing.T) {
	t.Setenv("NINO_KAFKA_BROKERS", " kafka-1:9092,kafka-2:9092,kafka-1:9092 ")
	t.Setenv("NINO_KAFKA_TOPICS", "users,audit")
	t.Setenv("NINO_KAFKA_CLIENT_ID", "api-producer")
	t.Setenv("NINO_KAFKA_CONSUMER_CONCURRENCY", "12")
	t.Setenv("NINO_KAFKA_POLL_MAX_RECORDS", "250")
	cfg := Default()
	applyEnv(&cfg)
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.ConsumerConcurrency != 12 || cfg.Kafka.PollMaxRecords != 250 {
		t.Fatalf("Kafka env overrides = %#v", cfg.Kafka)
	}
	if cfg.Kafka.ClientID != "api-producer" {
		t.Fatalf("client ID = %q", cfg.Kafka.ClientID)
	}
}

func TestNormalizeList(t *testing.T) {
	got := splitList(" users, audit,users, ,audit ")
	if len(got) != 2 || got[0] != "users" || got[1] != "audit" {
		t.Fatalf("splitList() = %#v", got)
	}
}
