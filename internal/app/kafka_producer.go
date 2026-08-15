package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/config"
	kafkadata "github.com/luck/go-learning/internal/data/kafka"
)

// WithKafkaProducer 为任意进程附加 Kafka 发布能力。它不关心调用者是 HTTP、
// gRPC 还是 Consumer；调用者按需将该 Option 传给 New。
func WithKafkaProducer(log *slog.Logger) Option {
	return func(_ context.Context, cfg config.Config, application *App) error {
		if !cfg.Kafka.Enabled {
			return nil
		}
		if application.kafkaProducer != nil {
			return fmt.Errorf("kafka producer is already configured")
		}

		producer, err := kafkadata.NewProducer(cfg.Kafka, log)
		if err != nil {
			return err
		}
		application.kafkaProducer = producer
		// user-events 是内部业务 Topic，由固定 Handler 按稳定消息 schema 发布。
		// 将它从通用事件 API 白名单移除，可以防止任意 JSON 阻塞 Consumer。
		application.Services.EventService = biz.NewEventService(producer, topicsExcept(cfg.Kafka.Topics, biz.UserCreateTopic))
		application.Services.MessagePublisher = biz.NewMessagePublisher(producer, cfg.Kafka.Topics)
		return nil
	}
}

// RequireKafkaTopics 在启动时验证当前进程需要的 Topic 已配置。业务模块使用
// 这个选项表达自身契约，例如 HTTP 用户创建必须拥有 user-events。
func RequireKafkaTopics(topics ...string) Option {
	return func(_ context.Context, cfg config.Config, _ *App) error {
		if !cfg.Kafka.Enabled {
			return nil
		}
		for _, topic := range topics {
			if !containsTopic(cfg.Kafka.Topics, topic) {
				return fmt.Errorf("kafka topics must include %q", topic)
			}
		}
		return nil
	}
}
